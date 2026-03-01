from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class TrendConfig:
    source_dir: str = "data/cache/signals/daily"
    fallback_source_dir: str = "data/cache/features/daily"
    output_dir: str = "data/cache/signals/daily"
    report_dir: str = "outputs/runs"
    write_mode: str = "upsert_latest"


@dataclass(frozen=True)
class TrendRunResult:
    run_id: str
    generated_at_utc: str
    symbols: list[str]
    output_files: list[str]
    report_file: str
    total_rows: int
    trend_known_rows: int
    trend_eligible_rows: int


REQUIRED_COLUMNS = {"symbol", "date", "adj_close", "sma200", "sma50"}


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _read_symbol_frames(base_dir: Path, source_dir: str, symbols: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        symbol_dir = base_dir / source_dir / f"symbol={symbol}"
        files = sorted(symbol_dir.glob("**/*.parquet"))
        for file in files:
            frames.append(pd.read_parquet(file))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_source_frames(base_dir: Path, config: TrendConfig, symbols: list[str]) -> pd.DataFrame:
    primary = _read_symbol_frames(base_dir, config.source_dir, symbols)
    if not primary.empty:
        return primary
    return _read_symbol_frames(base_dir, config.fallback_source_dir, symbols)


def _validate_columns(frame: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns for trend filter: {missing}")


def _dedupe_latest(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    deduped = frame.copy()
    deduped["date"] = pd.to_datetime(deduped["date"], errors="coerce")
    deduped["pulled_at_utc"] = pd.to_datetime(deduped.get("pulled_at_utc"), errors="coerce")
    deduped = deduped.sort_values(["symbol", "date", "pulled_at_utc"]).drop_duplicates(
        subset=["symbol", "date"],
        keep="last",
    )
    return deduped.reset_index(drop=True)


def _apply_trend_flags(frame: pd.DataFrame) -> pd.DataFrame:
    flagged = frame.copy()
    flagged["trend_known"] = flagged["adj_close"].notna() & flagged["sma200"].notna() & flagged["sma50"].notna()
    flagged["trend_eligible"] = flagged["trend_known"] & (flagged["adj_close"] > flagged["sma200"]) & (
        flagged["sma50"] > flagged["sma200"]
    )
    return flagged


def _write_partitioned_parquet(frame: pd.DataFrame, output_base: Path, *, write_mode: str) -> list[str]:
    if frame.empty:
        return []

    output_files: list[str] = []
    dated = frame.copy()
    dated["date"] = pd.to_datetime(dated["date"])
    dated["year"] = dated["date"].dt.year
    dated["month"] = dated["date"].dt.month
    timestamp = _now_utc().strftime("%Y%m%dT%H%M%S")

    for (symbol, year, month), chunk in dated.groupby(["symbol", "year", "month"], sort=True):
        target_dir = output_base / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        if write_mode == "upsert_latest":
            for existing in target_dir.glob("*.parquet"):
                existing.unlink()
        file_path = target_dir / f"part-{timestamp}.parquet"
        chunk.drop(columns=["year", "month"]).to_parquet(file_path, index=False)
        output_files.append(str(file_path))

    return output_files


def _write_report(
    *,
    output_dir: Path,
    run_id: str,
    generated_at_utc: str,
    symbols: list[str],
    total_rows: int,
    trend_known_rows: int,
    trend_eligible_rows: int,
) -> Path:
    target_dir = output_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    report_file = target_dir / "trend_report.json"
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "symbols": symbols,
        "total_rows": total_rows,
        "trend_known_rows": trend_known_rows,
        "trend_eligible_rows": trend_eligible_rows,
    }
    report_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_file


def apply_trend_eligibility_filter(
    symbols: list[str],
    *,
    config: TrendConfig | None = None,
    base_dir: str | Path = ".",
    run_id: str | None = None,
) -> TrendRunResult:
    active_config = config or TrendConfig()
    base_path = Path(base_dir)
    requested_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol and symbol.strip()})
    if not requested_symbols:
        raise ValueError("At least one symbol is required for trend eligibility filtering.")

    source = _read_source_frames(base_path, active_config, requested_symbols)
    if source.empty:
        raise ValueError("No source rows found for requested symbols.")
    _validate_columns(source)

    deduped = _dedupe_latest(source)
    filtered = deduped.loc[deduped["symbol"].isin(requested_symbols)].copy()
    flagged = _apply_trend_flags(filtered).sort_values(["symbol", "date"]).reset_index(drop=True)

    generated_at = _now_utc().isoformat()
    resolved_run_id = run_id or (_now_utc().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])

    output_files = _write_partitioned_parquet(
        flagged,
        base_path / active_config.output_dir,
        write_mode=active_config.write_mode,
    )
    known_rows = int(flagged["trend_known"].sum())
    eligible_rows = int(flagged["trend_eligible"].sum())
    report_file = _write_report(
        output_dir=base_path / active_config.report_dir,
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        total_rows=len(flagged),
        trend_known_rows=known_rows,
        trend_eligible_rows=eligible_rows,
    )

    return TrendRunResult(
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        output_files=output_files,
        report_file=str(report_file),
        total_rows=len(flagged),
        trend_known_rows=known_rows,
        trend_eligible_rows=eligible_rows,
    )
