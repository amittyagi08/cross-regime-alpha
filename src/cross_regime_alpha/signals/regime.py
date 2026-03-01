from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class RegimeConfig:
    source_dir: str = "data/cache/features/daily"
    output_dir: str = "data/cache/signals/daily"
    report_dir: str = "outputs/runs"
    benchmark_symbol: str = "SPY"
    write_mode: str = "upsert_latest"


@dataclass(frozen=True)
class RegimeRunResult:
    run_id: str
    generated_at_utc: str
    symbols: list[str]
    output_files: list[str]
    report_file: str
    total_rows: int
    regime_known_rows: int
    regime_on_rows: int


REQUIRED_COLUMNS = {"symbol", "date", "adj_close", "sma200"}


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _read_feature_frames(base_dir: Path, source_dir: str, symbols: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        symbol_dir = base_dir / source_dir / f"symbol={symbol}"
        files = sorted(symbol_dir.glob("**/*.parquet"))
        for file in files:
            frames.append(pd.read_parquet(file))

    if not frames:
        return pd.DataFrame(columns=sorted(REQUIRED_COLUMNS))
    return pd.concat(frames, ignore_index=True)


def _validate_columns(frame: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns for regime filter: {missing}")


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


def _build_regime_table(features: pd.DataFrame, benchmark_symbol: str) -> pd.DataFrame:
    benchmark = features.loc[features["symbol"] == benchmark_symbol].copy()
    if benchmark.empty:
        raise ValueError(f"Benchmark symbol {benchmark_symbol} not found in feature cache.")

    benchmark["regime_known"] = benchmark["adj_close"].notna() & benchmark["sma200"].notna()
    benchmark["regime_on"] = benchmark["regime_known"] & (benchmark["adj_close"] > benchmark["sma200"])

    return benchmark[["date", "regime_on", "regime_known", "adj_close", "sma200"]].rename(
        columns={
            "adj_close": "benchmark_adj_close",
            "sma200": "benchmark_sma200",
        }
    )


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
    benchmark_symbol: str,
    total_rows: int,
    regime_known_rows: int,
    regime_on_rows: int,
) -> Path:
    target_dir = output_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    report_file = target_dir / "regime_report.json"
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "symbols": symbols,
        "benchmark_symbol": benchmark_symbol,
        "total_rows": total_rows,
        "regime_known_rows": regime_known_rows,
        "regime_on_rows": regime_on_rows,
    }
    report_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_file


def apply_market_regime_filter(
    symbols: list[str],
    *,
    config: RegimeConfig | None = None,
    base_dir: str | Path = ".",
    run_id: str | None = None,
) -> RegimeRunResult:
    active_config = config or RegimeConfig()
    base_path = Path(base_dir)
    requested_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol and symbol.strip()})
    if not requested_symbols:
        raise ValueError("At least one symbol is required for regime filtering.")

    benchmark_symbol = _normalize_symbol(active_config.benchmark_symbol)
    read_symbols = sorted(set(requested_symbols + [benchmark_symbol]))
    features = _read_feature_frames(base_path, active_config.source_dir, read_symbols)
    _validate_columns(features)
    if features.empty:
        raise ValueError("No feature rows found for requested symbols/benchmark.")

    features = _dedupe_latest(features)
    regime_table = _build_regime_table(features, benchmark_symbol=benchmark_symbol)

    target = features.loc[features["symbol"].isin(requested_symbols)].copy()
    merged = target.merge(regime_table, on="date", how="left")
    merged["regime_known"] = merged["regime_known"].fillna(False).astype(bool)
    merged["regime_on"] = merged["regime_on"].fillna(False).astype(bool)
    merged["regime_benchmark_symbol"] = benchmark_symbol

    generated_at = _now_utc().isoformat()
    resolved_run_id = run_id or (_now_utc().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])

    output_files = _write_partitioned_parquet(
        merged.sort_values(["symbol", "date"]).reset_index(drop=True),
        base_path / active_config.output_dir,
        write_mode=active_config.write_mode,
    )

    known_rows = int(merged["regime_known"].sum())
    on_rows = int(merged["regime_on"].sum())
    report_file = _write_report(
        output_dir=base_path / active_config.report_dir,
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        benchmark_symbol=benchmark_symbol,
        total_rows=len(merged),
        regime_known_rows=known_rows,
        regime_on_rows=on_rows,
    )

    return RegimeRunResult(
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        output_files=output_files,
        report_file=str(report_file),
        total_rows=len(merged),
        regime_known_rows=known_rows,
        regime_on_rows=on_rows,
    )
