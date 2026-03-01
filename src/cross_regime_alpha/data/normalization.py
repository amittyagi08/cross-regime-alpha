from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
]


@dataclass(frozen=True)
class NormalizationConfig:
    source_dir: str = "data/cache/ibkr/normalized/daily"
    cleaned_dir: str = "data/cache/ibkr/cleaned/daily"
    report_dir: str = "outputs/runs"
    outlier_return_threshold: float = 0.20


@dataclass(frozen=True)
class QualitySummary:
    symbols_requested: int
    source_rows: int
    duplicate_rows_removed: int
    invalid_rows_removed: int
    outlier_rows_flagged: int
    aligned_rows: int
    missing_bar_rows: int
    outlier_return_threshold: float
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class NormalizationResult:
    run_id: str
    generated_at_utc: str
    symbols: list[str]
    cleaned_files: list[str]
    quality_report_file: str
    quality_summary: QualitySummary


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _read_symbol_frames(*, base_dir: Path, source_dir: str, symbols: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        pattern = base_dir / source_dir / f"symbol={symbol}" / "**" / "*.parquet"
        files = sorted(pattern.parent.glob(pattern.name)) if "**" not in str(pattern) else sorted((base_dir / source_dir / f"symbol={symbol}").glob("**/*.parquet"))
        if not files:
            continue
        for file in files:
            frames.append(pd.read_parquet(file))

    if not frames:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def _prepare_types(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["symbol"] = prepared["symbol"].astype(str).str.strip().str.upper()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce").dt.date
    for column in ["open", "high", "low", "close", "adj_close"]:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared["volume"] = pd.to_numeric(prepared["volume"], errors="coerce")
    return prepared


def _validate_required_columns(frame: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns for normalization: {missing}")


def _remove_duplicates(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    ordered = frame.sort_values(["symbol", "date"]).copy()
    duplicate_mask = ordered.duplicated(subset=["symbol", "date"], keep="last")
    duplicate_count = int(duplicate_mask.sum())
    deduped = ordered.loc[~duplicate_mask].copy()
    return deduped.reset_index(drop=True), duplicate_count


def _remove_invalid_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    required_not_null = frame[["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]].notna().all(axis=1)
    positive_prices = (frame[["open", "high", "low", "close", "adj_close"]] > 0).all(axis=1)
    non_negative_volume = frame["volume"] >= 0
    high_ok = frame["high"] >= frame[["open", "close", "low"]].max(axis=1)
    low_ok = frame["low"] <= frame[["open", "close", "high"]].min(axis=1)

    valid_mask = required_not_null & positive_prices & non_negative_volume & high_ok & low_ok
    invalid_count = int((~valid_mask).sum())
    valid = frame.loc[valid_mask].copy()
    valid["volume"] = valid["volume"].astype("int64")
    return valid.reset_index(drop=True), invalid_count


def _flag_outliers(frame: pd.DataFrame, *, threshold: float) -> tuple[pd.DataFrame, int]:
    flagged = frame.sort_values(["symbol", "date"]).copy()
    flagged["daily_return"] = flagged.groupby("symbol")["adj_close"].pct_change()
    flagged["is_outlier_jump"] = flagged["daily_return"].abs() > threshold
    outlier_count = int(flagged["is_outlier_jump"].fillna(False).sum())
    return flagged, outlier_count


def _align_to_common_calendar(frame: pd.DataFrame, symbols: list[str]) -> tuple[pd.DataFrame, int]:
    if frame.empty:
        aligned = pd.DataFrame(columns=REQUIRED_COLUMNS + ["daily_return", "is_outlier_jump", "is_missing_bar"])
        return aligned, 0

    all_dates = sorted(frame["date"].dropna().unique())
    requested_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol.strip()})
    if not requested_symbols:
        requested_symbols = sorted(frame["symbol"].dropna().unique())

    calendar = pd.MultiIndex.from_product([requested_symbols, all_dates], names=["symbol", "date"]).to_frame(index=False)
    aligned = calendar.merge(frame, on=["symbol", "date"], how="left", sort=True)
    aligned["is_missing_bar"] = aligned["open"].isna()
    missing_rows = int(aligned["is_missing_bar"].sum())
    return aligned, missing_rows


def _write_partitioned_parquet(frame: pd.DataFrame, *, base_dir: Path) -> list[str]:
    if frame.empty:
        return []

    output_files: list[str] = []
    dated = frame.copy()
    dated["date"] = pd.to_datetime(dated["date"])
    dated["year"] = dated["date"].dt.year
    dated["month"] = dated["date"].dt.month
    timestamp = _now_utc().strftime("%Y%m%dT%H%M%S")

    for (symbol, year, month), chunk in dated.groupby(["symbol", "year", "month"], sort=True):
        target_dir = base_dir / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / f"part-{timestamp}.parquet"
        chunk.drop(columns=["year", "month"]).to_parquet(target_file, index=False)
        output_files.append(str(target_file))

    return output_files


def _write_quality_report(
    summary: QualitySummary,
    *,
    run_id: str,
    generated_at_utc: str,
    symbols: list[str],
    output_dir: Path,
) -> Path:
    target_dir = output_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    report_path = target_dir / "normalization_report.json"
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "symbols": symbols,
        "quality_summary": {
            "symbols_requested": summary.symbols_requested,
            "source_rows": summary.source_rows,
            "duplicate_rows_removed": summary.duplicate_rows_removed,
            "invalid_rows_removed": summary.invalid_rows_removed,
            "outlier_rows_flagged": summary.outlier_rows_flagged,
            "aligned_rows": summary.aligned_rows,
            "missing_bar_rows": summary.missing_bar_rows,
            "outlier_return_threshold": summary.outlier_return_threshold,
            "warnings": summary.warnings,
        },
    }
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_path


def normalize_daily_data_cache(
    symbols: list[str],
    *,
    config: NormalizationConfig | None = None,
    base_dir: str | Path = ".",
    run_id: str | None = None,
) -> NormalizationResult:
    active_config = config or NormalizationConfig()
    working_base = Path(base_dir)
    requested_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol and symbol.strip()})
    if not requested_symbols:
        raise ValueError("At least one symbol is required for normalization.")

    source = _read_symbol_frames(base_dir=working_base, source_dir=active_config.source_dir, symbols=requested_symbols)
    _validate_required_columns(source)

    prepared = _prepare_types(source)
    deduped, duplicate_count = _remove_duplicates(prepared)
    valid, invalid_count = _remove_invalid_rows(deduped)
    flagged, outlier_count = _flag_outliers(valid, threshold=active_config.outlier_return_threshold)
    aligned, missing_rows = _align_to_common_calendar(flagged, requested_symbols)

    timestamp = _now_utc()
    resolved_run_id = run_id or (timestamp.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])
    generated_at = timestamp.isoformat()

    cleaned_files = _write_partitioned_parquet(
        aligned,
        base_dir=working_base / active_config.cleaned_dir,
    )

    warnings: list[str] = []
    if source.empty:
        warnings.append("No source rows found for requested symbols.")

    summary = QualitySummary(
        symbols_requested=len(requested_symbols),
        source_rows=len(source),
        duplicate_rows_removed=duplicate_count,
        invalid_rows_removed=invalid_count,
        outlier_rows_flagged=outlier_count,
        aligned_rows=len(aligned),
        missing_bar_rows=missing_rows,
        outlier_return_threshold=active_config.outlier_return_threshold,
        warnings=warnings,
    )

    report_path = _write_quality_report(
        summary,
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        output_dir=working_base / active_config.report_dir,
    )

    return NormalizationResult(
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        cleaned_files=cleaned_files,
        quality_report_file=str(report_path),
        quality_summary=summary,
    )
