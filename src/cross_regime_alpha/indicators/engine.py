from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IndicatorConfig:
    source_dir: str = "data/cache/ibkr/cleaned/daily"
    output_dir: str = "data/cache/features/daily"
    report_dir: str = "outputs/runs"
    sma200_period: int = 200
    sma50_period: int = 50
    ema20_period: int = 20
    rsi14_period: int = 14
    atr14_period: int = 14
    rolling_high_period: int = 20
    volume_sma50_period: int = 50
    include_volume_sma50: bool = True
    write_mode: str = "upsert_latest"


@dataclass(frozen=True)
class IndicatorRunResult:
    run_id: str
    generated_at_utc: str
    symbols: list[str]
    feature_files: list[str]
    report_file: str
    total_rows: int
    indicator_ready_rows: int


REQUIRED_INPUT_COLUMNS = {
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "is_missing_bar",
}


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _read_cleaned_frames(base_dir: Path, source_dir: str, symbols: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        symbol_dir = base_dir / source_dir / f"symbol={symbol}"
        files = sorted(symbol_dir.glob("**/*.parquet"))
        for file in files:
            frames.append(pd.read_parquet(file))

    if not frames:
        return pd.DataFrame(columns=sorted(REQUIRED_INPUT_COLUMNS))
    return pd.concat(frames, ignore_index=True)


def _validate_input_columns(frame: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_INPUT_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required cleaned columns for indicators: {missing}")


def _wilder_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_loss != 0, 100.0)
    return rsi


def _wilder_atr(adj_high: pd.Series, adj_low: pd.Series, adj_close: pd.Series, period: int) -> pd.Series:
    prev_close = adj_close.shift(1)
    tr = pd.concat(
        [
            (adj_high - adj_low).abs(),
            (adj_high - prev_close).abs(),
            (adj_low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return atr


def _compute_symbol_indicators(frame: pd.DataFrame, config: IndicatorConfig) -> pd.DataFrame:
    ordered = frame.sort_values("date").copy()
    ratio = (ordered["adj_close"] / ordered["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    ratio = ratio.fillna(1.0)
    ordered["adj_high"] = ordered["high"] * ratio
    ordered["adj_low"] = ordered["low"] * ratio

    ordered["sma200"] = ordered["adj_close"].rolling(config.sma200_period, min_periods=config.sma200_period).mean()
    ordered["sma50"] = ordered["adj_close"].rolling(config.sma50_period, min_periods=config.sma50_period).mean()
    ordered["ema20"] = ordered["adj_close"].ewm(
        span=config.ema20_period,
        min_periods=config.ema20_period,
        adjust=False,
    ).mean()
    ordered["rsi14"] = _wilder_rsi(ordered["adj_close"], config.rsi14_period)
    ordered["atr14"] = _wilder_atr(
        ordered["adj_high"],
        ordered["adj_low"],
        ordered["adj_close"],
        config.atr14_period,
    )
    ordered["rolling_high_20"] = ordered["adj_high"].rolling(
        config.rolling_high_period,
        min_periods=config.rolling_high_period,
    ).max()

    if config.include_volume_sma50:
        ordered["volume_sma50"] = ordered["volume"].rolling(
            config.volume_sma50_period,
            min_periods=config.volume_sma50_period,
        ).mean()
    else:
        ordered["volume_sma50"] = np.nan

    readiness_columns = ["sma200", "sma50", "ema20", "rsi14", "atr14", "rolling_high_20"]
    if config.include_volume_sma50:
        readiness_columns.append("volume_sma50")

    ordered["indicator_ready"] = ordered[readiness_columns].notna().all(axis=1)
    ordered.loc[ordered["is_missing_bar"].fillna(False), [
        "sma200",
        "sma50",
        "ema20",
        "rsi14",
        "atr14",
        "rolling_high_20",
        "volume_sma50",
    ]] = np.nan
    ordered.loc[ordered["is_missing_bar"].fillna(False), "indicator_ready"] = False
    ordered["indicator_ready"] = ordered["indicator_ready"].fillna(False).astype(bool)

    return ordered


def _dedupe_latest_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    deduped = frame.copy()
    deduped["pulled_at_utc"] = pd.to_datetime(deduped.get("pulled_at_utc"), errors="coerce")
    deduped = deduped.sort_values(["symbol", "date", "pulled_at_utc"]).drop_duplicates(
        subset=["symbol", "date"],
        keep="last",
    )
    return deduped.reset_index(drop=True)


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
    indicator_ready_rows: int,
    config: IndicatorConfig,
) -> Path:
    target_dir = output_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    report_file = target_dir / "indicators_report.json"
    payload = {
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "symbols": symbols,
        "total_rows": total_rows,
        "indicator_ready_rows": indicator_ready_rows,
        "config": {
            "sma200_period": config.sma200_period,
            "sma50_period": config.sma50_period,
            "ema20_period": config.ema20_period,
            "rsi14_period": config.rsi14_period,
            "atr14_period": config.atr14_period,
            "rolling_high_period": config.rolling_high_period,
            "volume_sma50_period": config.volume_sma50_period,
            "include_volume_sma50": config.include_volume_sma50,
        },
    }
    report_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return report_file


def compute_indicators_from_cleaned_cache(
    symbols: list[str],
    *,
    config: IndicatorConfig | None = None,
    base_dir: str | Path = ".",
    run_id: str | None = None,
) -> IndicatorRunResult:
    active_config = config or IndicatorConfig()
    base_path = Path(base_dir)
    requested_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if symbol and symbol.strip()})
    if not requested_symbols:
        raise ValueError("At least one symbol is required for indicator computation.")

    cleaned = _read_cleaned_frames(base_path, active_config.source_dir, requested_symbols)
    _validate_input_columns(cleaned)
    if cleaned.empty:
        raise ValueError("No cleaned cache rows found for requested symbols.")

    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned = _dedupe_latest_rows(cleaned)
    per_symbol: list[pd.DataFrame] = []
    for symbol, group in cleaned.groupby("symbol", sort=True):
        per_symbol.append(_compute_symbol_indicators(group.copy(), active_config))

    features = pd.concat(per_symbol, ignore_index=True).sort_values(["symbol", "date"]).reset_index(drop=True)
    generated_at = _now_utc().isoformat()
    resolved_run_id = run_id or (_now_utc().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])

    output_files = _write_partitioned_parquet(
        features,
        base_path / active_config.output_dir,
        write_mode=active_config.write_mode,
    )
    ready_rows = int(features["indicator_ready"].fillna(False).sum())
    report_file = _write_report(
        output_dir=base_path / active_config.report_dir,
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        total_rows=len(features),
        indicator_ready_rows=ready_rows,
        config=active_config,
    )

    return IndicatorRunResult(
        run_id=resolved_run_id,
        generated_at_utc=generated_at,
        symbols=requested_symbols,
        feature_files=output_files,
        report_file=str(report_file),
        total_rows=len(features),
        indicator_ready_rows=ready_rows,
    )
