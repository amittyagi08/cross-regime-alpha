from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from cross_regime_alpha.brokers.ibkr import IBKRClient


@dataclass(frozen=True)
class DailyIngestionConfig:
    end_datetime: str = ""
    duration_str: str = "5 Y"
    bar_size_setting: str = "1 day"
    use_rth: bool = True
    format_date: int = 1
    exchange: str = "SMART"
    currency: str = "USD"
    what_to_show_raw: str = "TRADES"
    what_to_show_adjusted: str = "ADJUSTED_LAST"
    batch_size: int = 25
    throttle_seconds: float = 0.2
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    raw_cache_dir: str = "data/cache/ibkr/raw/daily"
    normalized_cache_dir: str = "data/cache/ibkr/normalized/daily"
    run_metadata_dir: str = "outputs/runs"


@dataclass(frozen=True)
class SymbolIngestionResult:
    symbol: str
    row_count: int
    adjustment_method: str
    raw_files: list[str] = field(default_factory=list)
    normalized_files: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class IngestionRunResult:
    run_id: str
    generated_at_utc: str
    symbols_requested: int
    symbols_succeeded: int
    symbols_failed: int
    results: list[SymbolIngestionResult] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)
    metadata_file: str | None = None


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _bars_to_frame(bars: list[Any]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    records: list[dict[str, Any]] = []
    for bar in bars:
        if isinstance(bar, dict):
            record = {
                "date": bar.get("date"),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume", 0),
            }
        else:
            record = {
                "date": getattr(bar, "date", None),
                "open": getattr(bar, "open", None),
                "high": getattr(bar, "high", None),
                "low": getattr(bar, "low", None),
                "close": getattr(bar, "close", None),
                "volume": getattr(bar, "volume", 0),
            }
        records.append(record)

    frame = pd.DataFrame(records)
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    for col in ("open", "high", "low", "close"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0).astype("int64")
    frame = frame.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date")
    return frame.reset_index(drop=True)


def _request_bars_with_retries(
    client: IBKRClient,
    contract: Any,
    config: DailyIngestionConfig,
    *,
    what_to_show: str,
) -> list[Any]:
    attempt = 0
    last_error: Exception | None = None
    while attempt < config.max_retries:
        attempt += 1
        try:
            return client.request_historical_data(
                contract,
                endDateTime=config.end_datetime,
                durationStr=config.duration_str,
                barSizeSetting=config.bar_size_setting,
                whatToShow=what_to_show,
                useRTH=config.use_rth,
                formatDate=config.format_date,
            )
        except Exception as exc:  # pragma: no cover - covered by retry behavior tests
            last_error = exc
            if attempt >= config.max_retries:
                break
            time.sleep(config.retry_delay_seconds)
    if last_error is not None:
        raise last_error
    return []


def _build_symbol_frames(
    client: IBKRClient,
    symbol: str,
    config: DailyIngestionConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, str, list[str]]:
    from ib_insync import Stock

    normalized_symbol = _normalize_symbol(symbol)
    contract = Stock(normalized_symbol, config.exchange, config.currency)
    warnings: list[str] = []

    raw_bars = _request_bars_with_retries(client, contract, config, what_to_show=config.what_to_show_raw)
    raw_df = _bars_to_frame(raw_bars)
    if raw_df.empty:
        raise ValueError(f"No raw bars returned for symbol {normalized_symbol}")

    adjustment_method = "none"
    adj_close_series = raw_df["close"].copy()
    adjustment_factor = pd.Series(1.0, index=raw_df.index, dtype="float64")

    try:
        adj_bars = _request_bars_with_retries(
            client,
            contract,
            config,
            what_to_show=config.what_to_show_adjusted,
        )
        adj_df = _bars_to_frame(adj_bars)
        if not adj_df.empty:
            merged = raw_df[["date", "close"]].merge(
                adj_df[["date", "close"]].rename(columns={"close": "adj_close_candidate"}),
                on="date",
                how="left",
            )
            valid_adj = merged["adj_close_candidate"].notna()
            if valid_adj.any():
                adj_close_series = merged["adj_close_candidate"].fillna(merged["close"])
                denominator = merged["close"].replace(0, pd.NA)
                adjustment_factor = (
                    (adj_close_series / denominator)
                    .replace([float("inf"), -float("inf")], pd.NA)
                    .fillna(1.0)
                    .astype("float64")
                )
                adjustment_method = "ibkr_adjusted_last_factor"
            else:
                warnings.append(
                    f"No adjusted close values available for {normalized_symbol}; using close as adj_close."
                )
        else:
            warnings.append(
                f"No ADJUSTED_LAST bars available for {normalized_symbol}; using close as adj_close."
            )
    except Exception as exc:
        warnings.append(
            f"Adjusted close request failed for {normalized_symbol}; using close as adj_close. Error: {exc}"
        )

    pulled_at = _now_utc().isoformat()
    normalized_df = raw_df.copy()
    normalized_df["adj_close"] = adj_close_series.astype("float64")
    normalized_df["adjustment_factor"] = adjustment_factor.astype("float64")
    normalized_df["adjustment_method"] = adjustment_method
    normalized_df["what_to_show"] = config.what_to_show_raw
    normalized_df["exchange"] = config.exchange
    normalized_df["currency"] = config.currency
    normalized_df["source"] = "ibkr"
    normalized_df["pulled_at_utc"] = pulled_at
    normalized_df.insert(0, "symbol", normalized_symbol)

    raw_df = raw_df.copy()
    raw_df.insert(0, "symbol", normalized_symbol)
    raw_df["what_to_show"] = config.what_to_show_raw
    raw_df["exchange"] = config.exchange
    raw_df["currency"] = config.currency
    raw_df["source"] = "ibkr"
    raw_df["pulled_at_utc"] = pulled_at

    return raw_df, normalized_df, adjustment_method, warnings


def _write_partitioned_parquet(
    frame: pd.DataFrame,
    *,
    base_dir: str | Path,
    symbol: str,
) -> list[str]:
    if frame.empty:
        return []

    output_paths: list[str] = []
    dated = frame.copy()
    dated["date"] = pd.to_datetime(dated["date"])
    dated["year"] = dated["date"].dt.year
    dated["month"] = dated["date"].dt.month

    timestamp = _now_utc().strftime("%Y%m%dT%H%M%S")
    for (year, month), chunk in dated.groupby(["year", "month"], sort=True):
        target_dir = Path(base_dir) / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"part-{timestamp}.parquet"
        chunk.drop(columns=["year", "month"]).to_parquet(file_path, index=False)
        output_paths.append(str(file_path))

    return output_paths


def _write_run_metadata(result: IngestionRunResult, *, output_dir: str | Path) -> Path:
    target_dir = Path(output_dir) / result.run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    metadata_file = target_dir / "metadata.json"
    payload = {
        "run_id": result.run_id,
        "generated_at_utc": result.generated_at_utc,
        "symbols_requested": result.symbols_requested,
        "symbols_succeeded": result.symbols_succeeded,
        "symbols_failed": result.symbols_failed,
        "results": [
            {
                "symbol": item.symbol,
                "row_count": item.row_count,
                "adjustment_method": item.adjustment_method,
                "raw_files": item.raw_files,
                "normalized_files": item.normalized_files,
                "warnings": item.warnings,
            }
            for item in result.results
        ],
        "failures": result.failures,
    }
    metadata_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return metadata_file


def _batched(symbols: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        return [symbols]
    return [symbols[index : index + batch_size] for index in range(0, len(symbols), batch_size)]


def ingest_daily_bars(
    client: IBKRClient,
    symbols: list[str],
    *,
    config: DailyIngestionConfig | None = None,
    base_dir: str | Path = ".",
    auto_connect: bool = True,
) -> IngestionRunResult:
    active_config = config or DailyIngestionConfig()
    requested_symbols = [_normalize_symbol(symbol) for symbol in symbols if symbol and symbol.strip()]
    run_id = _now_utc().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]

    connected_here = False
    if auto_connect and not client.is_connected():
        client.connect()
        connected_here = True

    results: list[SymbolIngestionResult] = []
    failures: dict[str, str] = {}

    try:
        for batch in _batched(requested_symbols, active_config.batch_size):
            for symbol in batch:
                try:
                    raw_df, normalized_df, adjustment_method, warnings = _build_symbol_frames(
                        client,
                        symbol,
                        active_config,
                    )

                    raw_files = _write_partitioned_parquet(
                        raw_df,
                        base_dir=Path(base_dir) / active_config.raw_cache_dir,
                        symbol=symbol,
                    )
                    normalized_files = _write_partitioned_parquet(
                        normalized_df,
                        base_dir=Path(base_dir) / active_config.normalized_cache_dir,
                        symbol=symbol,
                    )

                    results.append(
                        SymbolIngestionResult(
                            symbol=symbol,
                            row_count=len(normalized_df),
                            adjustment_method=adjustment_method,
                            raw_files=raw_files,
                            normalized_files=normalized_files,
                            warnings=warnings,
                        )
                    )
                except Exception as exc:
                    failures[symbol] = str(exc)

                if active_config.throttle_seconds > 0:
                    time.sleep(active_config.throttle_seconds)
    finally:
        if connected_here:
            client.disconnect()

    result = IngestionRunResult(
        run_id=run_id,
        generated_at_utc=_now_utc().isoformat(),
        symbols_requested=len(requested_symbols),
        symbols_succeeded=len(results),
        symbols_failed=len(failures),
        results=results,
        failures=failures,
    )
    metadata_path = _write_run_metadata(
        result,
        output_dir=Path(base_dir) / active_config.run_metadata_dir,
    )

    return IngestionRunResult(
        run_id=result.run_id,
        generated_at_utc=result.generated_at_utc,
        symbols_requested=result.symbols_requested,
        symbols_succeeded=result.symbols_succeeded,
        symbols_failed=result.symbols_failed,
        results=result.results,
        failures=result.failures,
        metadata_file=str(metadata_path),
    )
