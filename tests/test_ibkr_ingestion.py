from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from cross_regime_alpha.data.ibkr_ingestion import DailyIngestionConfig, ingest_daily_bars


@dataclass
class FakeBar:
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


class FakeClient:
    def __init__(self, responses: dict[tuple[str, str], list[FakeBar] | Exception]) -> None:
        self._connected = False
        self._responses = responses
        self.calls: list[tuple[str, str]] = []

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def request_historical_data(self, contract, **kwargs):
        what = kwargs.get("whatToShow", "")
        key = (contract.symbol, what)
        self.calls.append(key)
        response = self._responses.get(key, [])
        if isinstance(response, Exception):
            raise response
        if isinstance(response, list) and response and isinstance(response[0], Exception):
            error = response.pop(0)
            raise error
        return response


def _bar(day: int, close: float, volume: int = 1000) -> FakeBar:
    return FakeBar(
        date=date(2026, 2, day),
        open=close - 1.0,
        high=close + 1.0,
        low=close - 2.0,
        close=close,
        volume=volume,
    )


def test_ingest_applies_adjusted_last_policy(tmp_path: Path) -> None:
    responses = {
        ("SPY", "TRADES"): [_bar(24, 100.0), _bar(25, 110.0)],
        ("SPY", "ADJUSTED_LAST"): [_bar(24, 95.0), _bar(25, 108.9)],
    }
    client = FakeClient(responses)
    config = DailyIngestionConfig(
        raw_cache_dir="data/cache/ibkr/raw/daily",
        normalized_cache_dir="data/cache/ibkr/normalized/daily",
        run_metadata_dir="outputs/runs",
        throttle_seconds=0,
    )

    result = ingest_daily_bars(client, ["spy"], config=config, base_dir=tmp_path)

    assert result.symbols_succeeded == 1
    symbol_result = result.results[0]
    assert symbol_result.adjustment_method == "ibkr_adjusted_last_factor"
    assert len(symbol_result.raw_files) == 1
    assert len(symbol_result.normalized_files) == 1

    normalized = pd.read_parquet(symbol_result.normalized_files[0])
    assert list(normalized["symbol"].unique()) == ["SPY"]
    assert list(normalized["adj_close"].round(2)) == [95.0, 108.9]


def test_ingest_falls_back_when_adjusted_unavailable(tmp_path: Path) -> None:
    responses = {
        ("SPY", "TRADES"): [_bar(24, 100.0), _bar(25, 110.0)],
        ("SPY", "ADJUSTED_LAST"): [],
    }
    client = FakeClient(responses)
    config = DailyIngestionConfig(throttle_seconds=0)

    result = ingest_daily_bars(client, ["SPY"], config=config, base_dir=tmp_path)

    symbol_result = result.results[0]
    assert symbol_result.adjustment_method == "none"
    assert symbol_result.warnings
    normalized = pd.read_parquet(symbol_result.normalized_files[0])
    assert list(normalized["adj_close"].round(2)) == [100.0, 110.0]


def test_ingest_retries_after_transient_error(tmp_path: Path) -> None:
    responses = {
        ("SPY", "TRADES"): [RuntimeError("temporary pacing"), _bar(24, 100.0), _bar(25, 101.0)],
        ("SPY", "ADJUSTED_LAST"): [_bar(24, 99.0), _bar(25, 100.5)],
    }
    client = FakeClient(responses)
    config = DailyIngestionConfig(throttle_seconds=0, retry_delay_seconds=0)

    result = ingest_daily_bars(client, ["SPY"], config=config, base_dir=tmp_path)

    assert result.symbols_succeeded == 1
    assert client.calls.count(("SPY", "TRADES")) == 2


def test_ingest_writes_partitioned_paths(tmp_path: Path) -> None:
    responses = {
        ("SPY", "TRADES"): [_bar(27, 120.0), _bar(28, 121.0), FakeBar(date(2026, 3, 2), 122, 123, 121, 122, 3000)],
        ("SPY", "ADJUSTED_LAST"): [_bar(27, 119.0), _bar(28, 120.0), FakeBar(date(2026, 3, 2), 121, 122, 120, 121, 3000)],
    }
    client = FakeClient(responses)
    config = DailyIngestionConfig(throttle_seconds=0)

    result = ingest_daily_bars(client, ["SPY"], config=config, base_dir=tmp_path)

    files = result.results[0].normalized_files
    assert len(files) == 2
    assert any("month=02" in path for path in files)
    assert any("month=03" in path for path in files)
    assert result.metadata_file is not None
    assert Path(result.metadata_file).exists()
