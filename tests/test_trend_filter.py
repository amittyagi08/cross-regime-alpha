from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from cross_regime_alpha.signals import TrendConfig, apply_trend_eligibility_filter


def _write_symbol_part(base: Path, symbol: str, year: int, month: int, frame: pd.DataFrame, *, name: str) -> None:
    target = base / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
    target.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target / f"{name}.parquet", index=False)


def test_trend_flags_and_report(tmp_path: Path) -> None:
    source_dir = tmp_path / "data/cache/signals/daily"
    frame = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
            "adj_close": [105.0, 100.0, 110.0, None],
            "sma200": [100.0, 100.0, 110.0, 100.0],
            "sma50": [101.0, 101.0, 111.0, 101.0],
            "pulled_at_utc": pd.to_datetime(["2024-01-10", "2024-01-10", "2024-01-10", "2024-01-10"]),
        }
    )
    _write_symbol_part(source_dir, "AAPL", 2024, 1, frame, name="part-a")

    config = TrendConfig(
        source_dir="data/cache/signals/daily",
        fallback_source_dir="data/cache/features/daily",
        output_dir="data/cache/signals/daily",
        report_dir="outputs/runs",
    )
    result = apply_trend_eligibility_filter(["AAPL"], config=config, base_dir=tmp_path, run_id="run-trend-01")

    assert result.total_rows == 4
    assert result.trend_known_rows == 3
    assert result.trend_eligible_rows == 1

    out_files = sorted((tmp_path / "data/cache/signals/daily/symbol=AAPL").glob("**/*.parquet"))
    assert out_files, "Expected trend output parquet files"

    out = pd.concat([pd.read_parquet(f) for f in out_files], ignore_index=True).sort_values("date")
    assert list(out["trend_known"]) == [True, True, True, False]
    assert list(out["trend_eligible"]) == [True, False, False, False]

    report = json.loads((tmp_path / "outputs/runs/run-trend-01/trend_report.json").read_text(encoding="utf-8"))
    assert report["trend_known_rows"] == 3
    assert report["trend_eligible_rows"] == 1


def test_upsert_latest_replaces_older_partitions(tmp_path: Path) -> None:
    source_dir = tmp_path / "data/cache/signals/daily"
    older = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "adj_close": [101.0, 102.0],
            "sma200": [100.0, 100.0],
            "sma50": [101.0, 101.0],
            "pulled_at_utc": pd.to_datetime(["2024-01-05T10:00:00Z", "2024-01-05T10:00:00Z"]),
        }
    )
    newer = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "adj_close": [111.0, 112.0],
            "sma200": [100.0, 100.0],
            "sma50": [101.0, 101.0],
            "pulled_at_utc": pd.to_datetime(["2024-01-06T10:00:00Z", "2024-01-06T10:00:00Z"]),
        }
    )
    _write_symbol_part(source_dir, "AAPL", 2024, 1, older, name="part-older")
    _write_symbol_part(source_dir, "AAPL", 2024, 1, newer, name="part-newer")

    result = apply_trend_eligibility_filter(["AAPL"], base_dir=tmp_path, run_id="run-trend-02")
    assert result.total_rows == 2

    out_files = sorted((tmp_path / "data/cache/signals/daily/symbol=AAPL").glob("**/*.parquet"))
    assert len(out_files) == 1

    out = pd.read_parquet(out_files[0]).sort_values("date")
    assert list(out["adj_close"]) == [111.0, 112.0]


def test_fallback_to_feature_cache_when_signal_cache_missing(tmp_path: Path) -> None:
    feature_dir = tmp_path / "data/cache/features/daily"
    frame = pd.DataFrame(
        {
            "symbol": ["MSFT"],
            "date": pd.to_datetime(["2024-02-01"]),
            "adj_close": [420.0],
            "sma200": [400.0],
            "sma50": [410.0],
        }
    )
    _write_symbol_part(feature_dir, "MSFT", 2024, 2, frame, name="part-f")

    result = apply_trend_eligibility_filter(["MSFT"], base_dir=tmp_path, run_id="run-trend-03")
    assert result.total_rows == 1
    assert result.trend_known_rows == 1
    assert result.trend_eligible_rows == 1
