from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from cross_regime_alpha.signals import apply_market_regime_filter


def _write_feature_parquet(tmp_path: Path, symbol: str, rows: list[dict]) -> None:
    target = (
        tmp_path
        / "data"
        / "cache"
        / "features"
        / "daily"
        / f"symbol={symbol}"
        / "year=2026"
        / "month=02"
    )
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(target / "part-test.parquet", index=False)


def _row(symbol: str, date_text: str, adj_close: float, sma200: float | None) -> dict:
    return {
        "symbol": symbol,
        "date": date_text,
        "adj_close": adj_close,
        "sma200": sma200,
        "pulled_at_utc": "2026-03-01T10:00:00+00:00",
        "open": adj_close,
        "high": adj_close,
        "low": adj_close,
        "close": adj_close,
        "volume": 1,
        "is_missing_bar": False,
    }


def test_regime_filter_maps_spy_state_to_aapl(tmp_path: Path) -> None:
    _write_feature_parquet(
        tmp_path,
        "SPY",
        [
            _row("SPY", "2026-02-24", 100.0, 90.0),
            _row("SPY", "2026-02-25", 90.0, 90.0),
            _row("SPY", "2026-02-26", 95.0, None),
        ],
    )
    _write_feature_parquet(
        tmp_path,
        "AAPL",
        [
            _row("AAPL", "2026-02-24", 200.0, 180.0),
            _row("AAPL", "2026-02-25", 201.0, 181.0),
            _row("AAPL", "2026-02-26", 202.0, 182.0),
        ],
    )

    result = apply_market_regime_filter(["AAPL"], base_dir=tmp_path)
    files = [Path(path) for path in result.output_files]
    merged = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"]).dt.strftime("%Y-%m-%d")

    d1 = merged.loc[merged["date"] == "2026-02-24"].iloc[0]
    d2 = merged.loc[merged["date"] == "2026-02-25"].iloc[0]
    d3 = merged.loc[merged["date"] == "2026-02-26"].iloc[0]

    assert bool(d1["regime_known"]) is True
    assert bool(d1["regime_on"]) is True
    assert bool(d2["regime_known"]) is True
    assert bool(d2["regime_on"]) is False
    assert bool(d3["regime_known"]) is False
    assert bool(d3["regime_on"]) is False


def test_regime_filter_keeps_latest_benchmark_row(tmp_path: Path) -> None:
    _write_feature_parquet(
        tmp_path,
        "SPY",
        [
            {**_row("SPY", "2026-02-24", 80.0, 90.0), "pulled_at_utc": "2026-03-01T09:00:00+00:00"},
            {**_row("SPY", "2026-02-24", 100.0, 90.0), "pulled_at_utc": "2026-03-01T12:00:00+00:00"},
        ],
    )
    _write_feature_parquet(tmp_path, "AAPL", [_row("AAPL", "2026-02-24", 200.0, 180.0)])

    result = apply_market_regime_filter(["AAPL"], base_dir=tmp_path)
    merged = pd.concat([pd.read_parquet(path) for path in result.output_files], ignore_index=True)
    assert int(merged.duplicated(["symbol", "date"]).sum()) == 0
    assert bool(merged.iloc[0]["regime_on"]) is True


def test_regime_filter_writes_report(tmp_path: Path) -> None:
    _write_feature_parquet(tmp_path, "SPY", [_row("SPY", "2026-02-24", 100.0, 90.0)])
    _write_feature_parquet(tmp_path, "AAPL", [_row("AAPL", "2026-02-24", 200.0, 180.0)])

    result = apply_market_regime_filter(["AAPL"], base_dir=tmp_path)
    report = Path(result.report_file)
    assert report.exists()
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["benchmark_symbol"] == "SPY"
    assert payload["regime_known_rows"] == 1
    assert payload["regime_on_rows"] == 1
