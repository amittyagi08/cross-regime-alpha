from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from cross_regime_alpha.indicators import IndicatorConfig, compute_indicators_from_cleaned_cache


def _write_cleaned_parquet(tmp_path: Path, symbol: str, rows: list[dict]) -> Path:
    target = (
        tmp_path
        / "data"
        / "cache"
        / "ibkr"
        / "cleaned"
        / "daily"
        / f"symbol={symbol}"
        / "year=2026"
        / "month=02"
    )
    target.mkdir(parents=True, exist_ok=True)
    file_path = target / "part-test.parquet"
    pd.DataFrame(rows).to_parquet(file_path, index=False)
    return file_path


def _row(symbol: str, date_text: str, close: float, volume: int = 1000) -> dict:
    return {
        "symbol": symbol,
        "date": date_text,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "adj_close": close,
        "volume": volume,
        "is_missing_bar": False,
    }


def _read_features(files: list[str]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in files]
    return pd.concat(frames, ignore_index=True)


def test_indicator_engine_computes_expected_columns(tmp_path: Path) -> None:
    rows = [_row("AAPL", f"2026-02-{day:02d}", 100 + day, 1000 + day) for day in range(1, 26)]
    _write_cleaned_parquet(tmp_path, "AAPL", rows)

    config = IndicatorConfig(
        sma200_period=10,
        sma50_period=5,
        ema20_period=5,
        rsi14_period=5,
        atr14_period=5,
        rolling_high_period=5,
        volume_sma50_period=5,
    )
    result = compute_indicators_from_cleaned_cache(["AAPL"], config=config, base_dir=tmp_path)

    features = _read_features(result.feature_files).sort_values("date")
    expected = {
        "sma200",
        "sma50",
        "ema20",
        "rsi14",
        "atr14",
        "rolling_high_20",
        "volume_sma50",
        "indicator_ready",
    }
    assert expected.issubset(set(features.columns))
    assert result.total_rows == len(features)
    assert result.indicator_ready_rows > 0


def test_indicator_engine_warmup_marks_not_ready(tmp_path: Path) -> None:
    rows = [_row("SPY", f"2026-02-{day:02d}", 200 + day, 2000 + day) for day in range(1, 9)]
    _write_cleaned_parquet(tmp_path, "SPY", rows)

    config = IndicatorConfig(
        sma200_period=20,
        sma50_period=10,
        ema20_period=10,
        rsi14_period=10,
        atr14_period=10,
        rolling_high_period=10,
        volume_sma50_period=10,
    )
    result = compute_indicators_from_cleaned_cache(["SPY"], config=config, base_dir=tmp_path)

    features = _read_features(result.feature_files)
    assert bool(features["indicator_ready"].any()) is False
    assert result.indicator_ready_rows == 0


def test_indicator_engine_writes_report_file(tmp_path: Path) -> None:
    rows = [_row("SPY", f"2026-02-{day:02d}", 300 + day, 3000 + day) for day in range(1, 16)]
    _write_cleaned_parquet(tmp_path, "SPY", rows)

    config = IndicatorConfig(
        sma200_period=8,
        sma50_period=5,
        ema20_period=5,
        rsi14_period=5,
        atr14_period=5,
        rolling_high_period=5,
        volume_sma50_period=5,
    )
    result = compute_indicators_from_cleaned_cache(["SPY"], config=config, base_dir=tmp_path)

    report_file = Path(result.report_file)
    assert report_file.exists()
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["symbols"] == ["SPY"]
    assert payload["total_rows"] == result.total_rows
    assert payload["config"]["include_volume_sma50"] is True


def test_indicator_engine_dedupes_symbol_date_keep_latest(tmp_path: Path) -> None:
    base_rows = [_row("SPY", f"2026-02-{day:02d}", 300 + day, 3000 + day) for day in range(1, 12)]
    dup_old = {**_row("SPY", "2026-02-10", 310.0, 3100), "pulled_at_utc": "2026-03-01T10:00:00+00:00"}
    dup_new = {**_row("SPY", "2026-02-10", 315.0, 3150), "pulled_at_utc": "2026-03-01T12:00:00+00:00"}
    rows = []
    for item in base_rows:
        rows.append({**item, "pulled_at_utc": "2026-03-01T09:00:00+00:00"})
    rows.append(dup_old)
    rows.append(dup_new)

    _write_cleaned_parquet(tmp_path, "SPY", rows)

    config = IndicatorConfig(
        sma200_period=5,
        sma50_period=5,
        ema20_period=5,
        rsi14_period=5,
        atr14_period=5,
        rolling_high_period=5,
        volume_sma50_period=5,
    )
    result = compute_indicators_from_cleaned_cache(["SPY"], config=config, base_dir=tmp_path)
    features = _read_features(result.feature_files)

    assert int(features.duplicated(["symbol", "date"]).sum()) == 0
    target = features.loc[features["date"].astype(str) == "2026-02-10"]
    assert len(target) == 1
    assert float(target.iloc[0]["close"]) == 315.0
