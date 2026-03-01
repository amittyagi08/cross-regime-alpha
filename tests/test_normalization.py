from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from cross_regime_alpha.data.normalization import NormalizationConfig, normalize_daily_data_cache


def _write_input_parquet(tmp_path: Path, symbol: str, rows: list[dict]) -> Path:
    target = (
        tmp_path
        / "data"
        / "cache"
        / "ibkr"
        / "normalized"
        / "daily"
        / f"symbol={symbol}"
        / "year=2026"
        / "month=02"
    )
    target.mkdir(parents=True, exist_ok=True)
    file_path = target / "part-test.parquet"
    pd.DataFrame(rows).to_parquet(file_path, index=False)
    return file_path


def _base_row(symbol: str, date: str, close: float) -> dict:
    return {
        "symbol": symbol,
        "date": date,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "adj_close": close,
        "volume": 1000,
        "adjustment_factor": 1.0,
        "adjustment_method": "none",
        "what_to_show": "TRADES",
        "exchange": "SMART",
        "currency": "USD",
        "source": "ibkr",
        "pulled_at_utc": "2026-03-01T00:00:00+00:00",
    }


def _read_cleaned_frames(cleaned_files: list[str]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in cleaned_files]
    return pd.concat(frames, ignore_index=True)


def test_normalization_removes_duplicates_and_invalid_rows(tmp_path: Path) -> None:
    rows = [
        _base_row("SPY", "2026-02-24", 100.0),
        _base_row("SPY", "2026-02-24", 101.0),
        {**_base_row("SPY", "2026-02-25", 102.0), "close": -1.0},
    ]
    _write_input_parquet(tmp_path, "SPY", rows)

    result = normalize_daily_data_cache(["SPY"], base_dir=tmp_path)

    assert result.quality_summary.duplicate_rows_removed == 1
    assert result.quality_summary.invalid_rows_removed == 1

    cleaned = _read_cleaned_frames(result.cleaned_files)
    valid_rows = cleaned.loc[~cleaned["is_missing_bar"]]
    assert len(valid_rows) == 1
    assert float(valid_rows.iloc[0]["close"]) == 101.0


def test_normalization_aligns_calendar_and_marks_missing_rows(tmp_path: Path) -> None:
    _write_input_parquet(
        tmp_path,
        "SPY",
        [
            _base_row("SPY", "2026-02-24", 100.0),
            _base_row("SPY", "2026-02-25", 101.0),
        ],
    )
    _write_input_parquet(
        tmp_path,
        "AAPL",
        [
            _base_row("AAPL", "2026-02-24", 200.0),
        ],
    )

    result = normalize_daily_data_cache(["AAPL", "SPY"], base_dir=tmp_path)

    cleaned = _read_cleaned_frames(result.cleaned_files)
    assert result.quality_summary.missing_bar_rows == 1
    missing = cleaned[(cleaned["symbol"] == "AAPL") & (cleaned["date"].astype(str) == "2026-02-25")]
    assert len(missing) == 1
    assert bool(missing.iloc[0]["is_missing_bar"]) is True


def test_normalization_flags_outlier_jumps_and_writes_report(tmp_path: Path) -> None:
    _write_input_parquet(
        tmp_path,
        "SPY",
        [
            _base_row("SPY", "2026-02-24", 100.0),
            _base_row("SPY", "2026-02-25", 150.0),
        ],
    )

    config = NormalizationConfig(outlier_return_threshold=0.20)
    result = normalize_daily_data_cache(["SPY"], config=config, base_dir=tmp_path)

    cleaned = _read_cleaned_frames(result.cleaned_files)
    outlier_rows = cleaned.loc[cleaned["is_outlier_jump"] == True]  # noqa: E712
    assert len(outlier_rows) == 1
    assert result.quality_summary.outlier_rows_flagged == 1

    report_file = Path(result.quality_report_file)
    assert report_file.exists()
    payload = json.loads(report_file.read_text(encoding="utf-8"))
    assert payload["quality_summary"]["outlier_rows_flagged"] == 1
