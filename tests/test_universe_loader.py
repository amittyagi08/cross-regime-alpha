from __future__ import annotations

import csv
from pathlib import Path

from cross_regime_alpha.data.universe import (
    load_universe_from_config,
    resolve_universe,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_resolve_universe_from_csv_header(tmp_path: Path) -> None:
    source = tmp_path / "tickers.csv"
    _write(source, "ticker\nAAPL\nMSFT\nAAPL\nbad!\n")

    result = resolve_universe(source)

    assert result.tickers == ["AAPL", "MSFT"]
    assert result.invalid_tickers == ["BAD!"]
    assert result.duplicate_count == 1


def test_resolve_universe_with_include_exclude(tmp_path: Path) -> None:
    source = tmp_path / "tickers.csv"
    include = tmp_path / "include.txt"
    exclude = tmp_path / "exclude.json"
    _write(source, "ticker\nAAPL\nMSFT\n")
    _write(include, "NVDA\nMETA\n")
    _write(exclude, '["MSFT"]')

    result = resolve_universe(source, include_file=include, exclude_file=exclude)

    assert result.tickers == ["AAPL", "META", "NVDA"]


def test_resolve_universe_from_json_object(tmp_path: Path) -> None:
    source = tmp_path / "tickers.json"
    _write(source, '{"tickers": ["brk.b", "bf.b", "spy"]}')

    result = resolve_universe(source)

    assert result.tickers == ["BF.B", "BRK.B", "SPY"]


def test_load_universe_from_config_writes_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "tickers.csv"
    _write(source, "ticker\nAAPL\nMSFT\n")

    config = {
        "universe": {
            "tickers_file": str(source.relative_to(tmp_path)),
            "resolved_output_file": "outputs/universe/resolved_tickers.csv",
        }
    }

    result = load_universe_from_config(config, base_dir=tmp_path)
    output = tmp_path / "outputs" / "universe" / "resolved_tickers.csv"

    assert result.tickers == ["AAPL", "MSFT"]
    assert output.exists()

    with output.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["ticker"]
    assert rows[1] == ["AAPL"]
    assert rows[2] == ["MSFT"]
