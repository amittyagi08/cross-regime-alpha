from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,14}$")


@dataclass(frozen=True)
class UniverseLoadResult:
    tickers: list[str]
    invalid_tickers: list[str]
    duplicate_count: int


def _normalize_ticker(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _is_valid_ticker(ticker: str) -> bool:
    return bool(TICKER_PATTERN.match(ticker))


def _extract_ticker_from_row(row: dict[str, Any]) -> str:
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in ("ticker", "symbol", "symbols", "tickers"):
        if key in lowered:
            return _normalize_ticker(lowered[key])
    if row:
        return _normalize_ticker(next(iter(row.values())))
    return ""


def _read_csv(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        first_line = ""
        for line in sample.splitlines():
            stripped = line.strip()
            if stripped:
                first_line = stripped
                break
        first_token = first_line.split(",")[0].strip().lower() if first_line else ""
        has_header = first_token in {"ticker", "tickers", "symbol", "symbols"}
        if has_header:
            reader = csv.DictReader(f)
            return [_extract_ticker_from_row(r) for r in reader]
        raw = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            raw.append(_normalize_ticker(line.split(",")[0]))
        return raw


def _read_txt(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig") as f:
        return [_normalize_ticker(line) for line in f if line.strip()]


def _read_json(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        tickers: list[str] = []
        for item in data:
            if isinstance(item, dict):
                tickers.append(_extract_ticker_from_row(item))
            else:
                tickers.append(_normalize_ticker(item))
        return tickers
    if isinstance(data, dict):
        for key in ("tickers", "symbols"):
            value = data.get(key)
            if isinstance(value, list):
                return [_normalize_ticker(x) for x in value]
    raise ValueError(f"Unsupported JSON structure in {path}")


def _read_tickers(path: Path) -> list[str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".txt":
        return _read_txt(path)
    if suffix == ".json":
        return _read_json(path)
    raise ValueError(f"Unsupported universe file type: {path.suffix}")


def _resolve_path(value: str | None, base_dir: str | Path = ".") -> Path | None:
    if not value:
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    return Path(base_dir) / path


def resolve_universe(
    tickers_file: str | Path,
    *,
    include_file: str | Path | None = None,
    exclude_file: str | Path | None = None,
    base_dir: str | Path = ".",
) -> UniverseLoadResult:
    tickers_path = _resolve_path(str(tickers_file), base_dir=base_dir)
    if tickers_path is None or not tickers_path.exists():
        raise FileNotFoundError(f"Universe file not found: {tickers_file}")

    include_path = _resolve_path(str(include_file), base_dir=base_dir) if include_file else None
    exclude_path = _resolve_path(str(exclude_file), base_dir=base_dir) if exclude_file else None

    raw = _read_tickers(tickers_path)
    if include_path:
        if not include_path.exists():
            raise FileNotFoundError(f"Include file not found: {include_file}")
        raw.extend(_read_tickers(include_path))

    excludes: set[str] = set()
    if exclude_path:
        if not exclude_path.exists():
            raise FileNotFoundError(f"Exclude file not found: {exclude_file}")
        excludes = {t for t in _read_tickers(exclude_path) if t}

    invalid: list[str] = []
    seen: set[str] = set()
    valid: list[str] = []
    duplicates = 0

    for ticker in raw:
        if not ticker:
            continue
        if not _is_valid_ticker(ticker):
            invalid.append(ticker)
            continue
        if ticker in excludes:
            continue
        if ticker in seen:
            duplicates += 1
            continue
        seen.add(ticker)
        valid.append(ticker)

    valid.sort()
    return UniverseLoadResult(
        tickers=valid,
        invalid_tickers=sorted(set(invalid)),
        duplicate_count=duplicates,
    )


def save_resolved_universe(
    result: UniverseLoadResult,
    output_file: str | Path,
    *,
    source_file: str | Path | None = None,
    include_file: str | Path | None = None,
    exclude_file: str | Path | None = None,
) -> Path:
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "source_file": str(source_file) if source_file else None,
        "include_file": str(include_file) if include_file else None,
        "exclude_file": str(exclude_file) if exclude_file else None,
        "ticker_count": len(result.tickers),
        "invalid_ticker_count": len(result.invalid_tickers),
        "duplicate_count": result.duplicate_count,
    }

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        for ticker in result.tickers:
            writer.writerow([ticker])
        if result.invalid_tickers:
            writer.writerow([])
            writer.writerow(["invalid_tickers"])
            for ticker in result.invalid_tickers:
                writer.writerow([ticker])
        writer.writerow([])
        writer.writerow(["metadata"])
        for key, value in metadata.items():
            writer.writerow([key, "" if value is None else value])

    return output_path


def load_universe_from_config(config: dict[str, Any], *, base_dir: str | Path = ".") -> UniverseLoadResult:
    universe_cfg = config.get("universe", {})
    tickers_file = universe_cfg.get("tickers_file")
    if not tickers_file:
        raise ValueError("Missing required config key: universe.tickers_file")

    include_file = universe_cfg.get("include_file")
    exclude_file = universe_cfg.get("exclude_file")
    output_file = universe_cfg.get("resolved_output_file")

    result = resolve_universe(
        tickers_file=tickers_file,
        include_file=include_file,
        exclude_file=exclude_file,
        base_dir=base_dir,
    )

    if output_file:
        output_path = _resolve_path(output_file, base_dir=base_dir)
        if output_path is None:
            raise ValueError("Invalid config key: universe.resolved_output_file")
        save_resolved_universe(
            result,
            output_path,
            source_file=tickers_file,
            include_file=include_file,
            exclude_file=exclude_file,
        )

    return result
