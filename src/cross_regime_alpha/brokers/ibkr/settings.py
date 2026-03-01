from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

DEFAULT_ENV_FILE = ".env"


def _parse_bool(value: str | bool | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _parse_int(value: str | int | None, *, default: int, name: str) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    stripped = str(value).strip()
    if not stripped:
        return default
    try:
        return int(stripped)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {name}: {value}") from exc


@dataclass(frozen=True)
class IBKRSettings:
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 1
    readonly: bool = True
    timeout_seconds: int = 10
    account: str | None = None

    @classmethod
    def from_env(
        cls,
        *,
        env_file: str | Path | None = None,
        base_dir: str | Path = ".",
        load_default_env_file: bool = True,
        env_prefix: str = "IBKR_",
    ) -> "IBKRSettings":
        file_values: dict[str, str | None] = {}
        file_path: Path | None = None

        if env_file:
            file_path = Path(env_file)
            if not file_path.is_absolute():
                file_path = Path(base_dir) / file_path
            if not file_path.exists():
                raise FileNotFoundError(f"IBKR env file not found: {env_file}")
        elif load_default_env_file:
            candidate = Path(base_dir) / DEFAULT_ENV_FILE
            if candidate.exists():
                file_path = candidate

        if file_path is not None:
            file_values = dict(dotenv_values(file_path))

        def get_value(key: str) -> str | None:
            env_key = f"{env_prefix}{key}"
            if env_key in os.environ:
                return os.environ[env_key]
            raw = file_values.get(env_key)
            if raw is None:
                return None
            return str(raw)

        host = (get_value("HOST") or cls.host).strip()
        port = _parse_int(get_value("PORT"), default=cls.port, name=f"{env_prefix}PORT")
        client_id = _parse_int(
            get_value("CLIENT_ID"),
            default=cls.client_id,
            name=f"{env_prefix}CLIENT_ID",
        )
        timeout_seconds = _parse_int(
            get_value("TIMEOUT_SECONDS"),
            default=cls.timeout_seconds,
            name=f"{env_prefix}TIMEOUT_SECONDS",
        )
        readonly = _parse_bool(get_value("READONLY"), default=cls.readonly)
        account = get_value("ACCOUNT")
        account = account.strip() if account else None

        return cls(
            host=host,
            port=port,
            client_id=client_id,
            readonly=readonly,
            timeout_seconds=timeout_seconds,
            account=account,
        )
