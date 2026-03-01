from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .settings import IBKRSettings


class IBKRConnectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class IBKRHealthStatus:
    connected: bool
    message: str
    server_time_utc: str | None = None


class IBKRClient:
    def __init__(
        self,
        settings: IBKRSettings,
        *,
        ib_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        self._ib_factory = ib_factory or self._default_ib_factory
        self._ib: Any | None = None

    @staticmethod
    def _default_ib_factory() -> Any:
        from ib_insync import IB

        return IB()

    @classmethod
    def from_env(
        cls,
        *,
        env_file: str | Path | None = None,
        base_dir: str | Path = ".",
        load_default_env_file: bool = True,
        env_prefix: str = "IBKR_",
        ib_factory: Callable[[], Any] | None = None,
    ) -> "IBKRClient":
        settings = IBKRSettings.from_env(
            env_file=env_file,
            base_dir=base_dir,
            load_default_env_file=load_default_env_file,
            env_prefix=env_prefix,
        )
        return cls(settings=settings, ib_factory=ib_factory)

    def connect(self) -> None:
        if self._ib is None:
            self._ib = self._ib_factory()

        try:
            self._ib.connect(
                self.settings.host,
                self.settings.port,
                clientId=self.settings.client_id,
                readonly=self.settings.readonly,
                timeout=self.settings.timeout_seconds,
            )
            if not self._ib.isConnected():
                raise IBKRConnectionError(
                    "IBKR connection failed: connected=False. "
                    "Verify TWS/Gateway is running and API access is enabled."
                )
        except IBKRConnectionError:
            raise
        except Exception as exc:  # pragma: no cover - exact exception type is broker-lib specific
            raise IBKRConnectionError(self._format_connect_error(exc)) from exc

    def disconnect(self) -> None:
        if self._ib is not None and self._ib.isConnected():
            self._ib.disconnect()

    def is_connected(self) -> bool:
        return bool(self._ib and self._ib.isConnected())

    def health_check(self) -> IBKRHealthStatus:
        if self._ib is None or not self._ib.isConnected():
            return IBKRHealthStatus(
                connected=False,
                message="IBKR not connected",
                server_time_utc=None,
            )

        try:
            server_time = self._ib.reqCurrentTime()
            if isinstance(server_time, datetime):
                if server_time.tzinfo is None:
                    server_time = server_time.replace(tzinfo=timezone.utc)
                server_time_utc = server_time.astimezone(timezone.utc).isoformat()
            else:
                server_time_utc = str(server_time)
            return IBKRHealthStatus(
                connected=True,
                message="IBKR connection healthy",
                server_time_utc=server_time_utc,
            )
        except Exception as exc:  # pragma: no cover - depends on live broker behavior
            return IBKRHealthStatus(
                connected=True,
                message=f"Connected but failed to query server time: {exc}",
                server_time_utc=None,
            )

    def request_historical_data(self, contract: Any, **kwargs: Any) -> list[Any]:
        if self._ib is None or not self._ib.isConnected():
            raise IBKRConnectionError("IBKR is not connected. Call connect() before requesting data.")
        try:
            return self._ib.reqHistoricalData(contract, **kwargs)
        except Exception as exc:  # pragma: no cover - depends on broker library exceptions
            raise IBKRConnectionError(f"IBKR historical data request failed: {exc}") from exc

    @staticmethod
    def _format_connect_error(error: Exception) -> str:
        raw = str(error).strip() or error.__class__.__name__
        lowered = raw.lower()
        if "timeout" in lowered:
            return (
                "IBKR connection timeout. Check host/port, ensure TWS or IB Gateway is running, "
                "and confirm API connections are allowed."
            )
        if "refused" in lowered or "actively refused" in lowered:
            return (
                "IBKR connection refused. Verify host/port and enable API in TWS/Gateway settings."
            )
        if "clientid" in lowered or "client id" in lowered or "duplicate" in lowered:
            return (
                "IBKR client ID conflict. Use a different IBKR_CLIENT_ID and reconnect."
            )
        if "permission" in lowered or "not subscribed" in lowered:
            return (
                "IBKR permission/subscription error. Verify market data permissions for this account."
            )
        if "auth" in lowered or "login" in lowered:
            return (
                "IBKR authentication error. Confirm account login is active in TWS/Gateway session."
            )
        return f"IBKR connection error: {raw}"
