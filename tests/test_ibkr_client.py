from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cross_regime_alpha.brokers.ibkr import IBKRClient, IBKRConnectionError, IBKRSettings


class FakeIB:
    def __init__(self) -> None:
        self.connected = False
        self.connect_called = False
        self.disconnect_called = False
        self.raise_on_connect: Exception | None = None
        self.raise_on_time: Exception | None = None

    def connect(self, host: str, port: int, clientId: int, readonly: bool, timeout: int) -> None:  # noqa: N803
        self.connect_called = True
        if self.raise_on_connect:
            raise self.raise_on_connect
        self.connected = True

    def disconnect(self) -> None:
        self.disconnect_called = True
        self.connected = False

    def isConnected(self) -> bool:  # noqa: N802
        return self.connected

    def reqCurrentTime(self) -> datetime:
        if self.raise_on_time:
            raise self.raise_on_time
        return datetime(2026, 2, 28, 12, 30, tzinfo=timezone.utc)


def test_settings_from_env_file(tmp_path) -> None:
    env_file = tmp_path / ".env.ibkr"
    env_file.write_text(
        "\n".join(
            [
                "IBKR_HOST=192.168.1.10",
                "IBKR_PORT=4002",
                "IBKR_CLIENT_ID=42",
                "IBKR_READONLY=false",
                "IBKR_TIMEOUT_SECONDS=15",
                "IBKR_ACCOUNT=DU123456",
            ]
        ),
        encoding="utf-8",
    )

    settings = IBKRSettings.from_env(env_file=env_file)

    assert settings.host == "192.168.1.10"
    assert settings.port == 4002
    assert settings.client_id == 42
    assert settings.readonly is False
    assert settings.timeout_seconds == 15
    assert settings.account == "DU123456"


def test_env_vars_override_env_file(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env.ibkr"
    env_file.write_text(
        "\n".join(
            [
                "IBKR_HOST=10.0.0.5",
                "IBKR_PORT=4001",
                "IBKR_CLIENT_ID=99",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7496")
    monkeypatch.setenv("IBKR_CLIENT_ID", "1")

    settings = IBKRSettings.from_env(env_file=env_file)

    assert settings.host == "127.0.0.1"
    assert settings.port == 7496
    assert settings.client_id == 1


def test_client_from_env_uses_loaded_settings(tmp_path) -> None:
    env_file = tmp_path / ".env.ibkr"
    env_file.write_text(
        "\n".join(
            [
                "IBKR_HOST=127.0.0.1",
                "IBKR_PORT=7496",
                "IBKR_CLIENT_ID=7",
                "IBKR_READONLY=true",
                "IBKR_TIMEOUT_SECONDS=12",
            ]
        ),
        encoding="utf-8",
    )
    fake = FakeIB()

    client = IBKRClient.from_env(env_file=env_file, ib_factory=lambda: fake)
    client.connect()

    assert client.settings.port == 7496
    assert client.settings.client_id == 7
    assert client.settings.timeout_seconds == 12
    assert client.is_connected() is True


def test_connect_and_health_check_success() -> None:
    fake = FakeIB()
    client = IBKRClient(IBKRSettings(), ib_factory=lambda: fake)

    client.connect()
    status = client.health_check()

    assert fake.connect_called is True
    assert client.is_connected() is True
    assert status.connected is True
    assert status.server_time_utc is not None


def test_connect_error_has_clear_message() -> None:
    fake = FakeIB()
    fake.raise_on_connect = RuntimeError("connection refused")
    client = IBKRClient(IBKRSettings(), ib_factory=lambda: fake)

    with pytest.raises(IBKRConnectionError) as exc_info:
        client.connect()

    assert "connection refused" in str(exc_info.value).lower()


def test_health_check_when_time_lookup_fails() -> None:
    fake = FakeIB()
    client = IBKRClient(IBKRSettings(), ib_factory=lambda: fake)
    client.connect()
    fake.raise_on_time = RuntimeError("permission denied")

    status = client.health_check()

    assert status.connected is True
    assert "failed to query server time" in status.message.lower()
