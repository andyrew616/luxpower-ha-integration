"""Tests for the supported Home Assistant-independent read client."""

import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from luxpower import (
    LuxPowerCommunicationError,
    LuxPowerReadClient,
    LuxPowerTelemetry,
    TelemetryGroup,
    input_register_group,
)


def make_client(**options):
    """Create a read-only client with reserved documentation addresses."""
    return LuxPowerReadClient(
        host="192.0.2.1",
        port=8000,
        dongle_serial="DG00000001",
        inverter_serial="0000000001",
        skip_initial_data=False,
        connection_retries=1,
        **options,
    )


def test_public_import_does_not_require_or_import_home_assistant():
    """Prove the standalone dependency graph works with HA unavailable."""
    repository = Path(__file__).resolve().parents[1]
    script = """
import builtins
import asyncio
import sys
from unittest.mock import AsyncMock

real_import = builtins.__import__
def reject_home_assistant(name, *args, **kwargs):
    if name == "homeassistant" or name.startswith("homeassistant."):
        raise AssertionError(f"standalone import reached {name}")
    return real_import(name, *args, **kwargs)

builtins.__import__ = reject_home_assistant
from luxpower import LuxPowerReadClient, LuxPowerTelemetry, TelemetryGroup, input_register_group
client = LuxPowerReadClient(
    host="192.0.2.1",
    port=8000,
    dongle_serial="DG00000001",
    inverter_serial="0000000001",
)
assert LuxPowerTelemetry is not None
assert input_register_group(0) is TelemetryGroup.OPERATIONAL
assert hasattr(client, "async_read")
client._client.async_get_data = AsyncMock(
    return_value={"input": {0: 1}, "hold": {0: 2}, "battery": {}}
)
telemetry = asyncio.run(client.async_read())
assert telemetry.input_registers == {0: 1}
assert not any(name == "homeassistant" or name.startswith("homeassistant.") for name in sys.modules)
"""
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [sys.executable, "-S", "-c", script],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_read_client_exposes_no_write_operation():
    """Keep the first standalone API deliberately read-only."""
    client = make_client()

    assert hasattr(client, "async_read")
    assert not hasattr(client, "async_write_register")
    assert not any("write" in name for name in dir(client) if not name.startswith("_"))


@pytest.mark.asyncio
async def test_read_client_returns_typed_telemetry_using_existing_poll_path():
    """Read decoded data through a fake transport without bypassing poll cadence."""
    observed = iter([
        datetime(2026, 1, 2, 10, 0, second, tzinfo=timezone.utc)
        for second in range(4)
    ])
    client = make_client(block_size=375, clock=lambda: next(observed))
    reader = AsyncMock()
    writer = MagicMock()
    writer.wait_closed = AsyncMock()
    protocol_client = client._client

    async def fake_registers(_writer, _reader, register, request_type, function_code):
        assert function_code == (4 if request_type == "input" else 3)
        if request_type == "input":
            return {register: 1000 + register}
        return {register: 2000 + register}

    with patch.object(
        protocol_client._connection_manager,
        "async_connect",
        AsyncMock(return_value=(reader, writer)),
    ), patch.object(
        protocol_client,
        "async_request_registers",
        AsyncMock(side_effect=fake_registers),
    ) as requests:
        telemetry = await client.async_read()

    assert telemetry == LuxPowerTelemetry(
        input_registers={0: 1000, 375: 1375},
        holding_registers={0: 2000, 375: 2375},
        batteries={},
    )
    assert telemetry.observed_at.input_registers == {
        0: datetime(2026, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
        375: datetime(2026, 1, 2, 10, 0, 1, tzinfo=timezone.utc),
    }
    assert telemetry.observed_at.holding_registers == {
        0: datetime(2026, 1, 2, 10, 0, 2, tzinfo=timezone.utc),
        375: datetime(2026, 1, 2, 10, 0, 3, tzinfo=timezone.utc),
    }
    assert telemetry.grouped_input_registers()[TelemetryGroup.OPERATIONAL][0] == 1000
    assert requests.await_args_list == [
        call(writer, reader, 0, "input", 4),
        call(writer, reader, 375, "input", 4),
        call(writer, reader, 0, "hold", 3),
        call(writer, reader, 375, "hold", 3),
    ]
    writer.close.assert_called_once_with()
    writer.wait_closed.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_read_client_surfaces_library_owned_failures():
    """Standalone consumers receive LuxPower exceptions, never HA exceptions."""
    client = make_client()

    with patch.object(
        client._client,
        "async_get_data",
        AsyncMock(side_effect=LuxPowerCommunicationError("offline")),
    ):
        with pytest.raises(LuxPowerCommunicationError, match="offline"):
            await client.async_read()
