"""Tests for truthful per-value observation timestamps."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.lxp_modbus.classes.modbus_client import LxpModbusApiClient
from custom_components.lxp_modbus.const import (
    BATTERY_BACKOFF_POLL_EVERY,
    BATTERY_EMPTY_POLLS_BEFORE_BACKOFF,
    HOLD_REGISTER_POLL_EVERY,
)
from custom_components.lxp_modbus.constants.input_registers import I_BAT_PARALLEL_NUM


class TickClock:
    """Deterministic aware clock that records every acceptance-point call."""

    def __init__(self) -> None:
        self.next_value = datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc)
        self.returned: list[datetime] = []

    def __call__(self) -> datetime:
        value = self.next_value
        self.returned.append(value)
        self.next_value += timedelta(seconds=1)
        return value


def make_client(clock, **options):
    """Build a protocol client whose transport is replaced in each test."""
    return LxpModbusApiClient(
        "192.0.2.1",
        8000,
        "DG00000001",
        "0000000001",
        asyncio.Lock(),
        connection_retries=1,
        skip_initial_data=False,
        clock=clock,
        **options,
    )


async def run_poll(client, request_side_effect) -> AsyncMock:
    """Run one poll with a fake connection and caller-defined block responses."""
    reader = AsyncMock(spec=asyncio.StreamReader)
    writer = AsyncMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    request = AsyncMock(side_effect=request_side_effect)
    with patch.object(
        client._connection_manager,
        "async_connect",
        AsyncMock(return_value=(reader, writer)),
    ), patch.object(
        client._connection_manager,
        "async_discard_initial_data",
        AsyncMock(),
    ), patch.object(
        client._connection_manager,
        "async_close",
        AsyncMock(),
    ), patch.object(client, "async_request_registers", request):
        await client._async_poll_once()
    return request


@pytest.mark.asyncio
async def test_successful_block_stamps_each_register_once_and_later_read_advances_it():
    """All accepted values in a block share its observation time."""
    clock = TickClock()
    client = make_client(clock, block_size=750)
    client._force_hold_poll = False

    await run_poll(
        client,
        lambda _writer, _reader, _reg, _kind, _function: {0: 1, 1: 2},
    )
    first = client.get_observation_times()
    assert first.input_registers[0] == first.input_registers[1] == clock.returned[0]
    assert first.input_registers[0].tzinfo is timezone.utc

    await run_poll(
        client,
        lambda _writer, _reader, _reg, _kind, _function: {0: 3, 1: 4},
    )
    second = client.get_observation_times()
    assert second.input_registers[0] == second.input_registers[1] == clock.returned[1]
    assert second.input_registers[0] > first.input_registers[0]


@pytest.mark.asyncio
async def test_partial_poll_advances_only_successful_block():
    """A timed-out block retains both its cached value and original timestamp."""
    clock = TickClock()
    client = make_client(clock, block_size=375)
    client._force_hold_poll = False

    await run_poll(
        client,
        lambda _writer, _reader, reg, _kind, _function: {reg: reg + 1},
    )
    before = client.get_observation_times()

    async def partial(_writer, _reader, reg, _kind, _function):
        if reg == 375:
            raise asyncio.TimeoutError
        return {0: 99}

    await run_poll(client, partial)
    data = client.get_cached_data()
    after = client.get_observation_times()

    assert data["input"] == {0: 99, 375: 376}
    assert after.input_registers[0] > before.input_registers[0]
    assert after.input_registers[375] == before.input_registers[375]


@pytest.mark.asyncio
async def test_aborted_attempt_does_not_refresh_an_earlier_successful_block():
    """A later connection error prevents unmerged local data appearing fresh."""
    clock = TickClock()
    client = make_client(clock, block_size=375)
    client._force_hold_poll = False
    await run_poll(
        client,
        lambda _writer, _reader, reg, _kind, _function: {reg: reg + 1},
    )
    before_data = client.get_cached_data()
    before_time = client.get_observation_times()

    async def aborted(_writer, _reader, reg, _kind, _function):
        if reg == 375:
            raise OSError("connection lost")
        return {0: 999}

    with pytest.raises(OSError, match="connection lost"):
        await run_poll(client, aborted)

    assert client.get_cached_data() == before_data
    assert client.get_observation_times() == before_time


@pytest.mark.asyncio
async def test_complete_failed_poll_returns_cache_without_refreshing_it():
    """The two-failure cache window never turns old values into new observations."""
    clock = TickClock()
    client = make_client(clock, block_size=750)

    def initial(_writer, _reader, _reg, request_type, _function):
        return {0: 10} if request_type == "input" else {0: 20}

    await run_poll(client, initial)
    before = client.get_observation_times()

    with patch.object(
        client._connection_manager,
        "async_connect",
        AsyncMock(side_effect=ConnectionRefusedError("offline")),
    ):
        cached = await client.async_get_data()

    assert cached == {"input": {0: 10}, "hold": {0: 20}, "battery": {}}
    assert client.get_observation_times() == before


@pytest.mark.asyncio
async def test_holding_times_advance_only_when_holding_registers_are_polled():
    """Skipped every-fifth-poll configuration reads retain their prior age."""
    clock = TickClock()
    client = make_client(clock, block_size=750)
    hold_value = 20

    def response(_writer, _reader, _reg, request_type, _function):
        return {0: 10} if request_type == "input" else {0: hold_value}

    first_request = await run_poll(client, response)
    first = client.get_observation_times().holding_registers[0]
    assert any(call.args[3] == "hold" for call in first_request.await_args_list)

    skipped_request = await run_poll(client, response)
    assert not any(call.args[3] == "hold" for call in skipped_request.await_args_list)
    assert client.get_observation_times().holding_registers[0] == first

    for _ in range(HOLD_REGISTER_POLL_EVERY - 1):
        last_request = await run_poll(client, response)

    assert any(call.args[3] == "hold" for call in last_request.await_args_list)
    assert client.get_observation_times().holding_registers[0] > first


@pytest.mark.asyncio
async def test_write_ack_does_not_claim_a_holding_register_read():
    """An accepted write changes the cache but only its readback can set freshness."""
    clock = TickClock()
    client = make_client(clock, block_size=125)
    response = MagicMock(
        packet_error=False,
        device_function=6,
        exception=0,
        parsed_values_dictionary={21: 7},
        info="",
    )

    with patch(
        "custom_components.lxp_modbus.classes.modbus_client.LxpResponse",
        return_value=response,
    ):
        assert client._evaluate_write_response(b"accepted", 21, 7, 0) is True

    assert client.get_cached_data()["hold"][21] == 7
    assert 21 not in client.get_observation_times().holding_registers
    assert clock.returned == []


@pytest.mark.asyncio
async def test_write_ack_clears_freshness_that_belonged_to_replaced_value():
    """A newly ACKed value must never inherit the prior cached value's age."""
    clock = TickClock()
    client = make_client(clock, block_size=125)
    old_observation = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)
    client._last_good_hold_regs[21] = 6
    client._hold_observed_at[21] = old_observation
    response = MagicMock(
        packet_error=False,
        device_function=6,
        exception=0,
        parsed_values_dictionary={21: 7},
        info="",
    )

    with patch(
        "custom_components.lxp_modbus.classes.modbus_client.LxpResponse",
        return_value=response,
    ):
        assert client._evaluate_write_response(b"accepted", 21, 7, 0) is True

    assert client.get_cached_data()["hold"][21] == 7
    assert 21 not in client.get_observation_times().holding_registers
    assert clock.returned == []


@pytest.mark.asyncio
async def test_successful_post_write_readback_stamps_the_returned_holding_block():
    """The existing same-connection readback is a genuine holding observation."""
    clock = TickClock()
    client = make_client(clock, block_size=125)

    with patch.object(
        client,
        "async_request_registers",
        AsyncMock(return_value={21: 7, 22: 8}),
    ):
        await client._async_reread_hold_block(MagicMock(), AsyncMock(), 21)

    observed = client.get_observation_times().holding_registers
    assert observed[21] == observed[22] == clock.returned[0]


@pytest.mark.asyncio
async def test_battery_times_respect_backoff_and_actual_responses():
    """A skipped BMS block leaves each decoded battery value at its old time."""
    clock = TickClock()
    client = make_client(clock, block_size=750, request_battery_data=True)
    client._force_hold_poll = False
    battery_voltage = 520

    def response(_writer, _reader, reg, request_type, _function):
        if request_type == "input/bat":
            return {"BATTERY01": {"serial": "BATTERY01", 8: battery_voltage}}
        assert reg == 0
        return {I_BAT_PARALLEL_NUM: 1}

    await run_poll(client, response)
    first = client.get_observation_times().batteries["BATTERY01"]
    assert first["serial"] == first[8]

    client._battery_empty_polls = BATTERY_EMPTY_POLLS_BEFORE_BACKOFF
    skipped_request = await run_poll(client, response)
    assert not any(call.args[3] == "input/bat" for call in skipped_request.await_args_list)
    assert client.get_observation_times().batteries["BATTERY01"] == first

    client._battery_backoff_counter = BATTERY_BACKOFF_POLL_EVERY - 1
    battery_voltage = 530
    retried_request = await run_poll(client, response)
    assert any(call.args[3] == "input/bat" for call in retried_request.await_args_list)
    assert client.get_cached_data()["battery"]["BATTERY01"][8] == 530
    assert client.get_observation_times().batteries["BATTERY01"][8] > first[8]


@pytest.mark.asyncio
async def test_observation_snapshot_is_detached_from_internal_metadata():
    """Mutating a returned sidecar cannot corrupt later snapshots."""
    clock = TickClock()
    client = make_client(clock, block_size=750, request_battery_data=True)
    client._force_hold_poll = False

    def response(_writer, _reader, _reg, request_type, _function):
        if request_type == "input/bat":
            return {"BATTERY01": {8: 520}}
        return {I_BAT_PARALLEL_NUM: 1}

    await run_poll(client, response)
    snapshot = client.get_observation_times()
    expected_input = snapshot.input_registers[I_BAT_PARALLEL_NUM]
    expected_battery = snapshot.batteries["BATTERY01"][8]

    snapshot.input_registers[I_BAT_PARALLEL_NUM] = datetime.min.replace(
        tzinfo=timezone.utc
    )
    snapshot.batteries["BATTERY01"][8] = datetime.min.replace(tzinfo=timezone.utc)

    fresh_copy = client.get_observation_times()
    assert fresh_copy.input_registers[I_BAT_PARALLEL_NUM] == expected_input
    assert fresh_copy.batteries["BATTERY01"][8] == expected_battery


@pytest.mark.asyncio
async def test_naive_clock_is_rejected_before_data_or_freshness_is_merged():
    """Observation times must always represent an unambiguous absolute instant."""
    client = make_client(lambda: datetime(2026, 1, 2, 10, 0), block_size=750)
    client._force_hold_poll = False

    with pytest.raises(ValueError, match="timezone-aware"):
        await run_poll(
            client,
            lambda _writer, _reader, _reg, _kind, _function: {0: 1},
        )

    assert client.get_cached_data()["input"] == {}
    assert client.get_observation_times().input_registers == {}
