"""Synthetic tests for the single-reader frame-aware FC4 session."""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from custom_components.lxp_modbus.classes.frame_decoder import LuxFrameDecoder
from custom_components.lxp_modbus.classes.lxp_packet_utils import LxpPacketUtils
from custom_components.lxp_modbus.classes.lxp_request_builder import LxpRequestBuilder
from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadSession,
)
from custom_components.lxp_modbus.exceptions import (
    LuxPowerAmbiguousRequestError,
    LuxPowerConnectionError,
    LuxPowerCommunicationError,
    LuxPowerReadTimeoutError,
    LuxPowerSessionClosedError,
)
from test_data import FUNCTION_193_MESSAGE


DONGLE = b"TESTDONGLE"
INVERTER = b"TESTINV001"


def input_response(
    start=0,
    count=40,
    *,
    values=None,
    serial=INVERTER,
    dongle=DONGLE,
    function=4,
    action=1,
    data_length_adjust=0,
):
    """Build a CRC-valid protocol-5 FC4 response with synthetic identifiers."""
    words = list(range(count)) if values is None else list(values)
    value_bytes = b"".join(value.to_bytes(2, "little") for value in words)
    data_frame = (
        bytes([action, function])
        + serial
        + start.to_bytes(2, "little")
        + len(value_bytes).to_bytes(1, "little")
        + value_bytes
    )
    crc = LxpPacketUtils.compute_crc(data_frame).to_bytes(2, "little")
    body = (
        bytes([1, LxpRequestBuilder.TRANSLATED_DATA])
        + dongle
        + (len(data_frame) + 2 + data_length_adjust).to_bytes(2, "little")
        + data_frame
        + crc
    )
    return (
        LxpRequestBuilder.PREFIX
        + (5).to_bytes(2, "little")
        + len(body).to_bytes(2, "little")
        + body
    )


class QueueReader:
    """Controllable stream reader which detects competing reads."""

    def __init__(self):
        self._queue = asyncio.Queue()
        self.read_calls = 0
        self.active_reads = 0
        self.max_active_reads = 0

    async def read(self, _count):
        self.read_calls += 1
        self.active_reads += 1
        self.max_active_reads = max(self.max_active_reads, self.active_reads)
        try:
            value = await self._queue.get()
            return b"" if value is None else value
        finally:
            self.active_reads -= 1

    def feed(self, value):
        self._queue.put_nowait(value)

    def eof(self):
        self._queue.put_nowait(None)


class FakeWriter:
    def __init__(self, on_write=None):
        self.on_write = on_write
        self.packets = []
        self.closed = False

    def write(self, packet):
        self.packets.append(packet)
        if self.on_write:
            self.on_write(packet)

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


def make_session(reader, writer, **kwargs):
    async def connector(_host, _port):
        return reader, writer

    return LuxReadSession(
        "192.0.2.1",
        DONGLE.decode(),
        INVERTER.decode(),
        connector=connector,
        **kwargs,
    )


def test_request_latency_history_covers_a_sixty_minute_qualification():
    session = make_session(QueueReader(), FakeWriter())
    session._request_latencies_ms.extend(float(index) for index in range(600))
    session._request_latency_samples_total = 600

    metrics = session.metrics()

    assert metrics.request_latency_samples_total == 600
    assert len(metrics.request_latencies_ms) == 600
    assert metrics.request_latencies_ms[0] == 0.0


def test_decoder_assembles_byte_fragmentation_and_retains_leftovers():
    first = input_response(0)
    second = input_response(40)
    decoder = LuxFrameDecoder()
    emitted = []

    for byte in first + second[:17]:
        emitted.extend(decoder.feed(bytes([byte])))

    assert emitted == [first]
    assert decoder.stats().buffered_bytes == 17
    assert decoder.feed(second[17:]) == (second,)
    assert decoder.stats().buffered_bytes == 0


def test_decoder_emits_multiple_frames_and_resynchronizes_malformed_length():
    first = input_response(0)
    second = input_response(40)
    malformed = b"\xa1\x1a\x05\x00\xff\xffjunk"
    decoder = LuxFrameDecoder()

    frames = decoder.feed(b"junk" + malformed + first + second)

    assert frames == (first, second)
    assert decoder.stats().malformed_lengths == 1
    assert decoder.stats().discarded_bytes >= len(b"junk") + 1


def test_decoder_resynchronizes_plausible_corrupt_length_before_valid_frame():
    plausible_corruption = b"\xa1\x1a\x05\x00\x84\x03corrupt"
    valid = input_response(40)
    decoder = LuxFrameDecoder()

    frames = decoder.feed(plausible_corruption + valid)

    assert frames == (valid,)
    assert decoder.stats().malformed_lengths == 1


def test_untrusted_function_193_cannot_trigger_speculative_resynchronization():
    plausible_corruption = b"\xa1\x1a\x05\x00\x84\x03corrupt"
    function_193 = bytes.fromhex(FUNCTION_193_MESSAGE)
    valid = input_response(40)
    decoder = LuxFrameDecoder()

    assert decoder.feed(plausible_corruption + function_193) == ()
    assert decoder.stats().malformed_lengths == 0
    assert decoder.stats().buffered_bytes == len(plausible_corruption + function_193)

    assert decoder.feed(valid) == (valid,)
    assert decoder.stats().malformed_lengths == 1


@pytest.mark.asyncio
async def test_exact_response_and_unmatched_fc4_are_routed_independently():
    reader = QueueReader()
    writer = FakeWriter(
        lambda _packet: reader.feed(input_response(0) + input_response(160))
    )
    times = iter([
        datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 10, 0, 1, tzinfo=timezone.utc),
    ])
    session = make_session(reader, writer, clock=lambda: next(times))
    await session.async_connect()

    result = await session.async_read_input(160, 40)
    first = await session.async_next_observation(timeout=0.1)
    second = await session.async_next_observation(timeout=0.1)

    assert result.register_start == 160
    assert result.explicit_response is True
    assert first.register_start == 0 and first.explicit_response is False
    assert second.register_start == 160 and second.explicit_response is True
    assert session.metrics().unmatched_fc4_observations == 1
    assert session.metrics().expected_fc4_responses == 1
    assert session.snapshot().input_sources[0] is LuxObservationSource.UNSOLICITED
    assert session.snapshot().input_sources[160] is LuxObservationSource.EXPLICIT
    detached_sources = session.snapshot().input_sources
    detached_sources[0] = LuxObservationSource.EXPLICIT
    assert session.snapshot().input_sources[0] is LuxObservationSource.UNSOLICITED
    assert session.snapshot().observed_at.input_registers[0] < (
        session.snapshot().observed_at.input_registers[160]
    )
    assert reader.max_active_reads == 1
    await session.async_close()


@pytest.mark.asyncio
async def test_unmatched_frame_does_not_complete_pending_request():
    reader = QueueReader()
    writer = FakeWriter(lambda _packet: reader.feed(input_response(0)))
    session = make_session(reader, writer)
    await session.async_connect()

    request = asyncio.create_task(session.async_read_input(160, 40))
    await asyncio.sleep(0.01)
    assert not request.done()
    reader.feed(input_response(160))

    assert (await request).register_start == 160
    await session.async_close()


@pytest.mark.asyncio
async def test_bad_crc_and_wrong_count_cannot_complete_pending_request():
    reader = QueueReader()
    bad_crc = bytearray(input_response(160))
    bad_crc[-1] ^= 1
    writer = FakeWriter(
        lambda _packet: reader.feed(
            bytes(bad_crc) + input_response(160, 39) + input_response(160, 40)
        )
    )
    session = make_session(reader, writer)
    await session.async_connect()

    result = await session.async_read_input(160, 40)

    assert result.register_count == 40
    assert session.metrics().invalid_frames == 1
    assert session.metrics().unmatched_fc4_observations == 1
    assert session.metrics().expected_fc4_responses == 1
    await session.async_close()


@pytest.mark.asyncio
async def test_wrong_targets_function_and_envelope_never_refresh_telemetry():
    reader = QueueReader()
    writer = FakeWriter()
    session = make_session(reader, writer)
    await session.async_connect()

    reader.feed(
        input_response(0, serial=b"OTHERINV01")
        + input_response(0, dongle=b"OTHERDONG1")
        + input_response(0, function=3)
        + input_response(0, action=0)
        + input_response(0, data_length_adjust=1)
    )
    await asyncio.sleep(0.01)

    assert session.metrics().invalid_frames == 5
    assert session.snapshot().input_registers == {}
    reasons = [
        event.classification
        for event in session.diagnostics().events
        if event.kind.value == "invalid_frame"
    ]
    assert reasons == [
        "inverter_target_mismatch",
        "dongle_target_mismatch",
        "device_function",
        "address_action",
        "data_length",
    ]
    await session.async_close()


@pytest.mark.asyncio
async def test_explicit_fc4_exception_fails_request_without_freshness():
    reader = QueueReader()

    def exception_response(start):
        data_frame = bytes([1, 0x84]) + INVERTER + start.to_bytes(2, "little") + bytes([3])
        crc = LxpPacketUtils.compute_crc(data_frame).to_bytes(2, "little")
        body = bytes([1, 194]) + DONGLE + (len(data_frame) + 2).to_bytes(2, "little") + data_frame + crc
        return b"\xa1\x1a" + (5).to_bytes(2, "little") + len(body).to_bytes(2, "little") + body

    writer = FakeWriter(lambda _packet: reader.feed(exception_response(0)))
    session = make_session(reader, writer)
    await session.async_connect()

    from custom_components.lxp_modbus.exceptions import LuxPowerReadRejectedError
    with pytest.raises(LuxPowerReadRejectedError):
        await session.async_read_input(0, 40)

    assert session.connected is True
    assert session.snapshot().input_registers == {}
    await session.async_close()


@pytest.mark.asyncio
async def test_function_193_is_diagnostic_only_and_never_refreshes_fc4():
    reader = QueueReader()
    writer = FakeWriter()
    session = make_session(reader, writer)
    await session.async_connect()

    reader.feed(bytes.fromhex(FUNCTION_193_MESSAGE))
    await asyncio.sleep(0.01)

    assert session.metrics().function_193_frames == 1
    assert session.metrics().validated_fc4_frames == 0
    assert session.snapshot().input_registers == {}
    assert session.snapshot().observed_at.input_registers == {}
    await session.async_close()


@pytest.mark.asyncio
async def test_duplicate_valid_reception_is_a_new_truthful_local_observation():
    reader = QueueReader()
    writer = FakeWriter()
    times = iter([
        datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 10, 0, 5, tzinfo=timezone.utc),
    ])
    session = make_session(reader, writer, clock=lambda: next(times))
    await session.async_connect()
    frame = input_response(0)

    reader.feed(frame)
    first = await session.async_next_observation(timeout=0.1)
    reader.feed(frame)
    second = await session.async_next_observation(timeout=0.1)

    assert first.duplicate is False
    assert second.duplicate is True
    assert second.observed_at > first.observed_at
    assert session.metrics().duplicate_fc4_frames == 1
    await session.async_close()


@pytest.mark.asyncio
async def test_timeout_taints_connection_and_late_response_cannot_cross_reconnect():
    readers = [QueueReader(), QueueReader()]
    writers = [FakeWriter(), FakeWriter(lambda _packet: readers[1].feed(input_response(0)))]
    connections = iter(zip(readers, writers))

    async def connector(_host, _port):
        return next(connections)

    session = LuxReadSession(
        "192.0.2.1",
        DONGLE.decode(),
        INVERTER.decode(),
        connector=connector,
        request_timeout=0.02,
    )
    await session.async_connect()

    with pytest.raises(LuxPowerReadTimeoutError):
        await session.async_read_input(0, 40)
    assert session.connected is False
    readers[0].feed(input_response(0))

    await session.async_connect()
    result = await session.async_read_input(0, 40)

    assert result.register_start == 0
    assert session.metrics().connections == 2
    assert session.metrics().expected_fc4_responses == 1
    await session.async_close()


@pytest.mark.asyncio
async def test_timeout_taints_before_waiting_for_lifecycle_cleanup_lock():
    reader = QueueReader()
    session = make_session(reader, FakeWriter(), request_timeout=0.01)
    await session.async_connect()
    await session._lifecycle_lock.acquire()
    request = asyncio.create_task(session.async_read_input(0, 40))
    await asyncio.sleep(0.02)

    # The timeout handler is blocked entering async_close. Its synchronous
    # taint must already make this late frame unable to refresh the cache.
    reader.feed(input_response(0))
    await asyncio.sleep(0.01)
    assert session.snapshot().input_registers == {}
    assert session.metrics().validated_fc4_frames == 0

    session._lifecycle_lock.release()
    with pytest.raises(LuxPowerReadTimeoutError):
        await request


@pytest.mark.asyncio
async def test_cancellation_suppressing_old_reader_cannot_cross_generation():
    class CancellationSuppressingReader:
        async def read(self, _count):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return input_response(0)

    old_reader = CancellationSuppressingReader()
    new_reader = QueueReader()
    writers = [
        FakeWriter(),
        FakeWriter(lambda _packet: new_reader.feed(input_response(0))),
    ]
    connections = iter(((old_reader, writers[0]), (new_reader, writers[1])))

    async def connector(_host, _port):
        return next(connections)

    session = LuxReadSession(
        "192.0.2.1", DONGLE.decode(), INVERTER.decode(), connector=connector
    )
    await session.async_connect()
    await asyncio.sleep(0)
    await session.async_close()

    assert session.snapshot().input_registers == {}
    assert session.metrics().frames_received == 0

    await session.async_connect()
    result = await session.async_read_input(0, 40)
    assert result.explicit_response is True
    assert session.metrics().expected_fc4_responses == 1
    await session.async_close()


@pytest.mark.asyncio
async def test_reconnect_starts_empty_and_does_not_advance_freshness():
    readers = [QueueReader(), QueueReader()]
    writers = [FakeWriter(lambda _packet: readers[0].feed(input_response(0))), FakeWriter()]
    connections = iter(zip(readers, writers))

    async def connector(_host, _port):
        return next(connections)

    session = LuxReadSession(
        "192.0.2.1", DONGLE.decode(), INVERTER.decode(), connector=connector
    )
    await session.async_connect()
    await session.async_read_input(0, 40)
    before = session.snapshot()
    readers[0].feed(input_response(40)[:20])
    await asyncio.sleep(0.01)
    await session.async_close()
    await session.async_connect()
    after = session.snapshot()

    assert after.input_registers == before.input_registers
    assert after.observed_at.input_registers == before.observed_at.input_registers
    assert session.metrics().decoder_buffered_bytes == 0
    await session.async_close()


@pytest.mark.asyncio
async def test_connection_establishment_failure_is_typed_and_counted():
    async def connector(_host, _port):
        raise ConnectionRefusedError

    session = LuxReadSession(
        "192.0.2.1", DONGLE.decode(), INVERTER.decode(), connector=connector
    )

    with pytest.raises(LuxPowerConnectionError):
        await session.async_connect()

    assert session.metrics().connection_attempts == 1
    assert session.metrics().connection_failures == 1


@pytest.mark.asyncio
async def test_drain_timeout_is_ambiguous_not_response_timeout():
    reader = QueueReader()

    class SlowDrainWriter(FakeWriter):
        async def drain(self):
            await asyncio.Event().wait()

    session = make_session(reader, SlowDrainWriter(), request_timeout=0.01)
    await session.async_connect()

    with pytest.raises(LuxPowerAmbiguousRequestError):
        await session.async_read_input(0, 40)

    assert session.metrics().ambiguous_requests == 1
    assert session.metrics().request_timeouts == 0


@pytest.mark.asyncio
async def test_split_deadlines_keep_a_short_drain_budget_with_long_reply_window():
    reader = QueueReader()

    class SlowDrainWriter(FakeWriter):
        async def drain(self):
            await asyncio.Event().wait()

    session = make_session(
        reader,
        SlowDrainWriter(),
        drain_timeout=0.01,
        reply_timeout=0.1,
    )
    await session.async_connect()

    with pytest.raises(LuxPowerAmbiguousRequestError):
        await session.async_read_input(0, 40)

    request = session.diagnostics().requests[-1]
    assert request.outcome.value == "ambiguous_drain_timeout"
    assert request.drain_timeout_budget_ms == 10
    assert request.reply_timeout_budget_ms == 100
    assert request.split_deadlines is True
    assert session.metrics().request_timeouts == 0
    assert any(
        event.kind.value == "drain_deadline_expired"
        for event in session.diagnostics().events
    )


@pytest.mark.asyncio
async def test_split_reply_window_starts_after_successful_drain():
    reader = QueueReader()

    class DelayedDrainWriter(FakeWriter):
        async def drain(self):
            await asyncio.sleep(0.02)

    writer = DelayedDrainWriter()
    session = make_session(
        reader,
        writer,
        drain_timeout=0.03,
        reply_timeout=0.03,
    )
    writer.on_write = lambda _packet: asyncio.get_running_loop().call_later(
        0.04, reader.feed, input_response(0)
    )
    await session.async_connect()

    result = await session.async_read_input(0, 40)

    assert result.register_start == 0
    request = session.diagnostics().requests[-1]
    assert request.outcome.value == "success"
    assert request.drain_completed is True
    assert request.split_deadlines is True
    assert request.accepted_response_latency_ms >= 30
    assert request.reply_wait_duration_ms is not None
    assert request.reply_wait_duration_ms >= 10
    await session.async_close()


@pytest.mark.asyncio
async def test_legacy_timeout_remains_one_combined_drain_and_reply_budget():
    reader = QueueReader()

    class DelayedDrainWriter(FakeWriter):
        async def drain(self):
            await asyncio.sleep(0.02)

    writer = DelayedDrainWriter()
    session = make_session(reader, writer, request_timeout=0.03)
    writer.on_write = lambda _packet: asyncio.get_running_loop().call_later(
        0.04, reader.feed, input_response(0)
    )
    await session.async_connect()

    with pytest.raises(LuxPowerReadTimeoutError):
        await session.async_read_input(0, 40)

    request = session.diagnostics().requests[-1]
    assert request.outcome.value == "response_timeout"
    assert request.split_deadlines is False
    assert request.drain_timeout_budget_ms == 30
    assert request.reply_timeout_budget_ms == 30
    assert request.generation_invalidated is True


@pytest.mark.asyncio
async def test_split_reply_timeout_preserves_generation_taint():
    reader = QueueReader()
    session = make_session(
        reader,
        FakeWriter(),
        drain_timeout=0.01,
        reply_timeout=0.02,
    )
    await session.async_connect()

    with pytest.raises(LuxPowerReadTimeoutError):
        await session.async_read_input(0, 40)

    request = session.diagnostics().requests[-1]
    assert request.outcome.value == "response_timeout"
    assert request.split_deadlines is True
    assert request.generation_invalidated is True
    assert session.connected is False
    assert any(
        event.kind.value == "reply_deadline_expired"
        for event in session.diagnostics().events
    )


@pytest.mark.asyncio
async def test_split_deadline_configuration_is_additive_and_validated():
    reader = QueueReader()
    session = make_session(
        reader,
        FakeWriter(),
        request_timeout=3,
        drain_timeout=1,
        reply_timeout=10,
    )

    assert session.request_timeout_seconds == 3
    assert session.drain_timeout_seconds == 1
    assert session.reply_timeout_seconds == 10
    assert session.split_request_deadlines is True

    with pytest.raises(ValueError, match="cannot be combined"):
        await session.async_read_input(
            0,
            40,
            timeout=3,
            reply_timeout=10,
        )


@pytest.mark.asyncio
async def test_per_call_legacy_timeout_restores_combined_budget_on_split_session():
    reader = QueueReader()

    class DelayedDrainWriter(FakeWriter):
        async def drain(self):
            await asyncio.sleep(0.02)

    writer = DelayedDrainWriter()
    session = make_session(
        reader,
        writer,
        drain_timeout=0.03,
        reply_timeout=0.1,
    )
    writer.on_write = lambda _packet: asyncio.get_running_loop().call_later(
        0.04, reader.feed, input_response(0)
    )
    await session.async_connect()

    with pytest.raises(LuxPowerReadTimeoutError):
        await session.async_read_input(0, 40, timeout=0.03)

    request = session.diagnostics().requests[-1]
    assert request.split_deadlines is False
    assert request.timeout_budget_ms == 30
    assert request.drain_timeout_budget_ms == 30
    assert request.reply_timeout_budget_ms == 30


@pytest.mark.asyncio
async def test_idle_eof_is_classified_on_next_acquisition():
    reader = QueueReader()
    session = make_session(reader, FakeWriter())
    await session.async_connect()
    reader.eof()
    await asyncio.sleep(0.01)

    from custom_components.lxp_modbus.exceptions import LuxPowerConnectionLostError
    with pytest.raises(LuxPowerConnectionLostError):
        await session.async_read_input(0, 40)


@pytest.mark.asyncio
async def test_shutdown_during_session_reconnect_delay_prevents_reopen():
    first_reader = QueueReader()
    second_reader = QueueReader()
    first_writer = FakeWriter()
    second_writer = FakeWriter()
    connections = iter(((first_reader, first_writer), (second_reader, second_writer)))

    async def connector(_host, _port):
        return next(connections)

    session = LuxReadSession(
        "192.0.2.1", DONGLE.decode(), INVERTER.decode(), connector=connector
    )
    await session.async_connect()
    reconnect = asyncio.create_task(session.async_reconnect(delay=0.03))
    while not first_writer.closed:
        await asyncio.sleep(0)
    await session.async_close()

    with pytest.raises(LuxPowerSessionClosedError):
        await reconnect
    assert session.connected is False
    assert session.metrics().connections == 1


@pytest.mark.asyncio
async def test_cancelling_an_ambiguous_request_taints_its_connection():
    reader = QueueReader()
    writer = FakeWriter()
    session = make_session(reader, writer)
    await session.async_connect()
    request = asyncio.create_task(session.async_read_input(0, 40))
    await asyncio.sleep(0.01)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert session.connected is False
    assert writer.closed is True


@pytest.mark.asyncio
async def test_connection_loss_fails_pending_and_resets_partial_decoder():
    reader = QueueReader()
    writer = FakeWriter(lambda _packet: (reader.feed(input_response(0)[:20]), reader.eof()))
    session = make_session(reader, writer)
    await session.async_connect()

    with pytest.raises(LuxPowerCommunicationError):
        await session.async_read_input(0, 40)

    assert session.connected is False
    assert session.metrics().connection_losses == 1
    assert session.metrics().invalid_frames == 1
    assert session.metrics().decoder_buffered_bytes == 0


@pytest.mark.asyncio
async def test_shutdown_fails_pending_and_cancels_the_only_reader():
    reader = QueueReader()
    writer = FakeWriter()
    session = make_session(reader, writer)
    await session.async_connect()
    request = asyncio.create_task(session.async_read_input(0, 40))
    await asyncio.sleep(0.01)

    await session.async_close()

    with pytest.raises(LuxPowerSessionClosedError):
        await request
    assert writer.closed is True
    assert reader.max_active_reads == 1


@pytest.mark.asyncio
async def test_explicit_requests_are_serialized_and_only_fc4_is_sent():
    reader = QueueReader()

    def respond(packet):
        assert packet[21] == 4
        start = int.from_bytes(packet[32:34], "little")
        count = int.from_bytes(packet[34:36], "little")
        reader.feed(input_response(start, count))

    writer = FakeWriter(respond)
    session = make_session(reader, writer)
    await session.async_connect()

    results = await asyncio.gather(
        session.async_read_input(0, 40),
        session.async_read_input(40, 40),
    )

    assert [result.register_start for result in results] == [0, 40]
    assert len(writer.packets) == 2
    assert reader.max_active_reads == 1
    assert not any("write" in name for name in dir(session) if not name.startswith("_"))
    await session.async_close()


@pytest.mark.asyncio
async def test_snapshot_is_detached_without_mutable_aliasing():
    reader = QueueReader()
    writer = FakeWriter(lambda _packet: reader.feed(input_response(0)))
    session = make_session(reader, writer)
    await session.async_connect()
    await session.async_read_input(0, 40)

    first = session.snapshot()
    first.input_registers[0] = 999
    first.observed_at.input_registers[0] = datetime(2000, 1, 1, tzinfo=timezone.utc)

    second = session.snapshot()
    assert second.input_registers[0] == 0
    assert second.observed_at.input_registers[0].year != 2000
    await session.async_close()
