"""Observational metadata uses the existing ring; never changes FC4 decisions."""

import json
from dataclasses import asdict

import pytest
from test_frame_aware_session import (
    FakeWriter,
    QueueReader,
    input_response,
    make_session,
)

from custom_components.lxp_modbus.exceptions import LuxPowerReadTimeoutError
from luxpower.qualified import DiagnosticReadProfile, QualifiedLuxReadClient


@pytest.mark.asyncio
@pytest.mark.parametrize("block", [0, 80])
async def test_fragment_receipt_frame_identity_and_write_order(block):
    reader = QueueReader()
    packet = input_response(block)

    def reply(_):
        reader.feed(packet[:11])
        reader.feed(packet[11:])

    session = make_session(reader, FakeWriter(reply))
    await session.async_connect()
    try:
        await session.async_read_input(block, 40)
        snap = session.diagnostics()
        events = snap.events
        writes = [e for e in events if e.kind.value == "write_started"]
        chunks = [e for e in events if e.kind.value == "bytes_received"]
        frames = [e for e in events if e.kind.value == "frame_completed"]
        assert len(writes) == 1
        assert [e.byte_count for e in chunks] == [11, len(packet) - 11]
        assert chunks[0].buffered_bytes == 11 and chunks[1].buffered_bytes == 0
        assert frames[-1].function_code == 4 and frames[-1].register_start == block
        assert frames[-1].register_count == 40
        assert writes[0].sequence < chunks[0].sequence < frames[-1].sequence
        assert (
            snap.requests[-1].queued_monotonic_seconds
            <= snap.requests[-1].started_monotonic_seconds
        )
        assert snap.origin_utc.endswith("+00:00")
        assert session.metrics().expected_fc4_responses == 1
        assert reader.max_active_reads == 1
        assert "TESTINV001" not in json.dumps(asdict(snap))
    finally:
        await session.async_close()


@pytest.mark.asyncio
async def test_partial_timeout_and_late_old_generation_are_observed_not_admitted():
    reader = QueueReader()
    session = make_session(
        reader,
        FakeWriter(lambda _: reader.feed(input_response(80)[:13])),
        request_timeout=0.02,
    )
    await session.async_connect()
    generation = session._generation
    try:
        with pytest.raises(LuxPowerReadTimeoutError):
            await session.async_read_input(80, 40)
        snap = session.diagnostics()
        assert any(
            e.kind.value == "bytes_received" and e.buffered_bytes == 13
            for e in snap.events
        )
        assert snap.requests[-1].outcome.value == "response_timeout"
        before = session.metrics().validated_fc4_frames
        session._route_frame(input_response(80), generation)
        assert session.metrics().validated_fc4_frames == before
        assert session.diagnostics().events[-1].kind.value == "old_generation_frame"
    finally:
        await session.async_close()


@pytest.mark.asyncio
async def test_invalid_frame_metadata_does_not_change_matching():
    reader = QueueReader()

    def reply(_):
        bad = bytearray(input_response(80))
        bad[-1] ^= 1
        reader.feed(bytes(bad))
        reader.feed(input_response(80))

    session = make_session(reader, FakeWriter(reply))
    await session.async_connect()
    try:
        await session.async_read_input(80, 40)
        assert session.metrics().invalid_frames == 1
        assert session.metrics().expected_fc4_responses == 1
        assert any(
            e.kind.value == "invalid_frame" and e.classification
            for e in session.diagnostics().events
        )
        assert (
            len(
                [
                    e
                    for e in session.diagnostics().events
                    if e.kind.value == "frame_completed"
                ]
            )
            == 2
        )
    finally:
        await session.async_close()


def test_supported_facade_exposes_detached_journal_without_io():
    client = QualifiedLuxReadClient(
        "192.0.2.1", "TESTDONGLE", "TESTINV001", profile=DiagnosticReadProfile()
    )
    snap = client.transport_diagnostics()
    assert snap.requests_total == 0
    assert snap.event_capacity == 512
    assert snap.request_capacity == 4096
    assert snap.failure_capacity == 64
    assert client.transport_metrics().connection_attempts == 0


@pytest.mark.asyncio
async def test_new_passive_hook_failure_does_not_change_transport(monkeypatch):
    reader = QueueReader()
    session = make_session(
        reader, FakeWriter(lambda _: reader.feed(input_response(80)))
    )
    original = session._diagnostics.record_event

    def fail_passive(kind, *args, **kwargs):
        if kind.value in ("write_started", "bytes_received", "frame_completed"):
            raise RuntimeError("diagnostic-only failure")
        return original(kind, *args, **kwargs)

    monkeypatch.setattr(session._diagnostics, "record_event", fail_passive)
    await session.async_connect()
    try:
        await session.async_read_input(80, 40)
        assert session.metrics().expected_fc4_responses == 1
        assert session.metrics().request_timeouts == 0
        assert session.diagnostics().passive_event_errors == 3
    finally:
        await session.async_close()
