"""Tests for bounded sanitized FC4 timeout diagnostics."""

import asyncio
from dataclasses import asdict
import json

import pytest

from custom_components.lxp_modbus.exceptions import LuxPowerReadTimeoutError
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxDiagnosticEventKind,
    LuxReadDiagnosticJournal,
    LuxReadPurpose,
    LuxReadRequestContext,
    LuxReadRequestOutcome,
)
from test_data import FUNCTION_193_MESSAGE
from test_frame_aware_session import (
    FakeWriter,
    QueueReader,
    input_response,
    make_session,
)


@pytest.mark.asyncio
async def test_request_registration_precedes_write_and_records_success():
    reader = QueueReader()
    holder = {}

    def on_write(_packet):
        diagnostics = holder["session"].diagnostics()
        assert diagnostics.events[-1].kind is LuxDiagnosticEventKind.REQUEST_REGISTERED
        reader.feed(input_response(0))

    session = make_session(reader, FakeWriter(on_write))
    holder["session"] = session
    await session.async_connect()

    await session.async_read_input(
        0,
        40,
        context=LuxReadRequestContext(purpose=LuxReadPurpose.NORMAL_PROFILE),
    )
    request = session.diagnostics().requests[-1]

    assert request.outcome is LuxReadRequestOutcome.SUCCESS
    assert request.purpose is LuxReadPurpose.NORMAL_PROFILE
    assert request.write_returned is True
    assert request.drain_completed is True
    assert request.matching_response_routed is True
    await session.async_close()


@pytest.mark.asyncio
async def test_response_during_drain_preserves_real_event_order():
    reader = QueueReader()

    class ResponseDuringDrainWriter(FakeWriter):
        def write(self, packet):
            super().write(packet)
            reader.feed(input_response(0))

        async def drain(self):
            await asyncio.sleep(0.01)

    session = make_session(reader, ResponseDuringDrainWriter())
    await session.async_connect()
    await session.async_read_input(0, 40)

    diagnostics = session.diagnostics()
    request = diagnostics.requests[-1]
    request_events = [
        event
        for event in diagnostics.events
        if event.request_sequence == request.request_sequence
    ]
    matched = next(
        event for event in request_events if event.kind is LuxDiagnosticEventKind.MATCHED_FC4
    )
    drained = next(
        event
        for event in request_events
        if event.kind is LuxDiagnosticEventKind.DRAIN_COMPLETED
    )

    assert request.matched_before_drain_completion_observed is True
    assert matched.sequence < drained.sequence
    await session.async_close()


@pytest.mark.asyncio
async def test_timeout_episode_captures_other_pending_traffic_without_values():
    reader = QueueReader()

    def on_write(_packet):
        reader.feed(input_response(40, values=[65432] * 40))
        reader.feed(bytes.fromhex(FUNCTION_193_MESSAGE))

    session = make_session(
        reader,
        FakeWriter(on_write),
        request_timeout=0.02,
    )
    await session.async_connect()

    with pytest.raises(LuxPowerReadTimeoutError):
        await session.async_read_input(0, 40)

    diagnostics = session.diagnostics()
    request = diagnostics.requests[-1]
    assert request.outcome is LuxReadRequestOutcome.RESPONSE_TIMEOUT
    assert request.timeout_budget_ms == 20
    assert request.drain_timeout_budget_ms == 20
    assert request.reply_timeout_budget_ms == 20
    assert request.split_deadlines is False
    assert request.reply_wait_duration_ms is None
    assert request.unmatched_fc4_while_pending == 1
    assert request.fc193_while_pending == 1
    assert request.invalid_frames_while_pending == 0
    assert request.generation_invalidated is True
    assert request.future_done_when_timeout_handled is False
    assert diagnostics.failures_total == 1
    assert diagnostics.timeout_episodes[-1].request == request
    assert diagnostics.timeout_episodes[-1].late_old_generation_frame_observation_supported is False
    lifecycle = [
        event
        for event in diagnostics.events
        if event.kind
        in (
            LuxDiagnosticEventKind.CONNECTION_OPENED,
            LuxDiagnosticEventKind.CLOSE_STARTED,
            LuxDiagnosticEventKind.CLOSE_COMPLETED,
        )
    ]
    assert {event.generation for event in lifecycle} == {request.generation}
    assert "65432" not in json.dumps(asdict(diagnostics))


def test_diagnostic_retention_is_bounded_and_reports_drops():
    journal = LuxReadDiagnosticJournal(
        event_capacity=3,
        request_capacity=2,
        failure_capacity=1,
        failure_event_context=2,
    )
    for index in range(5):
        state = journal.begin_request(
            generation=1,
            register_start=index * 40,
            register_count=40,
            timeout_seconds=3,
            context=LuxReadRequestContext(),
            connection_opened_monotonic=journal.now(),
            requests_previously_on_generation=index,
        )
        journal.finalize_request(state, LuxReadRequestOutcome.SUCCESS)

    diagnostics = journal.snapshot()
    assert diagnostics.requests_total == 5
    assert len(diagnostics.requests) == 2
    assert diagnostics.requests_dropped == 3
    assert diagnostics.events_total == 10
    assert len(diagnostics.events) == 3
    assert diagnostics.events_dropped == 7


@pytest.mark.asyncio
async def test_diagnostics_exclude_private_target_and_raw_packet_data():
    reader = QueueReader()
    writer = FakeWriter(lambda _packet: reader.feed(input_response(0)))

    async def connector(_host, _port):
        return reader, writer

    from custom_components.lxp_modbus.classes.read_session import LuxReadSession

    session = LuxReadSession(
        "private-host.example",
        "TESTDONGLE",
        "TESTINV001",
        connector=connector,
    )
    await session.async_connect()
    await session.async_read_input(0, 40)
    serialized = json.dumps(asdict(session.diagnostics()))

    assert "private-host.example" not in serialized
    assert "TESTDONGLE" not in serialized
    assert "TESTINV001" not in serialized
    assert "packet" not in serialized.lower()
    assert "values" not in serialized.lower()
    assert "target_mismatch" not in serialized
    await session.async_close()


def test_diagnostic_records_independent_phase_budgets():
    journal = LuxReadDiagnosticJournal()
    state = journal.begin_request(
        generation=1,
        register_start=0,
        register_count=40,
        drain_timeout_seconds=1,
        reply_timeout_seconds=10,
        split_deadlines=True,
        context=LuxReadRequestContext(),
        connection_opened_monotonic=journal.now(),
        requests_previously_on_generation=0,
    )

    request = journal.finalize_request(state, LuxReadRequestOutcome.SUCCESS)

    assert journal.snapshot().schema_version == 4
    assert request.timeout_budget_ms == 10000
    assert request.drain_timeout_budget_ms == 1000
    assert request.reply_timeout_budget_ms == 10000
    assert request.split_deadlines is True
