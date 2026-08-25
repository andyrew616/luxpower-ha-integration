"""Counterbalanced, read-only FC4 order and pacing experiment."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Awaitable, Callable, Mapping, Sequence

from custom_components.lxp_modbus.classes.read_session import LuxReadSession
from custom_components.lxp_modbus.const import READ_TIMEOUT
from custom_components.lxp_modbus.exceptions import LuxPowerCommunicationError
from custom_components.lxp_modbus.read_profiles import InputReadBlock
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxReadDiagnosticJournal,
    LuxReadPurpose,
    LuxReadRequestContext,
)
from luxpower.profile_validation import (
    _load_private_target,
    _parse_implementation_revision,
    _verify_live_source_revision,
    _write_private_report,
)

FC4_MATRIX_SCHEMA_VERSION = 1
FC4_MATRIX_VERSION = "1.0"
FIRST_BLOCK = InputReadBlock(0, 40)
SECOND_BLOCK = InputReadBlock(80, 40)
DEFAULT_QUIET_PERIOD_SECONDS = 1.0
DEFAULT_BETWEEN_REPETITION_COOLDOWN_SECONDS = 1.0
MAX_CONSECUTIVE_TIMEOUT_REPETITIONS = 5


@dataclass(frozen=True)
class FC4MatrixCell:
    """One block-order and pacing combination."""

    name: str
    first_block: InputReadBlock
    second_block: InputReadBlock
    quiet_period_seconds: float


MATRIX_CELLS: Mapping[str, FC4MatrixCell] = {
    "A1": FC4MatrixCell("A1", FIRST_BLOCK, SECOND_BLOCK, 0.0),
    "A2": FC4MatrixCell(
        "A2", FIRST_BLOCK, SECOND_BLOCK, DEFAULT_QUIET_PERIOD_SECONDS
    ),
    "B1": FC4MatrixCell("B1", SECOND_BLOCK, FIRST_BLOCK, 0.0),
    "B2": FC4MatrixCell(
        "B2", SECOND_BLOCK, FIRST_BLOCK, DEFAULT_QUIET_PERIOD_SECONDS
    ),
}

# Every four-repetition round contains each cell exactly once. Alternating the
# round order prevents any cell from permanently occupying the same time slot.
_COUNTERBALANCED_ROUNDS = (
    ("A1", "B2", "A2", "B1"),
    ("B1", "A2", "B2", "A1"),
)


def counterbalanced_sequence(repetitions_per_cell: int) -> tuple[str, ...]:
    """Return a deterministic balanced sequence with the requested cell count."""
    if repetitions_per_cell <= 0:
        raise ValueError("repetitions_per_cell must be positive")
    sequence: list[str] = []
    for round_index in range(repetitions_per_cell):
        sequence.extend(_COUNTERBALANCED_ROUNDS[round_index % 2])
    return tuple(sequence)


def _nearest_rank(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def _distribution(values: Sequence[float]) -> dict:
    samples = tuple(float(value) for value in values)
    return {
        "samples": len(samples),
        "median": round(statistics.median(samples), 3) if samples else None,
        "p95": (
            round(_nearest_rank(samples, 0.95), 3)
            if len(samples) >= 20
            else None
        ),
        "min": round(min(samples), 3) if samples else None,
        "max": round(max(samples), 3) if samples else None,
    }


def _percentage(numerator: int, denominator: int) -> float | None:
    return round(100 * numerator / denominator, 6) if denominator else None


def _request_record(request, *, position: int, cell: FC4MatrixCell) -> dict:
    """Serialize only Stage 9 sanitized request metadata."""
    return {
        "position": position,
        "block": {
            "start": request.register_start,
            "count": request.register_count,
            "end": request.register_end,
        },
        "cell_pacing_assignment": (
            "quiet" if cell.quiet_period_seconds else "immediate"
        ),
        "pacing_exposure": (
            "not_exposed"
            if position == 1
            else ("quiet" if cell.quiet_period_seconds else "immediate")
        ),
        "outcome": request.outcome.value,
        "accepted_response_latency_ms": request.accepted_response_latency_ms,
        "measured_gap_from_previous_accepted_response_seconds": (
            request.time_since_previous_accepted_response_seconds
            if position == 2
            else None
        ),
        "connection_generation": request.generation,
        "connection_age_seconds": request.connection_age_seconds,
        "requests_previously_on_generation": (
            request.requests_previously_on_generation
        ),
        "unmatched_fc4_while_pending": request.unmatched_fc4_while_pending,
        "fc193_while_pending": request.fc193_while_pending,
        "invalid_frames_while_pending": request.invalid_frames_while_pending,
        "drain_duration_ms": request.drain_duration_ms,
        "elapsed_ms": request.elapsed_ms,
        "generation_invalidated": request.generation_invalidated,
    }


def _summarize_repetitions(repetitions: Sequence[Mapping[str, object]]) -> dict:
    request_records = [
        request
        for repetition in repetitions
        for request in repetition["requests"]
    ]

    def summarize_requests(records: Sequence[Mapping[str, object]]) -> dict:
        attempts = len(records)
        timeouts = sum(record["outcome"] == "response_timeout" for record in records)
        latencies = [
            float(record["accepted_response_latency_ms"])
            for record in records
            if record["accepted_response_latency_ms"] is not None
        ]
        return {
            "attempts": attempts,
            "successes": sum(record["outcome"] == "success" for record in records),
            "timeouts": timeouts,
            "timeout_percent_per_request": _percentage(timeouts, attempts),
            "accepted_response_latency_ms": _distribution(latencies),
        }

    by_cell: dict[str, dict] = {}
    for cell_name in MATRIX_CELLS:
        cell_repetitions = [
            repetition
            for repetition in repetitions
            if repetition["cell"] == cell_name
        ]
        cell_requests = [
            request
            for repetition in cell_repetitions
            for request in repetition["requests"]
        ]
        timed_out_repetitions = sum(
            any(request["outcome"] == "response_timeout" for request in repetition["requests"])
            for repetition in cell_repetitions
        )
        gaps = [
            float(request["measured_gap_from_previous_accepted_response_seconds"])
            for request in cell_requests
            if request["position"] == 2
            and request["measured_gap_from_previous_accepted_response_seconds"] is not None
        ]
        by_cell[cell_name] = {
            "repetitions_attempted": len(cell_repetitions),
            "repetitions_completed": sum(
                repetition["completed"] for repetition in cell_repetitions
            ),
            "first_request_timeouts": sum(
                request["position"] == 1 and request["outcome"] == "response_timeout"
                for request in cell_requests
            ),
            "second_request_timeouts": sum(
                request["position"] == 2 and request["outcome"] == "response_timeout"
                for request in cell_requests
            ),
            "timeout_percent_per_repetition": _percentage(
                timed_out_repetitions, len(cell_repetitions)
            ),
            "request_metrics": summarize_requests(cell_requests),
            "actual_second_request_gap_seconds": _distribution(gaps),
            "unmatched_fc4": sum(
                int(repetition["session_metrics"]["unmatched_fc4_observations"])
                for repetition in cell_repetitions
            ),
            "fc193": sum(
                int(repetition["session_metrics"]["function_193_frames"])
                for repetition in cell_repetitions
            ),
            "invalid_frames": sum(
                int(repetition["session_metrics"]["invalid_frames"])
                for repetition in cell_repetitions
            ),
        }

    def grouped(field: str, values: Sequence[object]) -> dict:
        return {
            str(value): summarize_requests(
                [request for request in request_records if request[field] == value]
            )
            for value in values
        }

    by_block = {
        str(start): summarize_requests(
            [request for request in request_records if request["block"]["start"] == start]
        )
        for start in (0, 80)
    }
    by_position = grouped("position", (1, 2))
    by_cell_pacing_assignment = grouped(
        "cell_pacing_assignment", ("immediate", "quiet")
    )
    by_pacing = {
        pacing: summarize_requests(
            [
                request
                for request in request_records
                if request["pacing_exposure"] == pacing
            ]
        )
        for pacing in ("immediate", "quiet")
    }
    pacing_interactions = {
        f"block_{start}_second_{pacing}": summarize_requests(
            [
                request
                for request in request_records
                if request["block"]["start"] == start
                and request["position"] == 2
                and request["pacing_exposure"] == pacing
            ]
        )
        for start in (0, 80)
        for pacing in ("immediate", "quiet")
    }
    cell_assignment_interactions = {
        f"block_{start}_{position}_{pacing}_cell": summarize_requests(
            [
                request
                for request in request_records
                if request["block"]["start"] == start
                and request["position"] == position
                and request["cell_pacing_assignment"] == pacing
            ]
        )
        for start in (0, 80)
        for position in (1, 2)
        for pacing in ("immediate", "quiet")
    }
    return {
        "per_cell": by_cell,
        "by_block": by_block,
        "by_ordinal_position": by_position,
        "by_pacing": by_pacing,
        "by_cell_pacing_assignment": by_cell_pacing_assignment,
        "pacing_interactions_second_request_only": pacing_interactions,
        "cell_assignment_interactions_non_causal": cell_assignment_interactions,
    }


SessionFactory = Callable[[], LuxReadSession]
Sleep = Callable[[float], Awaitable[None]]


async def execute_fc4_matrix(
    session_factory: SessionFactory,
    *,
    repetitions_per_cell: int = 10,
    between_repetition_cooldown_seconds: float = (
        DEFAULT_BETWEEN_REPETITION_COOLDOWN_SECONDS
    ),
    sleep: Sleep = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    implementation_revision: str,
    implementation_revision_verified: bool,
) -> dict:
    """Execute the isolated read-only matrix without recovery or request skipping."""
    if between_repetition_cooldown_seconds < 0:
        raise ValueError("between-repetition cooldown cannot be negative")
    if not implementation_revision_verified:
        raise ValueError("matrix requires independently verified revision provenance")
    sequence = counterbalanced_sequence(repetitions_per_cell)
    report: dict = {
        "schema_version": FC4_MATRIX_SCHEMA_VERSION,
        "experiment_version": FC4_MATRIX_VERSION,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "provenance": {
            "implementation_revision": implementation_revision,
            "revision_source": "clean_git_checkout_verified",
            "diagnostic_schema_version": LuxReadDiagnosticJournal.SCHEMA_VERSION,
            "run_mode": "fc4_order_pacing_matrix",
        },
        "safety": {
            "read_only": True,
            "function_code": 4,
            "writes_exposed": False,
            "recovery_enabled": False,
            "unsolicited_may_skip_planned_request": False,
            "home_assistant_condition": "operator_managed_normal_state",
        },
        "configuration": {
            "request_timeout_seconds": None,
            "repetitions_per_cell": repetitions_per_cell,
            "within_repetition_quiet_seconds": DEFAULT_QUIET_PERIOD_SECONDS,
            "between_repetition_cooldown_seconds": (
                between_repetition_cooldown_seconds
            ),
            "fresh_session_per_repetition": True,
            "maximum_consecutive_timeout_repetitions": (
                MAX_CONSECUTIVE_TIMEOUT_REPETITIONS
            ),
            "matrix": {name: asdict(cell) for name, cell in MATRIX_CELLS.items()},
            "sequence_strategy": "alternating_balanced_four-cell_rounds_v1",
            "run_sequence": list(sequence),
        },
        "repetitions": [],
        "aborted": False,
        "abort_reason": None,
    }

    consecutive_connection_failures = 0
    consecutive_timeout_repetitions = 0
    for order_index, cell_name in enumerate(sequence, start=1):
        cell = MATRIX_CELLS[cell_name]
        repetition_index = 1 + sum(
            repetition["cell"] == cell_name for repetition in report["repetitions"]
        )
        session = session_factory()
        actual_timeout = session.request_timeout_seconds
        configured_timeout = report["configuration"]["request_timeout_seconds"]
        if configured_timeout is None:
            report["configuration"]["request_timeout_seconds"] = actual_timeout
        elif actual_timeout != configured_timeout:
            raise ValueError("every matrix session must use the same request timeout")
        started = monotonic()
        communication_failure: str | None = None
        close_reason = "completed"
        connected = False
        try:
            try:
                await session.async_connect()
                connected = True
            except LuxPowerCommunicationError as exc:
                communication_failure = type(exc).__name__
                close_reason = "connection_establishment_failure"
                consecutive_connection_failures += 1

            if connected:
                context = LuxReadRequestContext(purpose=LuxReadPurpose.FORCED_PREFLIGHT)
                for position, block in enumerate(
                    (cell.first_block, cell.second_block), start=1
                ):
                    if position == 2 and cell.quiet_period_seconds:
                        await sleep(cell.quiet_period_seconds)
                    try:
                        await session.async_read_input(
                            block.start,
                            block.count,
                            context=context,
                        )
                        if session.metrics().invalid_frames:
                            communication_failure = "invalid_frame_observed"
                            close_reason = "protocol_safety_stop"
                            break
                    except LuxPowerCommunicationError as exc:
                        communication_failure = type(exc).__name__
                        close_reason = f"request_{position}_failure"
                        break
        finally:
            await session.async_close()

        diagnostics = session.diagnostics()
        metrics = session.metrics()
        requests = [
            _request_record(request, position=position, cell=cell)
            for position, request in enumerate(diagnostics.requests, start=1)
        ]
        completed = bool(
            len(requests) == 2
            and all(request["outcome"] == "success" for request in requests)
        )
        repetition = {
            "cell": cell_name,
            "cell_repetition_index": repetition_index,
            "run_order_index": order_index,
            "first_block": asdict(cell.first_block),
            "second_block": asdict(cell.second_block),
            "intended_quiet_period_seconds": cell.quiet_period_seconds,
            "completed": completed,
            "communication_failure": communication_failure,
            "close_reason": close_reason,
            "connection_generation": (
                diagnostics.requests[0].generation if diagnostics.requests else None
            ),
            "connection_generations_created": metrics.connections,
            "total_duration_seconds": round(monotonic() - started, 6),
            "requests": requests,
            "session_metrics": asdict(metrics),
            "diagnostics": asdict(diagnostics),
        }
        report["repetitions"].append(repetition)

        print(
            f"FC4 matrix {order_index}/{len(sequence)} {cell_name}: "
            f"{'complete' if completed else close_reason}",
            file=sys.stderr,
            flush=True,
        )

        if metrics.invalid_frames:
            report["aborted"] = True
            report["abort_reason"] = "invalid_frame_observed"
            break
        if metrics.ambiguous_requests:
            report["aborted"] = True
            report["abort_reason"] = "ambiguous_request_observed"
            break
        if metrics.modbus_rejections:
            report["aborted"] = True
            report["abort_reason"] = "modbus_rejection_observed"
            break
        timed_out = any(
            request["outcome"] == "response_timeout" for request in requests
        )
        consecutive_timeout_repetitions = (
            consecutive_timeout_repetitions + 1 if timed_out else 0
        )
        if (
            consecutive_timeout_repetitions
            >= MAX_CONSECUTIVE_TIMEOUT_REPETITIONS
        ):
            report["aborted"] = True
            report["abort_reason"] = "five_consecutive_timeout_repetitions"
            break
        if metrics.connection_losses:
            consecutive_connection_failures += 1
        elif connected:
            consecutive_connection_failures = 0
        if consecutive_connection_failures >= 3:
            report["aborted"] = True
            report["abort_reason"] = "three_consecutive_connection_failures"
            break
        if order_index < len(sequence) and between_repetition_cooldown_seconds:
            await sleep(between_repetition_cooldown_seconds)

    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    report["sequence_completed"] = [
        repetition["cell"] for repetition in report["repetitions"]
    ]
    report["cell_counts"] = dict(Counter(report["sequence_completed"]))
    report["analysis"] = _summarize_repetitions(report["repetitions"])
    return report


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="LuxPower FC4 order/pacing matrix (STRICTLY READ-ONLY)"
    )
    parser.add_argument("--confirm-read-only", action="store_true")
    parser.add_argument(
        "--implementation-revision", type=_parse_implementation_revision
    )
    parser.add_argument("--repetitions-per-cell", type=int, default=10)
    parser.add_argument(
        "--between-repetition-cooldown-seconds",
        type=float,
        default=DEFAULT_BETWEEN_REPETITION_COOLDOWN_SECONDS,
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser


async def _async_main(arguments: argparse.Namespace) -> int:
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    implementation_revision = _verify_live_source_revision(
        arguments.implementation_revision
    )
    host, port, dongle, inverter = _load_private_target(os.environ)

    def session_factory() -> LuxReadSession:
        return LuxReadSession(
            host,
            dongle,
            inverter,
            port=port,
            request_timeout=READ_TIMEOUT,
        )

    report = await execute_fc4_matrix(
        session_factory,
        repetitions_per_cell=arguments.repetitions_per_cell,
        between_repetition_cooldown_seconds=(
            arguments.between_repetition_cooldown_seconds
        ),
        implementation_revision=implementation_revision,
        implementation_revision_verified=True,
    )
    serialized = json.dumps(report, indent=2, sort_keys=True)
    _write_private_report(arguments.output, serialized)
    print(
        f"LuxPower FC4 READ-ONLY matrix completed: "
        f"{len(report['repetitions'])} repetitions",
        file=sys.stderr,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    arguments = parser.parse_args(argv)
    try:
        return asyncio.run(_async_main(arguments))
    except ValueError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
