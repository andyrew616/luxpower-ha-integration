"""Read-only live validation for the critical Lux energy-flow profile."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
from typing import Mapping, Sequence

from custom_components.lxp_modbus.const import READ_TIMEOUT
from custom_components.lxp_modbus.exceptions import LuxPowerCommunicationError
from custom_components.lxp_modbus.observation import utc_now
from custom_components.lxp_modbus.read_profiles import (
    ENERGY_FLOW_PROFILE_DEFINITION_VERSION,
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    profile_block_details,
)
from custom_components.lxp_modbus.recovery import RecoveryPolicy
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxReadDiagnosticJournal,
    LuxReadDiagnosticsSnapshot,
    LuxReadPurpose,
    LuxReadRequestDiagnostic,
    LuxReadRequestOutcome,
)
from luxpower.hybrid import (
    LuxPowerHybridReadClient,
    _latency_summary,
    _metrics_delta,
)

PROFILE_VALIDATION_SCHEMA_VERSION = 4
PROFILE_VALIDATION_VERSION = "4.0"


def _maximum_age(freshness: Mapping[str, object]) -> float | None:
    """Use unrounded age for decisions, with schema-v3 compatibility fallback."""
    value = freshness.get("max_age_seconds_raw", freshness["max_age_seconds"])
    return float(value) if value is not None else None


def _nearest_rank_p95(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def _nearest_rank_p99(values: Sequence[float]) -> float | None:
    """Return p99 only when at least 100 samples support that claim."""
    if len(values) < 100:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.99) - 1)]


def summarize_profile_samples(samples: Sequence[Mapping[str, object]]) -> dict:
    """Summarize independently sampled worst-required-register freshness."""
    freshness = [sample["profile_freshness"] for sample in samples]
    complete = [
        item
        for item in freshness
        if item["known"] == item["required"] and item["max_age_seconds"] is not None
    ]
    ages = [_maximum_age(item) for item in complete]
    ages = [age for age in ages if age is not None]
    outliers: dict[str, int] = {}
    for item in complete:
        register = str(item["worst_register"])
        outliers[register] = outliers.get(register, 0) + 1
    return {
        "samples": len(samples),
        "complete_samples": len(complete),
        "median_worst_age_seconds": round(statistics.median(ages), 3) if ages else None,
        "p95_worst_age_seconds": round(_nearest_rank_p95(ages), 3) if ages else None,
        "p99_worst_age_seconds": round(_nearest_rank_p99(ages), 3) if len(ages) >= 100 else None,
        "max_worst_age_seconds": round(max(ages), 3) if ages else None,
        "worst_register_sample_counts": outliers,
    }


def _time_beyond_target(
    samples: Sequence[Mapping[str, object]], target_seconds: float
) -> float:
    """Integrate sampled stale intervals using their actual UTC timestamps."""
    total = 0.0
    for current, following in zip(samples, samples[1:]):
        freshness = current["profile_freshness"]
        stale = bool(
            _maximum_age(freshness) is None
            or freshness["known"] != freshness["required"]
            or _maximum_age(freshness) > target_seconds
        )
        if stale:
            total += (
                datetime.fromisoformat(following["at"])
                - datetime.fromisoformat(current["at"])
            ).total_seconds()
    return total


def _time_beyond_target_by_health_state(
    samples: Sequence[Mapping[str, object]], target_seconds: float
) -> dict[str, float]:
    """Partition stale time by instantaneous state without claiming causality."""
    totals = {"while_recovering": 0.0, "outside_recovering": 0.0}
    for current, following in zip(samples, samples[1:]):
        freshness = current["profile_freshness"]
        stale = bool(
            _maximum_age(freshness) is None
            or freshness["known"] != freshness["required"]
            or _maximum_age(freshness) > target_seconds
        )
        if not stale:
            continue
        bucket = (
            "while_recovering"
            if current.get("acquisition_health") == "recovering"
            else "outside_recovering"
        )
        totals[bucket] += (
            datetime.fromisoformat(following["at"])
            - datetime.fromisoformat(current["at"])
        ).total_seconds()
    return {name: round(value, 3) for name, value in totals.items()}


def _time_beyond_target_by_recovery_episode(
    samples: Sequence[Mapping[str, object]],
    target_seconds: float,
    recovery_events: Sequence[Mapping[str, object]],
) -> dict[str, float]:
    """Causally attribute stale intervals from failed request start to recovery end."""
    episodes = tuple(
        (
            datetime.fromisoformat(str(event["episode_started_at"])),
            datetime.fromisoformat(str(event["ended_at"])),
        )
        for event in recovery_events
    )
    totals = {"recovery_episode": 0.0, "normal_operation": 0.0}
    for current, following in zip(samples, samples[1:]):
        freshness = current["profile_freshness"]
        stale = bool(
            _maximum_age(freshness) is None
            or freshness["known"] != freshness["required"]
            or _maximum_age(freshness) > target_seconds
        )
        if not stale:
            continue
        started = datetime.fromisoformat(str(current["at"]))
        bucket = (
            "recovery_episode"
            if any(episode_start <= started <= episode_end for episode_start, episode_end in episodes)
            else "normal_operation"
        )
        totals[bucket] += (
            datetime.fromisoformat(str(following["at"])) - started
        ).total_seconds()
    return {name: round(value, 3) for name, value in totals.items()}


def _violation_episode_summary(
    samples: Sequence[Mapping[str, object]],
    target_seconds: float,
    recovery_events: Sequence[Mapping[str, object]],
) -> dict:
    """Describe contiguous sampled SLA violations without omitting recovery."""
    recovery_ranges = tuple(
        (
            datetime.fromisoformat(str(event["episode_started_at"])),
            datetime.fromisoformat(str(event["ended_at"])),
            getattr(event["failure_kind"], "value", str(event["failure_kind"])),
        )
        for event in recovery_events
    )
    episodes: list[dict] = []
    active: dict | None = None
    intervals = []
    for current, following in zip(samples, samples[1:]):
        started = datetime.fromisoformat(str(current["at"]))
        ended = datetime.fromisoformat(str(following["at"]))
        intervals.append((ended - started).total_seconds())
        freshness = current["profile_freshness"]
        stale = bool(
            _maximum_age(freshness) is None
            or freshness["known"] != freshness["required"]
            or _maximum_age(freshness) > target_seconds
        )
        if stale:
            age = _maximum_age(freshness)
            if active is None:
                active = {
                    "started_at": started,
                    "ended_at": ended,
                    "duration_seconds": 0.0,
                    "maximum_age_seconds": float(age) if age is not None else None,
                }
            active["ended_at"] = ended
            active["duration_seconds"] += (ended - started).total_seconds()
            if age is not None:
                active["maximum_age_seconds"] = max(
                    active["maximum_age_seconds"] or float(age), float(age)
                )
            continue
        if active is not None:
            episodes.append(active)
            active = None
    if active is not None:
        episodes.append(active)

    serialized = []
    for episode in episodes:
        causes = sorted({
            failure_kind
            for recovery_start, recovery_end, failure_kind in recovery_ranges
            if recovery_start <= episode["ended_at"]
            and recovery_end >= episode["started_at"]
        })
        serialized.append({
            "started_at": episode["started_at"].isoformat(),
            "ended_at": episode["ended_at"].isoformat(),
            "duration_seconds": round(episode["duration_seconds"], 3),
            "maximum_age_seconds": (
                round(episode["maximum_age_seconds"], 3)
                if episode["maximum_age_seconds"] is not None
                else None
            ),
            "cause": causes or ["undetermined_non_recovery"],
        })
    durations = [episode["duration_seconds"] for episode in serialized]
    return {
        "count": len(serialized),
        "longest_duration_seconds": round(max(durations), 3) if durations else 0.0,
        "sampling_interval_seconds": {
            "median": round(statistics.median(intervals), 6) if intervals else None,
            "max": round(max(intervals), 6) if intervals else None,
        },
        "episodes": serialized,
    }


def _recovery_metrics_delta(before, after) -> dict:
    fields = (
        "timeout_count",
        "connection_loss_count",
        "connection_establishment_failure_count",
        "ambiguous_request_count",
        "reconnect_attempts",
        "successful_reconnects",
        "failed_reconnects",
        "completed_recoveries",
        "retry_budget_exhausted",
        "acquisitions_abandoned",
        "connection_generations_created",
    )
    result = {name: getattr(after, name) - getattr(before, name) for name in fields}
    result["events"] = [
        asdict(event) for event in after.events[len(before.events):]
    ]
    result["final_health"] = after.health.value
    return result


def _numeric_summary(values: Sequence[float | None]) -> dict:
    """Summarize present finite timing values without inventing missing data."""
    present = [float(value) for value in values if value is not None]
    if not present:
        return {"samples": 0, "median": None, "p95": None, "min": None, "max": None}
    return {
        "samples": len(present),
        "median": round(statistics.median(present), 6),
        "p95": round(_nearest_rank_p95(present), 6),
        "min": round(min(present), 6),
        "max": round(max(present), 6),
    }


def _request_group_summary(
    requests: Sequence[LuxReadRequestDiagnostic],
) -> dict:
    attempts = len(requests)
    successes = [
        request
        for request in requests
        if request.outcome is LuxReadRequestOutcome.SUCCESS
    ]
    timeouts = [
        request
        for request in requests
        if request.outcome is LuxReadRequestOutcome.RESPONSE_TIMEOUT
    ]
    return {
        "attempts": attempts,
        "successes": len(successes),
        "timeouts": len(timeouts),
        "timeout_percent": (
            round(100 * len(timeouts) / attempts, 6) if attempts else None
        ),
        "outcomes": {
            outcome.value: sum(request.outcome is outcome for request in requests)
            for outcome in LuxReadRequestOutcome
            if any(request.outcome is outcome for request in requests)
        },
        "accepted_response_latency_ms": _latency_summary(
            [
                request.accepted_response_latency_ms
                for request in successes
                if request.accepted_response_latency_ms is not None
            ],
            samples_total=len(successes),
        ),
        "request_start_spacing_seconds": _numeric_summary(
            [request.time_since_previous_request_start_seconds for request in requests]
        ),
        "connection_age_seconds": _numeric_summary(
            [request.connection_age_seconds for request in requests]
        ),
        "requests_previously_on_generation": _numeric_summary(
            [float(request.requests_previously_on_generation) for request in requests]
        ),
    }


def _analyze_request_diagnostics(
    requests: Sequence[LuxReadRequestDiagnostic],
    *,
    complete: bool,
) -> dict:
    by_block: dict[str, list[LuxReadRequestDiagnostic]] = {}
    by_purpose: dict[str, list[LuxReadRequestDiagnostic]] = {}
    for request in requests:
        block = f"{request.register_start}-{request.register_end}"
        by_block.setdefault(block, []).append(request)
        by_purpose.setdefault(request.purpose.value, []).append(request)
    successful = [
        request
        for request in requests
        if request.outcome is LuxReadRequestOutcome.SUCCESS
    ]
    failed = [
        request
        for request in requests
        if request.outcome is not LuxReadRequestOutcome.SUCCESS
    ]
    timed_out = [
        request
        for request in requests
        if request.outcome is LuxReadRequestOutcome.RESPONSE_TIMEOUT
    ]
    generations: dict[str, dict[str, int]] = {}
    for request in requests:
        generation = str(request.generation)
        item = generations.setdefault(
            generation,
            {"attempts": 0, "successes": 0, "timeouts": 0},
        )
        item["attempts"] += 1
        item["successes"] += request.outcome is LuxReadRequestOutcome.SUCCESS
        item["timeouts"] += request.outcome is LuxReadRequestOutcome.RESPONSE_TIMEOUT
    return {
        "complete_request_history": complete,
        "overall": _request_group_summary(requests),
        "by_block": {
            block: _request_group_summary(group)
            for block, group in sorted(by_block.items())
        },
        "by_purpose": {
            purpose: _request_group_summary(group)
            for purpose, group in sorted(by_purpose.items())
        },
        "successful_vs_failed": {
            "successful_request_start_spacing_seconds": _numeric_summary(
                [
                    request.time_since_previous_request_start_seconds
                    for request in successful
                ]
            ),
            "failed_request_start_spacing_seconds": _numeric_summary(
                [
                    request.time_since_previous_request_start_seconds
                    for request in failed
                ]
            ),
            "successful_connection_age_seconds": _numeric_summary(
                [request.connection_age_seconds for request in successful]
            ),
            "failed_connection_age_seconds": _numeric_summary(
                [request.connection_age_seconds for request in failed]
            ),
        },
        "traffic_near_timeouts": {
            "timeout_requests": len(timed_out),
            "unmatched_fc4_while_pending": sum(
                request.unmatched_fc4_while_pending for request in timed_out
            ),
            "fc193_while_pending": sum(
                request.fc193_while_pending for request in timed_out
            ),
            "invalid_frames_while_pending": sum(
                request.invalid_frames_while_pending for request in timed_out
            ),
            "time_since_previous_unmatched_fc4_seconds": _numeric_summary(
                [request.time_since_previous_unmatched_fc4_seconds for request in timed_out]
            ),
            "time_since_previous_fc193_seconds": _numeric_summary(
                [request.time_since_previous_fc193_seconds for request in timed_out]
            ),
        },
        "requests_by_generation": generations,
        "late_old_generation_response": {
            "observation_supported": False,
            "detected": None,
            "limitation": (
                "generation fencing rejects old bytes before frame routing; "
                "absence of a decoded late frame is not proof that no bytes arrived"
            ),
        },
    }


def _diagnostic_delta(
    before: LuxReadDiagnosticsSnapshot | None,
    after: LuxReadDiagnosticsSnapshot | None,
) -> dict:
    """Return a cursor-based bounded phase view with explicit truncation."""
    if before is None or after is None:
        return {
            "available": False,
            "reason": "client does not expose Stage 9 request diagnostics",
        }
    request_cursor = before.requests_total
    event_cursor = before.events_total
    expected_requests = after.requests_total - request_cursor
    expected_events = after.events_total - event_cursor
    requests = tuple(
        request
        for request in after.requests
        if request.request_sequence > request_cursor
    )
    events = tuple(
        event for event in after.events if event.sequence > event_cursor
    )
    episodes = tuple(
        episode
        for episode in after.timeout_episodes
        if episode.request.request_sequence > request_cursor
    )
    requests_complete = len(requests) == expected_requests
    events_complete = len(events) == expected_events
    return {
        "available": True,
        "schema_version": after.schema_version,
        "request_cursor": request_cursor,
        "event_cursor": event_cursor,
        "expected_requests": expected_requests,
        "retained_requests": len(requests),
        "request_history_complete": requests_complete,
        "expected_events": expected_events,
        "retained_events": len(events),
        "event_history_complete": events_complete,
        "retention": {
            "request_capacity": after.request_capacity,
            "requests_total": after.requests_total,
            "requests_dropped": after.requests_dropped,
            "event_capacity": after.event_capacity,
            "events_total": after.events_total,
            "events_dropped": after.events_dropped,
            "failure_capacity": after.failure_capacity,
            "failures_total": after.failures_total,
            "failures_dropped": after.failures_dropped,
        },
        "requests": [asdict(request) for request in requests],
        "events": [asdict(event) for event in events],
        "timeout_episodes": [asdict(episode) for episode in episodes],
        "analysis": _analyze_request_diagnostics(
            requests,
            complete=requests_complete,
        ),
    }


def _client_diagnostics(
    client: LuxPowerHybridReadClient,
) -> LuxReadDiagnosticsSnapshot | None:
    diagnostics = getattr(client, "diagnostics", None)
    return diagnostics() if diagnostics is not None else None


def aggregate_qualification_reports(
    reports: Sequence[Mapping[str, object]],
) -> dict:
    """Aggregate sanitized sustained phases without inferring long-term rates."""
    phases: list[tuple[Mapping[str, object], Mapping[str, object]]] = []
    for report in reports:
        if int(report.get("schema_version", 0)) not in (3, 4):
            raise ValueError("qualification aggregation requires schema-v3/v4 reports")
        phases.extend(
            (report, phase)
            for phase in report.get("phases", [])
            if str(phase.get("name", "")).startswith("sustained_burn_in")
        )
    if not phases:
        raise ValueError("no sustained qualification phases found")
    targets = {float(phase["target_seconds"]) for _, phase in phases}
    if len(targets) != 1:
        raise ValueError("qualification reports use different freshness targets")

    runtime = sum(float(phase["actual_duration_seconds"]) for _, phase in phases)
    session_fields = (
        "explicit_requests",
        "expected_fc4_responses",
        "unmatched_fc4_observations",
        "request_timeouts",
        "connection_losses",
        "invalid_frames",
        "function_193_frames",
        "observation_queue_drops",
        "connection_failures",
    )
    session_totals = {
        field: sum(int(phase["session_metrics"][field]) for _, phase in phases)
        for field in session_fields
    }
    recovery_fields = (
        "reconnect_attempts",
        "successful_reconnects",
        "failed_reconnects",
        "retry_budget_exhausted",
    )
    recovery_totals = {
        field: sum(int(phase["recovery_metrics"][field]) for _, phase in phases)
        for field in recovery_fields
    }
    latency_values = [
        float(value)
        for _, phase in phases
        for value in phase["session_metrics"]["request_latency_ms"]["values_ms"]
    ]
    latency_samples_total = sum(
        int(phase["session_metrics"]["request_latency_ms"]["samples_total"])
        for _, phase in phases
    )
    attempts = session_totals["explicit_requests"]
    timeouts = session_totals["request_timeouts"]
    reconnects = recovery_totals["reconnect_attempts"]
    avoided = sum(
        int(phase["profile_source_metrics"]["explicit_requests_avoided_unsolicited"])
        for _, phase in phases
    )
    violations = sum(int(phase["stale_threshold_violations"]) for _, phase in phases)
    stale_duration = sum(
        float(phase["sampled_time_beyond_target_seconds"])
        for _, phase in phases
    )
    longest = max(
        float(phase["violation_episodes"]["longest_duration_seconds"])
        for _, phase in phases
    )
    return {
        "schema_version": 1,
        "source_report_schema_version": (
            next(iter({int(report["schema_version"]) for report, _ in phases}))
            if len({int(report["schema_version"]) for report, _ in phases}) == 1
            else sorted({int(report["schema_version"]) for report, _ in phases})
        ),
        "target_seconds": targets.pop(),
        "sustained_runs": len(phases),
        "total_runtime_seconds": round(runtime, 3),
        "total_runtime_hours": round(runtime / 3600, 6),
        "session_totals": session_totals,
        "connection_generations": sum(
            int(report["terminal_shutdown"]["connection_generations_created"])
            for report in reports
            if any(
                str(phase.get("name", "")).startswith("sustained_burn_in")
                for phase in report.get("phases", [])
            )
        ),
        "recovery_totals": recovery_totals,
        "explicit_requests_avoided_unsolicited": avoided,
        "observed_rates": {
            "timeouts_per_explicit_request": (
                round(timeouts / attempts, 9) if attempts else None
            ),
            "timeouts_per_hour": round(timeouts * 3600 / runtime, 6),
            "reconnects_per_hour": round(reconnects * 3600 / runtime, 6),
        },
        "request_latency_ms": _latency_summary(
            latency_values, samples_total=latency_samples_total
        ),
        "freshness": {
            "strict_target_met": all(bool(phase["target_met"]) for _, phase in phases),
            "violating_samples": violations,
            "sampled_time_beyond_target_seconds": round(stale_duration, 3),
            "longest_violation_episode_seconds": round(longest, 3),
            "maximum_worst_age_seconds": max(
                float(phase["freshness"]["max_worst_age_seconds"])
                for _, phase in phases
            ),
        },
        "natural_recovery_events": [
            event
            for _, phase in phases
            for event in phase["recovery_metrics"]["events"]
        ],
        "sample_size_limitation": (
            "Observed qualification rates are not guaranteed long-term rates."
        ),
    }


async def _run_profile_phase(
    client: LuxPowerHybridReadClient,
    *,
    name: str,
    target_seconds: float,
    duration_seconds: float,
) -> dict:
    before_session = client.metrics()
    before_profile = client.profile_metrics()
    before_recovery = client.recovery_metrics()
    before_diagnostics = _client_diagnostics(client)
    samples: list[dict] = []
    error = None
    started = time.monotonic()
    try:
        await client.async_run_profile(duration_seconds, sample_sink=samples)
    except LuxPowerCommunicationError as exc:
        error = type(exc).__name__
    actual_duration = time.monotonic() - started
    after_session = client.metrics()
    after_profile = client.profile_metrics()
    after_recovery = client.recovery_metrics()
    after_diagnostics = _client_diagnostics(client)
    session_delta = _metrics_delta(before_session, after_session)
    attempted = (
        after_profile.explicit_requests_attempted
        - before_profile.explicit_requests_attempted
    )
    avoided = (
        after_profile.explicit_requests_avoided_unsolicited
        - before_profile.explicit_requests_avoided_unsolicited
    )
    freshness = summarize_profile_samples(samples)
    violations = sum(
        _maximum_age(item["profile_freshness"]) is None
        or item["profile_freshness"]["known"]
        != item["profile_freshness"]["required"]
        or _maximum_age(item["profile_freshness"]) > target_seconds
        for item in samples
    )
    unsafe_events = sum(
        session_delta[field]
        for field in (
            "invalid_frames",
            "observation_queue_drops",
        )
    )
    recovery_delta = _recovery_metrics_delta(before_recovery, after_recovery)
    recovery_safe = bool(
        not error
        and not unsafe_events
        and recovery_delta["failed_reconnects"] == 0
        and recovery_delta["retry_budget_exhausted"] == 0
        and all(
            event["outcome"] == "profile_recovered"
            for event in recovery_delta["events"]
        )
    )
    freshness_met = bool(samples and not violations)
    stale_by_health = _time_beyond_target_by_health_state(samples, target_seconds)
    stale_by_episode = _time_beyond_target_by_recovery_episode(
        samples, target_seconds, recovery_delta["events"]
    )
    violation_episodes = _violation_episode_summary(
        samples, target_seconds, recovery_delta["events"]
    )
    return {
        "name": name,
        "target_seconds": target_seconds,
        "duration_seconds": duration_seconds,
        "actual_duration_seconds": round(actual_duration, 3),
        "status": "failed" if error else "success",
        "error": error,
        "failed_block": (
            asdict(client.last_profile_request_block)
            if error and client.last_profile_request_block is not None
            else None
        ),
        "session_metrics": session_delta,
        "recovery_metrics": recovery_delta,
        "profile_source_metrics": {
            "explicit_requests_attempted": attempted,
            "explicit_requests_per_minute": round(
                attempted * 60 / actual_duration, 3
            ),
            "explicit_requests_avoided_unsolicited": avoided,
            "blocks_satisfied_unsolicited": avoided,
            "avoidance_percent": (
                round(100 * avoided / (attempted + avoided), 3)
                if attempted + avoided
                else None
            ),
        },
        "freshness": freshness,
        "stale_threshold_violations": violations,
        "sampled_time_beyond_target_seconds": round(
            _time_beyond_target(samples, target_seconds), 3
        ),
        "sampled_time_beyond_target_by_health_state_seconds": stale_by_health,
        "sampled_time_beyond_target_attribution_seconds": stale_by_episode,
        "violation_episodes": violation_episodes,
        "transport_recovery_safe": recovery_safe,
        "freshness_target_met": freshness_met,
        "target_met": bool(recovery_safe and freshness_met),
        "request_diagnostics": _diagnostic_delta(
            before_diagnostics,
            after_diagnostics,
        ),
    }


async def execute_profile_validation(
    client: LuxPowerHybridReadClient,
    *,
    targets: Sequence[float],
    short_runs: int,
    short_seconds: float,
    burn_seconds: float,
    forced_samples: int = 5,
    safety_margin_seconds: float = 0.25,
    implementation_revision: str | None = None,
    implementation_revision_verified: bool = False,
) -> dict:
    """Run progressive profile validation and stop at the first failed gate."""
    if not targets or tuple(sorted(targets, reverse=True)) != tuple(targets):
        raise ValueError("targets must run slowest to fastest")
    if min(targets) <= 0 or short_runs < 0 or forced_samples <= 0:
        raise ValueError("targets/forced samples must be positive; short runs nonnegative")
    if short_runs and short_seconds <= 0:
        raise ValueError("short duration must be positive when short runs are enabled")
    if burn_seconds < 0 or safety_margin_seconds <= 0:
        raise ValueError("burn duration must be nonnegative and safety margin positive")
    if short_runs == 0 and burn_seconds == 0:
        raise ValueError("at least one short or sustained phase is required")
    if client.profile is None:
        raise ValueError("profile validation requires a configured profile")
    if implementation_revision is not None and not re.fullmatch(
        r"[0-9a-f]{40}", implementation_revision
    ):
        raise ValueError("implementation_revision must be a lowercase 40-byte SHA")

    report = {
        "schema_version": PROFILE_VALIDATION_SCHEMA_VERSION,
        "validation_version": PROFILE_VALIDATION_VERSION,
        "provenance": {
            "implementation_revision": implementation_revision or "unrecorded",
            "revision_source": (
                "clean_git_checkout_verified"
                if implementation_revision_verified
                else "operator_supplied" if implementation_revision else "unavailable"
            ),
            "profile_definition_version": ENERGY_FLOW_PROFILE_DEFINITION_VERSION,
            "diagnostic_schema_version": LuxReadDiagnosticJournal.SCHEMA_VERSION,
            "run_mode": "critical_profile_timeout_diagnostics",
            "request_timeout_seconds": getattr(
                client, "request_timeout_seconds", READ_TIMEOUT
            ),
        },
        "started_at": utc_now().isoformat(),
        "safety": {
            "read_only": True,
            "permitted_function_codes": [4],
            "writes_exposed": False,
        },
        "profile": {
            "definition_version": ENERGY_FLOW_PROFILE_DEFINITION_VERSION,
            "name": client.profile.name.value,
            "grid_topology": client.profile.grid_topology.value,
            "active_pv_strings": sorted(client.profile.active_pv_strings),
            "load_layout": client.profile.load_layout.value,
            "required_registers": sorted(client.profile.required_registers),
            "blocks": list(profile_block_details(client.profile)),
        },
        "recovery_policy": (
            asdict(client.recovery_policy)
            if client.recovery_policy is not None
            else None
        ),
        "phases": [],
    }
    await client.async_connect()
    stable = True
    try:
        forced_diagnostics_before = _client_diagnostics(client)
        forced_durations = []
        for _ in range(forced_samples):
            result = await client.async_read_profile()
            forced_durations.append(result.duration_ms / 1000)
        forced_p95 = _nearest_rank_p95(forced_durations)
        report["forced_profile_refresh"] = {
            "samples": len(forced_durations),
            "durations_seconds": [round(value, 3) for value in forced_durations],
            "median_seconds": round(statistics.median(forced_durations), 3),
            "p95_seconds": round(forced_p95, 3),
            "max_seconds": round(max(forced_durations), 3),
            "target_seconds": targets[0],
            "target_interval_consumed_percent": round(
                100 * statistics.median(forced_durations) / targets[0], 3
            ),
            "p95_timing_headroom_seconds": round(
                targets[0] - forced_p95, 3
            ),
            # Retained only for schema-v3 readers of historical five-second runs.
            "five_second_interval_consumed_percent": (
                round(100 * statistics.median(forced_durations) / 5, 3)
                if targets[0] == 5
                else None
            ),
            "five_second_p95_timing_headroom_seconds": (
                round(5 - forced_p95, 3) if targets[0] == 5 else None
            ),
            "request_diagnostics": _diagnostic_delta(
                forced_diagnostics_before,
                _client_diagnostics(client),
            ),
        }

        for target in targets:
            trigger = target - forced_p95 - safety_margin_seconds
            if trigger <= 0:
                report["phases"].append(
                    {
                        "name": "gate",
                        "target_seconds": target,
                        "target_met": False,
                        "reason": "forced refresh leaves no positive scheduling margin",
                    }
                )
                break
            client.set_freshness_target(timedelta(seconds=trigger))
            for index in range(short_runs):
                phase = await _run_profile_phase(
                    client,
                    name=f"short_{index + 1}",
                    target_seconds=target,
                    duration_seconds=short_seconds,
                )
                phase["request_trigger_age_seconds"] = round(trigger, 3)
                report["phases"].append(phase)
                if not phase["target_met"]:
                    stable = False
                    break
            if not stable:
                break
            if burn_seconds:
                burn = await _run_profile_phase(
                    client,
                    name="sustained_burn_in",
                    target_seconds=target,
                    duration_seconds=burn_seconds,
                )
                burn["request_trigger_age_seconds"] = round(trigger, 3)
                report["phases"].append(burn)
                if not burn["target_met"]:
                    stable = False
                    break
    except LuxPowerCommunicationError as exc:
        report["fatal_error"] = type(exc).__name__
        block = client.last_profile_request_block
        report["fatal_failure"] = {
            "error": type(exc).__name__,
            "block": asdict(block) if block is not None else None,
        }
    finally:
        operational_health_before_shutdown = client.acquisition_health.value
        await client.async_close()
    report["final_session_metrics"] = asdict(client.metrics())
    report["final_profile_metrics"] = asdict(client.profile_metrics())
    report["final_recovery_metrics"] = asdict(client.recovery_metrics())
    final_diagnostics = _client_diagnostics(client)
    report["final_request_diagnostics"] = (
        asdict(final_diagnostics) if final_diagnostics is not None else None
    )
    report["terminal_shutdown"] = {
        "intentional": True,
        "operational_health_before_shutdown": operational_health_before_shutdown,
        "health_after_shutdown": client.acquisition_health.value,
        "connection_generations_created": client.metrics().connections,
    }
    report["completed_at"] = utc_now().isoformat()
    return report


def _load_private_target(environ: Mapping[str, str]) -> tuple[str, int, str, str]:
    required = ("LUXPOWER_HOST", "LUXPOWER_DONGLE_SERIAL", "LUXPOWER_INVERTER_SERIAL")
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise ValueError(f"missing required environment variables: {', '.join(missing)}")
    return (
        environ["LUXPOWER_HOST"],
        int(environ.get("LUXPOWER_PORT", "8000")),
        environ["LUXPOWER_DONGLE_SERIAL"],
        environ["LUXPOWER_INVERTER_SERIAL"],
    )


def _write_private_report(path: Path, serialized: str) -> None:
    """Write a sanitized live report with owner-only permissions."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(serialized + "\n")
    finally:
        os.chmod(path, 0o600)


def _parse_targets(value: str) -> tuple[float, ...]:
    targets = tuple(float(item.strip()) for item in value.split(","))
    if not targets or any(target <= 0 for target in targets):
        raise argparse.ArgumentTypeError("targets must be positive")
    if tuple(sorted(targets, reverse=True)) != targets:
        raise argparse.ArgumentTypeError("targets must run slowest to fastest")
    return targets


def _parse_pv_strings(value: str) -> frozenset[int]:
    strings = frozenset(int(item.strip()) for item in value.split(","))
    if not strings or not strings <= frozenset(range(1, 7)):
        raise argparse.ArgumentTypeError("PV strings must be comma-separated values 1-6")
    return strings


def _parse_implementation_revision(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise argparse.ArgumentTypeError(
            "implementation revision must be a lowercase 40-byte SHA"
        )
    return value


def _verify_live_source_revision(
    expected_revision: str | None,
    *,
    repository_root: Path | None = None,
) -> str:
    """Bind live evidence to the exact clean Git checkout being executed."""
    if expected_revision is None:
        raise ValueError("live execution requires --implementation-revision")
    root = repository_root or Path(__file__).resolve().parents[1]
    revision = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=all"],
        capture_output=True,
        text=True,
        check=False,
    )
    if revision.returncode or status.returncode:
        raise ValueError("live execution requires a readable Git checkout")
    actual_revision = revision.stdout.strip()
    if actual_revision != expected_revision:
        raise ValueError("implementation revision does not match the live checkout")
    if status.stdout.strip():
        raise ValueError("live execution requires a clean Git checkout")
    return actual_revision


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="LuxPower critical energy-flow profile READ-ONLY validation"
    )
    parser.add_argument("--confirm-read-only", action="store_true")
    parser.add_argument(
        "--implementation-revision",
        type=_parse_implementation_revision,
    )
    parser.add_argument("--pv-strings", type=_parse_pv_strings, required=True)
    parser.add_argument(
        "--grid-topology",
        type=GridTopology,
        choices=tuple(GridTopology),
        required=True,
    )
    parser.add_argument(
        "--load-layout",
        type=LoadLayout,
        choices=tuple(LoadLayout),
        required=True,
    )
    parser.add_argument(
        "--capability-provenance",
        choices=("operator_configuration", "holding_register", "live_fc4_observation"),
        required=True,
    )
    parser.add_argument("--targets", type=_parse_targets, default=(5.0, 3.0, 2.0))
    parser.add_argument("--short-runs", type=int, default=2)
    parser.add_argument("--short-seconds", type=float, default=30)
    parser.add_argument("--burn-seconds", type=float, default=180)
    parser.add_argument("--forced-samples", type=int, default=5)
    parser.add_argument("--enable-recovery", action="store_true")
    parser.add_argument("--recovery-window-seconds", type=float, default=300)
    parser.add_argument("--recovery-window-attempts", type=int, default=2)
    parser.add_argument("--recovery-initial-cooldown", type=float, default=1)
    parser.add_argument("--recovery-repeated-cooldown", type=float, default=5)
    parser.add_argument("--output", type=Path)
    return parser


async def _async_main(arguments: argparse.Namespace) -> int:
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    implementation_revision = _verify_live_source_revision(
        arguments.implementation_revision
    )
    host, port, dongle, inverter = _load_private_target(os.environ)
    profile = EnergyFlowReadProfile(
        active_pv_strings=arguments.pv_strings,
        grid_topology=arguments.grid_topology,
        load_layout=arguments.load_layout,
    )
    recovery_policy = (
        RecoveryPolicy(
            max_reconnects_per_acquisition=1,
            max_reconnects_per_window=arguments.recovery_window_attempts,
            rolling_window_seconds=arguments.recovery_window_seconds,
            initial_cooldown_seconds=arguments.recovery_initial_cooldown,
            repeated_cooldown_seconds=arguments.recovery_repeated_cooldown,
        )
        if arguments.enable_recovery
        else None
    )
    client = LuxPowerHybridReadClient(
        host,
        dongle,
        inverter,
        port=port,
        profile=profile,
        recovery_policy=recovery_policy,
    )
    report = await execute_profile_validation(
        client,
        targets=arguments.targets,
        short_runs=arguments.short_runs,
        short_seconds=arguments.short_seconds,
        burn_seconds=arguments.burn_seconds,
        forced_samples=arguments.forced_samples,
        implementation_revision=implementation_revision,
        implementation_revision_verified=True,
    )
    report["profile"]["capability_provenance"] = arguments.capability_provenance
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if arguments.output:
        _write_private_report(arguments.output, serialized)
    print("LuxPower energy-flow READ-ONLY validation completed", file=sys.stderr)
    print(serialized)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_argument_parser().parse_args(argv)
    try:
        return asyncio.run(_async_main(arguments))
    except ValueError as exc:
        build_argument_parser().error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
