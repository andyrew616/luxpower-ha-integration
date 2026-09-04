"""Tests for read-only critical-profile live validation helpers."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from custom_components.lxp_modbus.classes.read_session import LuxReadSessionMetrics
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
)
from custom_components.lxp_modbus.recovery import (
    AcquisitionHealth,
    RecoveryEvent,
    RecoveryFailureKind,
    RecoveryMetrics,
)
from custom_components.lxp_modbus.observation import utc_now
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxReadDiagnosticJournal,
    LuxReadPurpose,
    LuxReadRequestContext,
    LuxReadRequestOutcome,
)
from luxpower.hybrid import HybridProfileMetrics

from luxpower.profile_validation import (
    _diagnostic_delta,
    _load_private_target,
    _nearest_rank_p95,
    _nearest_rank_p99,
    _recovery_metrics_delta,
    _time_beyond_target,
    _time_beyond_target_by_health_state,
    _time_beyond_target_by_recovery_episode,
    _time_by_health_state,
    _validate_deadline_options,
    _run_profile_phase,
    _verify_live_source_revision,
    _violation_episode_summary,
    _write_private_report,
    aggregate_qualification_reports,
    execute_profile_validation,
    StreamingProfileFreshnessAggregator,
    summarize_profile_samples,
)
from luxpower.hybrid import _latency_summary


def _recovery_event(index: int) -> RecoveryEvent:
    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(
        seconds=index
    )
    return RecoveryEvent(
        failure_kind=RecoveryFailureKind.REQUEST_TIMEOUT,
        episode_started_at=started_at.isoformat(),
        ended_at=(started_at + timedelta(milliseconds=500)).isoformat(),
        failed_register_start=0,
        failed_register_count=40,
        cooldown_seconds=1.0,
        reconnect_succeeded=True,
        failure_to_connection_seconds=0.1,
        failure_to_profile_recovery_seconds=0.2,
        maximum_profile_age_seconds=20.0,
        outcome="profile_recovered",
    )


def test_recovery_delta_reports_truthful_totals_after_event_rollover():
    before = RecoveryMetrics(
        health=AcquisitionHealth.HEALTHY,
        timeout_count=10,
        connection_loss_count=0,
        connection_establishment_failure_count=0,
        ambiguous_request_count=0,
        reconnect_attempts=10,
        successful_reconnects=10,
        failed_reconnects=0,
        completed_recoveries=10,
        retry_budget_exhausted=0,
        acquisitions_abandoned=0,
        connection_generations_created=11,
        events=tuple(_recovery_event(index) for index in range(10)),
        recovery_event_capacity=512,
        recovery_events_recorded=10,
    )
    after = replace(
        before,
        timeout_count=610,
        reconnect_attempts=610,
        successful_reconnects=610,
        completed_recoveries=610,
        connection_generations_created=611,
        events=tuple(_recovery_event(index) for index in range(98, 610)),
        recovery_events_recorded=610,
        recovery_events_dropped=98,
    )

    delta = _recovery_metrics_delta(before, after)

    assert delta["timeout_count"] == 600
    assert delta["completed_recoveries"] == 600
    assert delta["event_retention"] == {
        "capacity": 512,
        "recorded": 600,
        "retained": 512,
        "unretained": 88,
        "history_complete": False,
        "lifetime_dropped": 98,
    }
    assert len(delta["events"]) == 512
    assert delta["events"][0]["episode_started_at"] == _recovery_event(
        98
    ).episode_started_at
    assert delta["events"][-1]["episode_started_at"] == _recovery_event(
        609
    ).episode_started_at


def test_nearest_rank_statistics_and_profile_freshness_summary():
    samples = [
        {
            "profile_freshness": {
                "known": 2,
                "required": 2,
                "max_age_seconds": age,
                "worst_register": register,
            }
        }
        for age, register in ((1.0, 0), (2.0, 170), (3.0, 170), (4.0, 0))
    ]

    assert _nearest_rank_p95([1, 2, 3, 4]) == 4
    assert summarize_profile_samples(samples) == {
        "samples": 4,
        "complete_samples": 4,
        "median_worst_age_seconds": 2.5,
        "p95_worst_age_seconds": 4.0,
        "p99_worst_age_seconds": None,
        "max_worst_age_seconds": 4.0,
        "worst_register_sample_counts": {"0": 2, "170": 2},
    }

    assert _nearest_rank_p99(list(range(100))) == 98


def test_time_beyond_target_uses_actual_sample_intervals():
    samples = [
        {
            "at": "2026-08-25T12:00:00+00:00",
            "acquisition_health": "recovering",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 5.1
            },
        },
        {
            "at": "2026-08-25T12:00:00.125000+00:00",
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 4.0
            },
        },
        {
            "at": "2026-08-25T12:00:00.240000+00:00",
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 4.1
            },
        },
    ]

    assert _time_beyond_target(samples, 5.0) == 0.125
    assert _time_beyond_target_by_health_state(samples, 5.0) == {
        "while_recovering": 0.125,
        "outside_recovering": 0.0,
    }
    assert _time_by_health_state(samples) == {
        "healthy": 0.115,
        "recovering": 0.125,
        "degraded": 0.0,
    }
    events = [{
        "episode_started_at": "2026-08-25T11:59:59.900000+00:00",
        "ended_at": "2026-08-25T12:00:00.200000+00:00",
    }]
    assert _time_beyond_target_by_recovery_episode(samples, 5.0, events) == {
        "recovery_episode": 0.125,
        "normal_operation": 0.0,
    }


def test_streaming_freshness_matches_exact_helpers_before_capacity():
    samples = [
        {
            "at": "2026-08-25T12:00:00+00:00",
            "acquisition_health": "recovering",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": 5.1,
                "max_age_seconds_raw": 5.100001,
                "worst_register": 114,
            },
        },
        {
            "at": "2026-08-25T12:00:00.125000+00:00",
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": 4.0,
                "max_age_seconds_raw": 4.0,
                "worst_register": 0,
            },
        },
        {
            "at": "2026-08-25T12:00:00.240000+00:00",
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": 4.1,
                "max_age_seconds_raw": 4.1,
                "worst_register": 0,
            },
        },
    ]
    recovery_events = [{
        "episode_started_at": "2026-08-25T11:59:59.900000+00:00",
        "ended_at": "2026-08-25T12:00:00.200000+00:00",
        "failure_kind": RecoveryFailureKind.REQUEST_TIMEOUT,
    }]
    accumulator = StreamingProfileFreshnessAggregator(5.0, quantile_capacity=8)
    for sample in samples:
        accumulator.append(sample)

    result = accumulator.finalize(recovery_events)

    assert "quantile_estimation" in result["freshness"]
    assert {
        key: value
        for key, value in result["freshness"].items()
        if key != "quantile_estimation"
    } == summarize_profile_samples(samples)
    assert result["freshness"]["quantile_estimation"]["exact"] is True
    assert result["stale_threshold_violations"] == 1
    assert result["sampled_time_beyond_target_seconds"] == 0.125
    assert result["sampled_time_beyond_target_by_health_state_seconds"] == {
        "while_recovering": 0.125,
        "outside_recovering": 0.0,
    }
    assert result["sampled_time_by_health_state_seconds"] == {
        "healthy": 0.115,
        "recovering": 0.125,
        "degraded": 0.0,
    }
    assert result["sampled_time_beyond_target_attribution_seconds"] == {
        "recovery_episode": 0.125,
        "normal_operation": 0.0,
        "method": "continuous_interval_overlap",
        "complete": True,
    }
    assert result["bounded_retention"]["raw_samples_retained"] == 0
    assert result["evidence_complete"] is True


def test_streaming_freshness_quantile_retention_is_bounded():
    accumulator = StreamingProfileFreshnessAggregator(
        2000.0, quantile_capacity=8, reservoir_seed=17
    )
    started = utc_now()
    for index in range(1000):
        accumulator.append({
            "at": (started + timedelta(milliseconds=100 * index)).isoformat(),
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": float(index),
                "max_age_seconds_raw": float(index),
                "worst_register": 114,
            },
        })

    result = accumulator.finalize([])
    quantiles = result["freshness"]["quantile_estimation"]

    assert result["freshness"]["samples"] == 1000
    assert result["freshness"]["max_worst_age_seconds"] == 999.0
    assert quantiles["samples_seen"] == 1000
    assert quantiles["samples_retained"] == 8
    assert quantiles["exact"] is False
    assert result["bounded_retention"]["raw_samples_retained"] == 0


def test_streaming_freshness_reports_bounded_episode_truncation():
    accumulator = StreamingProfileFreshnessAggregator(
        5.0, quantile_capacity=8, episode_capacity=2
    )
    started = utc_now()
    for index, age in enumerate((6.0, 1.0, 6.0, 1.0, 6.0, 1.0, 1.0)):
        accumulator.append({
            "at": (started + timedelta(seconds=index)).isoformat(),
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": age,
                "worst_register": 114,
            },
        })

    result = accumulator.finalize([{
        "episode_started_at": started.isoformat(),
        "ended_at": (started + timedelta(seconds=7)).isoformat(),
        "failure_kind": RecoveryFailureKind.REQUEST_TIMEOUT,
    }])

    assert result["violation_episodes"]["count"] == 3
    assert result["violation_episodes"]["retained_count"] == 2
    assert result["violation_episodes"]["episodes_dropped"] == 1
    assert result["sampled_time_beyond_target_attribution_seconds"] == {
        "recovery_episode": None,
        "normal_operation": None,
        "method": "continuous_interval_overlap",
        "complete": False,
    }
    assert result["evidence_complete"] is False


def test_streaming_terminal_stale_transition_is_right_censored():
    accumulator = StreamingProfileFreshnessAggregator(5.0, quantile_capacity=8)
    started = utc_now()
    for index, age in enumerate((1.0, 6.0)):
        accumulator.append({
            "at": (started + timedelta(seconds=index)).isoformat(),
            "monotonic_seconds": 10.0 + index,
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": age,
                "worst_register": 114,
            },
        })

    result = accumulator.finalize([])
    episodes = result["violation_episodes"]

    assert result["stale_threshold_violations"] == 1
    assert episodes["count"] == 1
    assert episodes["ended_stale"] is True
    assert episodes["right_censored_episode_count"] == 1
    assert episodes["episodes"][0]["duration_seconds"] == 0.0
    assert episodes["episodes"][0]["right_censored"] is True


def test_streaming_single_stale_sample_is_right_censored_without_duration():
    accumulator = StreamingProfileFreshnessAggregator(5.0, quantile_capacity=8)
    accumulator.append({
        "at": utc_now().isoformat(),
        "monotonic_seconds": 10.0,
        "acquisition_health": "degraded",
        "profile_freshness": {
            "known": 0,
            "required": 1,
            "max_age_seconds": None,
            "worst_register": None,
        },
    })

    result = accumulator.finalize([])
    episodes = result["violation_episodes"]

    assert result["stale_threshold_violations"] == 1
    assert result["sampled_time_beyond_target_seconds"] == 0.0
    assert episodes["count"] == 1
    assert episodes["ended_stale"] is True
    assert episodes["episodes"][0]["right_censored"] is True


def test_streaming_dropped_terminal_episode_keeps_run_end_truthful():
    accumulator = StreamingProfileFreshnessAggregator(
        5.0, quantile_capacity=8, episode_capacity=1
    )
    started = utc_now()
    for index, age in enumerate((6.0, 1.0, 6.0)):
        accumulator.append({
            "at": (started + timedelta(seconds=index)).isoformat(),
            "monotonic_seconds": 10.0 + index,
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": age,
                "worst_register": 114,
            },
        })

    result = accumulator.finalize([])
    episodes = result["violation_episodes"]

    assert episodes["count"] == 2
    assert episodes["retained_count"] == 1
    assert episodes["episodes_dropped"] == 1
    assert episodes["ended_stale"] is True
    assert episodes["right_censored_episode_count"] == 1
    assert not any(episode["right_censored"] for episode in episodes["episodes"])
    assert result["sampled_time_beyond_target_attribution_seconds"][
        "complete"
    ] is False
    assert result["evidence_complete"] is False


def test_streaming_freshness_durations_use_monotonic_not_wall_clock():
    accumulator = StreamingProfileFreshnessAggregator(5.0, quantile_capacity=8)
    for at, monotonic in (
        ("2026-08-25T12:00:01+00:00", 10.0),
        ("2026-08-25T12:00:00+00:00", 10.1),
    ):
        accumulator.append({
            "at": at,
            "monotonic_seconds": monotonic,
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": 6.0,
                "worst_register": 114,
            },
        })

    result = accumulator.finalize([])

    assert result["sampled_time_beyond_target_seconds"] == 0.1
    assert result["bounded_retention"]["duration_clock"] == "monotonic"
    assert result["bounded_retention"]["clock_regressions"] == 0
    assert result["evidence_complete"] is True


def test_strict_ten_second_episodes_include_recovery_and_boundary_maximum():
    samples = [
        {
            "at": f"2026-08-25T12:00:0{index}+00:00",
            "acquisition_health": "recovering" if index in (2, 3) else "healthy",
            "profile_freshness": {
                "known": 2,
                "required": 2,
                "max_age_seconds": age,
                "worst_register": 114,
            },
        }
        for index, age in enumerate((9.9, 10.0, 10.1, 11.2, 9.0))
    ]
    events = [{
        "episode_started_at": "2026-08-25T12:00:01.500000+00:00",
        "ended_at": "2026-08-25T12:00:04+00:00",
        "failure_kind": RecoveryFailureKind.REQUEST_TIMEOUT,
    }]

    summary = summarize_profile_samples(samples)
    episodes = _violation_episode_summary(samples, 10.0, events)

    assert summary["max_worst_age_seconds"] == 11.2
    assert episodes["count"] == 1
    assert episodes["longest_duration_seconds"] == 2.0
    assert episodes["episodes"][0]["cause"] == ["request_timeout"]
    assert episodes["episodes"][0]["right_censored"] is False
    assert _time_beyond_target(samples, 10.0) == 2.0


def test_strict_sla_uses_unrounded_age_just_over_target():
    samples = [
        {
            "at": f"2026-08-25T12:00:00.{suffix}+00:00",
            "acquisition_health": "healthy",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": displayed,
                "max_age_seconds_raw": raw,
                "worst_register": 114,
            },
        }
        for suffix, displayed, raw in (
            ("000000", 10.0, 10.000001),
            ("100000", 9.999, 9.999499),
        )
    ]

    assert _time_beyond_target(samples, 10.0) == 0.1
    episodes = _violation_episode_summary(samples, 10.0, [])
    assert episodes["count"] == 1
    assert episodes["episodes"][0]["right_censored"] is False


def test_terminal_stale_episode_is_explicitly_right_censored():
    samples = [
        {
            "at": f"2026-08-25T12:00:00.{suffix}+00:00",
            "acquisition_health": "recovering",
            "profile_freshness": {
                "known": 1,
                "required": 1,
                "max_age_seconds": age,
                "worst_register": 114,
            },
        }
        for suffix, age in (("000000", 10.1), ("100000", 10.2))
    ]

    episodes = _violation_episode_summary(samples, 10.0, [])

    assert episodes["count"] == 1
    assert episodes["episodes"][0]["right_censored"] is True


def test_request_latency_distribution_and_timeout_thresholds():
    values = [500.0] * 98 + [1100.0, 2600.0]

    summary = _latency_summary(values, samples_total=100)

    assert summary["median"] == 500.0
    assert summary["p95"] == 500.0
    assert summary["p99"] == 1100.0
    assert summary["max"] == 2600.0
    assert summary["successful_max_margin_to_timeout_ms"] == 400.0
    assert summary["above_threshold_ms"]["1000"] == {
        "count": 2,
        "percent": 2.0,
    }
    assert summary["above_threshold_ms"]["2500"]["count"] == 1


def test_ten_second_reply_summary_uses_its_actual_deadline_and_decision_buckets():
    summary = _latency_summary(
        [700.0, 3000.0, 4000.0, 5000.0, 6000.0],
        samples_total=5,
        reply_timeout_seconds=10,
    )

    assert summary["reply_timeout_seconds"] == 10
    assert summary["successful_max_margin_to_timeout_ms"] == 4000.0
    assert summary["decision_buckets_ms"] == {
        "0-3000": 2,
        ">3000-5000": 2,
        ">5000-10000": 1,
        ">10000": 0,
        "beyond_reply_timeout": 0,
    }


def test_truncated_latency_history_suppresses_full_distribution_claims():
    summary = _latency_summary([700.0] * 4096, samples_total=5000)

    assert summary["samples"] == 4096
    assert summary["samples_total"] == 5000
    assert summary["truncated"] is True
    assert summary["median"] is None
    assert summary["p95"] is None
    assert summary["p99"] is None
    assert summary["max"] is None
    assert summary["above_threshold_ms"] is None
    assert summary["decision_buckets_ms"] is None
    assert summary["histogram_ms"] is None


def _qualification_report(*, timeout, reconnect, target_met, maximum, values):
    return {
        "schema_version": 3,
        "phases": [{
            "name": "sustained_burn_in",
            "target_seconds": 10.0,
            "actual_duration_seconds": 1800.0,
            "target_met": target_met,
            "session_metrics": {
                "explicit_requests": 100,
                "expected_fc4_responses": 100 - timeout,
                "unmatched_fc4_observations": 3,
                "request_timeouts": timeout,
                "connection_losses": 0,
                "invalid_frames": 0,
                "function_193_frames": 1,
                "observation_queue_drops": 0,
                "connection_failures": 0,
                "request_latency_ms": {
                    "values_ms": values,
                    "samples_total": len(values),
                },
            },
            "recovery_metrics": {
                "reconnect_attempts": reconnect,
                "successful_reconnects": reconnect,
                "failed_reconnects": 0,
                "retry_budget_exhausted": 0,
                "events": [],
            },
            "profile_source_metrics": {
                "explicit_requests_avoided_unsolicited": 4,
            },
            "freshness": {"max_worst_age_seconds": maximum},
            "stale_threshold_violations": 1 if not target_met else 0,
            "sampled_time_beyond_target_seconds": 0.2 if not target_met else 0,
            "violation_episodes": {
                "longest_duration_seconds": 0.2 if not target_met else 0,
            },
        }],
        "terminal_shutdown": {
            "intentional": True,
            "connection_generations_created": 1 + reconnect,
        },
    }


def test_multiple_run_aggregation_rates_and_terminal_shutdown():
    reports = [
        _qualification_report(
            timeout=1, reconnect=1, target_met=False, maximum=10.1,
            values=[700.0] * 99,
        ),
        _qualification_report(
            timeout=0, reconnect=0, target_met=True, maximum=8.0,
            values=[800.0] * 100,
        ),
    ]

    aggregate = aggregate_qualification_reports(reports)

    assert aggregate["sustained_runs"] == 2
    assert aggregate["total_runtime_hours"] == 1.0
    assert aggregate["session_totals"]["explicit_requests"] == 200
    assert aggregate["observed_rates"] == {
        "timeouts_per_explicit_request": 0.005,
        "timeouts_per_hour": 1.0,
        "reconnects_per_hour": 1.0,
    }
    assert aggregate["connection_generations"] == 3
    assert aggregate["freshness"]["strict_target_met"] is False
    assert aggregate["freshness"]["maximum_worst_age_seconds"] == 10.1
    assert aggregate["request_latency_ms"]["samples"] == 199
    serialized = json.dumps(aggregate)
    assert "192.0.2" not in serialized
    assert "PRIVATE" not in serialized


def test_schema_v5_aggregation_rejects_mixed_deadline_provenance():
    reports = [
        _qualification_report(
            timeout=0,
            reconnect=0,
            target_met=True,
            maximum=8.0,
            values=[700.0] * 100,
        )
        for _ in range(2)
    ]
    for report in reports:
        report["schema_version"] = 5
        report["profile"] = {
            "definition_version": 1,
            "name": "energy_flow",
            "grid_topology": "single_phase",
            "active_pv_strings": [1, 2, 3],
            "load_layout": "standard",
            "required_registers": [0, 114],
            "blocks": [{"start": 0, "count": 40}, {"start": 80, "count": 40}],
        }
        report["provenance"] = {
            "implementation_revision": "a" * 40,
            "profile_definition_version": 1,
            "drain_timeout_seconds": 3,
            "reply_timeout_seconds": 3,
            "split_request_deadlines": True,
        }

    aggregate = aggregate_qualification_reports(reports)
    assert aggregate["deadline_configuration"] == {
        "drain_timeout_seconds": 3,
        "reply_timeout_seconds": 3.0,
        "split_request_deadlines": True,
    }

    reports[1]["provenance"]["reply_timeout_seconds"] = 10
    with pytest.raises(ValueError, match="different revisions, profiles, or deadline"):
        aggregate_qualification_reports(reports)

    reports[1]["provenance"]["reply_timeout_seconds"] = 3
    reports[1]["profile"]["active_pv_strings"] = [1, 2]
    with pytest.raises(ValueError, match="different revisions, profiles, or deadline"):
        aggregate_qualification_reports(reports)


def test_schema_v6_aggregation_requires_matching_liveness_configuration():
    reports = [
        _qualification_report(
            timeout=0,
            reconnect=0,
            target_met=True,
            maximum=8.0,
            values=[700.0] * 100,
        )
        for _ in range(2)
    ]
    for report in reports:
        report["schema_version"] = 6
        report["phases"][0]["session_metrics"].update(
            {
                "tcp_keepalive_applied_connections": 1,
                "tcp_keepalive_idle_applied_connections": 1,
                "tcp_keepalive_configuration_failures": 0,
                "tcp_keepalive_configuration_unavailable": 0,
                "receive_inactivity_timeouts": 0,
            }
        )
        report["profile"] = {
            "definition_version": 1,
            "name": "energy_flow",
            "required_registers": [0, 114],
            "blocks": [{"start": 0, "count": 40}, {"start": 80, "count": 40}],
        }
        report["provenance"] = {
            "implementation_revision": "a" * 40,
            "profile_definition_version": 1,
            "drain_timeout_seconds": 3,
            "reply_timeout_seconds": 10,
            "split_request_deadlines": True,
            "tcp_keepalive_enabled": True,
            "tcp_keepalive_idle_seconds": 60,
            "receive_inactivity_timeout_seconds": 900.0,
        }

    aggregate = aggregate_qualification_reports(reports)
    assert aggregate["schema_version"] == 2
    assert aggregate["liveness_configuration"] == {
        "tcp_keepalive_enabled": True,
        "tcp_keepalive_idle_seconds": 60,
        "receive_inactivity_timeout_seconds": 900.0,
    }

    reports[1]["provenance"]["receive_inactivity_timeout_seconds"] = None
    with pytest.raises(ValueError, match="deadline/liveness configuration"):
        aggregate_qualification_reports(reports)

    reports[1]["provenance"]["receive_inactivity_timeout_seconds"] = 900.0
    del reports[1]["provenance"]["tcp_keepalive_enabled"]
    with pytest.raises(ValueError, match="missing required provenance"):
        aggregate_qualification_reports(reports)

    reports[1]["provenance"]["tcp_keepalive_enabled"] = True
    del reports[1]["phases"][0]["session_metrics"][
        "receive_inactivity_timeouts"
    ]
    with pytest.raises(ValueError, match="missing required liveness metrics"):
        aggregate_qualification_reports(reports)


def test_schema_v7_aggregation_requires_matching_recovery_policy():
    report = _qualification_report(
        timeout=0,
        reconnect=0,
        target_met=True,
        maximum=8.0,
        values=[700.0] * 100,
    )
    report["schema_version"] = 7
    report["phases"][0]["session_metrics"].update(
        {
            "tcp_keepalive_applied_connections": 1,
            "tcp_keepalive_idle_applied_connections": 1,
            "tcp_keepalive_configuration_failures": 0,
            "tcp_keepalive_configuration_unavailable": 0,
            "receive_inactivity_timeouts": 0,
        }
    )
    report["profile"] = {
        "definition_version": 1,
        "name": "energy_flow",
        "required_registers": [0, 114],
        "blocks": [{"start": 0, "count": 40}, {"start": 80, "count": 40}],
    }
    report["provenance"] = {
        "implementation_revision": "a" * 40,
        "profile_definition_version": 1,
        "drain_timeout_seconds": 3,
        "reply_timeout_seconds": 10,
        "split_request_deadlines": True,
        "tcp_keepalive_enabled": True,
        "tcp_keepalive_idle_seconds": 60,
        "receive_inactivity_timeout_seconds": 900.0,
    }
    report["recovery_policy"] = {
        "max_reconnects_per_acquisition": 1,
        "max_reconnects_per_window": 2,
        "max_connection_attempts_per_reconnect": 3,
        "rolling_window_seconds": 300.0,
        "initial_cooldown_seconds": 1.0,
        "repeated_cooldown_seconds": 5.0,
    }
    report["phases"][0]["sampled_time_by_health_state_seconds"] = {
        "healthy": 1790.0,
        "recovering": 10.0,
        "degraded": 0.0,
    }
    report["phases"][0]["violation_episodes"]["episodes"] = [
        {"right_censored": False}
    ]
    reports = [report, copy.deepcopy(report)]
    reports[1]["phases"][0]["violation_episodes"]["episodes"] = [
        {"right_censored": True}
    ]

    aggregate = aggregate_qualification_reports(reports)
    assert aggregate["source_report_schema_version"] == 7
    assert aggregate["schema_version"] == 3
    assert aggregate["liveness_configuration"] == {
        "tcp_keepalive_enabled": True,
        "tcp_keepalive_idle_seconds": 60,
        "receive_inactivity_timeout_seconds": 900.0,
    }
    assert aggregate["health_state_duration_seconds"] == {
        "healthy": 3580.0,
        "recovering": 20.0,
        "degraded": 0.0,
    }
    assert aggregate["freshness"]["right_censored_episode_count"] == 1
    assert aggregate["freshness"]["runs_ending_stale"] == 1

    reports[1]["recovery_policy"]["max_connection_attempts_per_reconnect"] = 2
    with pytest.raises(ValueError, match="different revisions, profiles"):
        aggregate_qualification_reports(reports)


@pytest.mark.parametrize("source_schema", (8, 9))
def test_schema_v8_plus_aggregation_requires_matching_streaming_provenance(
    source_schema,
):
    report = _qualification_report(
        timeout=0,
        reconnect=0,
        target_met=True,
        maximum=8.0,
        values=[700.0] * 100,
    )
    report["schema_version"] = source_schema
    report["phases"][0]["session_metrics"].update(
        {
            "tcp_keepalive_applied_connections": 1,
            "tcp_keepalive_idle_applied_connections": 1,
            "tcp_keepalive_configuration_failures": 0,
            "tcp_keepalive_configuration_unavailable": 0,
            "receive_inactivity_timeouts": 0,
        }
    )
    report["profile"] = {
        "definition_version": 1,
        "name": "energy_flow",
        "required_registers": [0, 114],
        "blocks": [{"start": 0, "count": 40}, {"start": 80, "count": 40}],
    }
    report["provenance"] = {
        "implementation_revision": "a" * 40,
        "profile_definition_version": 1,
        "drain_timeout_seconds": 3,
        "reply_timeout_seconds": 10,
        "split_request_deadlines": True,
        "tcp_keepalive_enabled": True,
        "tcp_keepalive_idle_seconds": 60,
        "receive_inactivity_timeout_seconds": 900.0,
        "freshness_aggregation": {
            "mode": "streaming_bounded",
            "quantile_method": (
                "deterministic_algorithm_r_reservoir_nearest_rank"
            ),
            "quantile_capacity": 16384,
            "violation_episode_capacity": 4096,
            "raw_samples_retained": 0,
            "duration_clock": "monotonic",
        },
    }
    report["recovery_policy"] = {
        "max_reconnects_per_acquisition": 1,
        "max_reconnects_per_window": 2,
        "max_connection_attempts_per_reconnect": 3,
        "rolling_window_seconds": 300.0,
        "initial_cooldown_seconds": 1.0,
        "repeated_cooldown_seconds": 5.0,
    }
    report["phases"][0]["freshness_evidence_complete"] = True
    if source_schema >= 9:
        report["phases"][0]["recovery_metrics"]["event_retention"] = {
            "capacity": 512,
            "recorded": 0,
            "retained": 0,
            "unretained": 0,
            "history_complete": True,
            "lifetime_dropped": 0,
        }
    report["phases"][0]["violation_episodes"].update(
        {"ended_stale": False, "right_censored_episode_count": 0}
    )
    reports = [report, copy.deepcopy(report)]

    aggregate = aggregate_qualification_reports(reports)

    assert aggregate["schema_version"] == 4
    assert aggregate["source_report_schema_version"] == source_schema
    assert aggregate["freshness"]["evidence_complete"] is True
    assert aggregate["freshness"]["episode_details_dropped"] == 0
    assert aggregate["recovery_event_retention"] == (
        {
            "recorded": 0,
            "retained": 0,
            "unretained": 0,
            "history_complete": True,
        }
        if source_schema >= 9
        else None
    )
    if source_schema >= 9:
        missing_retention = copy.deepcopy(reports)
        del missing_retention[0]["phases"][0]["recovery_metrics"][
            "event_retention"
        ]
        with pytest.raises(ValueError, match="missing recovery-event retention"):
            aggregate_qualification_reports(missing_retention)

    reports[1]["phases"][0]["violation_episodes"].update(
        {
            "ended_stale": True,
            "right_censored_episode_count": 1,
            "episodes": [],
        }
    )
    aggregate = aggregate_qualification_reports(reports)
    assert aggregate["freshness"]["right_censored_episode_count"] == 1
    assert aggregate["freshness"]["runs_ending_stale"] == 1

    reports[1]["provenance"]["freshness_aggregation"][
        "quantile_capacity"
    ] = 8192
    with pytest.raises(ValueError, match="different revisions, profiles"):
        aggregate_qualification_reports(reports)

    reports[1]["provenance"]["freshness_aggregation"][
        "quantile_capacity"
    ] = 16384
    del reports[1]["provenance"]["freshness_aggregation"]["duration_clock"]
    with pytest.raises(ValueError, match="missing freshness aggregation"):
        aggregate_qualification_reports(reports)


def test_live_qualification_requires_both_phase_deadlines():
    _validate_deadline_options(None, None)
    _validate_deadline_options(3, 10)

    with pytest.raises(ValueError, match="must be supplied together"):
        _validate_deadline_options(3, None)


def test_private_report_permissions(tmp_path):
    output = tmp_path / "qualification.json"

    _write_private_report(output, '{"safety": "read-only"}')

    assert output.read_text(encoding="utf-8") == '{"safety": "read-only"}\n'
    assert output.stat().st_mode & 0o777 == 0o600


def test_diagnostic_delta_reports_block_purpose_and_traffic_correlation():
    journal = LuxReadDiagnosticJournal(event_capacity=64, request_capacity=16)
    before = journal.snapshot()
    connection_opened = journal.now()

    normal = journal.begin_request(
        generation=1,
        register_start=0,
        register_count=40,
        timeout_seconds=3,
        context=LuxReadRequestContext(
            purpose=LuxReadPurpose.NORMAL_PROFILE,
            profile_worst_age_seconds=8,
            profile_health="healthy",
        ),
        connection_opened_monotonic=connection_opened,
        requests_previously_on_generation=0,
    )
    journal.observe_matched(normal, 0, 40)
    journal.finalize_request(normal, LuxReadRequestOutcome.SUCCESS)

    recovery = journal.begin_request(
        generation=2,
        register_start=80,
        register_count=40,
        timeout_seconds=3,
        context=LuxReadRequestContext(
            purpose=LuxReadPurpose.RECOVERY_REACQUISITION,
            profile_worst_age_seconds=12,
            profile_health="recovering",
        ),
        connection_opened_monotonic=journal.now(),
        requests_previously_on_generation=0,
    )
    journal.observe_unmatched(2, 0, 40, recovery)
    journal.observe_fc193(2, recovery)
    journal.mark_generation_invalidated(recovery)
    journal.finalize_request(recovery, LuxReadRequestOutcome.RESPONSE_TIMEOUT)

    delta = _diagnostic_delta(before, journal.snapshot())

    assert delta["request_history_complete"] is True
    assert delta["analysis"]["by_block"]["0-39"]["successes"] == 1
    assert delta["analysis"]["by_block"]["80-119"]["timeouts"] == 1
    assert (
        delta["analysis"]["by_purpose"]["recovery_reacquisition"]["timeouts"]
        == 1
    )
    traffic = delta["analysis"]["traffic_near_timeouts"]
    assert traffic["unmatched_fc4_while_pending"] == 1
    assert traffic["fc193_while_pending"] == 1
    assert traffic["invalid_frames_while_pending"] == 0
    assert delta["analysis"]["late_old_generation_response"]["detected"] is None
    assert len(delta["timeout_episodes"]) == 1


def _zero_session_metrics():
    return LuxReadSessionMetrics(
        connections=1,
        reconnects=0,
        bytes_received=0,
        frames_received=0,
        validated_fc4_frames=0,
        expected_fc4_responses=0,
        unmatched_fc4_observations=0,
        duplicate_fc4_frames=0,
        invalid_frames=0,
        function_193_frames=0,
        explicit_requests=0,
        request_timeouts=0,
        connection_losses=0,
        operational_registers_expected=0,
        operational_registers_unmatched=0,
        observation_queue_drops=0,
        request_latencies_ms=(),
        request_latency_samples_total=0,
        decoder_discarded_bytes=0,
        decoder_buffered_bytes=0,
    )


@pytest.mark.asyncio
async def test_delivery_drop_is_separate_from_transport_recovery_safety():
    class DeliveryDropClient:
        def __init__(self):
            self.dropped = False

        def metrics(self):
            return replace(
                _zero_session_metrics(),
                observation_queue_drops=int(self.dropped),
            )

        def profile_metrics(self):
            return HybridProfileMetrics(0, 0, 0)

        def recovery_metrics(self):
            return RecoveryMetrics(
                health=AcquisitionHealth.HEALTHY,
                timeout_count=0,
                connection_loss_count=0,
                connection_establishment_failure_count=0,
                ambiguous_request_count=0,
                reconnect_attempts=0,
                successful_reconnects=0,
                failed_reconnects=0,
                completed_recoveries=0,
                retry_budget_exhausted=0,
                acquisitions_abandoned=0,
                connection_generations_created=1,
            )

        async def async_run_profile(self, _duration, *, sample_sink):
            now = utc_now()
            for offset in (0.0, 0.1):
                sample_sink.append(
                    {
                        "at": (now + timedelta(seconds=offset)).isoformat(),
                        "acquisition_health": "healthy",
                        "profile_freshness": {
                            "known": 1,
                            "required": 1,
                            "median_age_seconds": 1.0,
                            "max_age_seconds": 1.0,
                            "max_age_seconds_raw": 1.0,
                            "worst_register": 0,
                        },
                    }
                )
            self.dropped = True

    phase = await _run_profile_phase(
        DeliveryDropClient(),
        name="delivery_drop",
        target_seconds=20.0,
        duration_seconds=0.1,
    )

    assert phase["transport_recovery_safe"] is True
    assert phase["observation_delivery_complete"] is False
    assert phase["freshness_target_met"] is True
    assert phase["target_met"] is False


@pytest.mark.asyncio
async def test_incomplete_recovery_event_history_fails_phase_qualification():
    class RolledRecoveryClient:
        def __init__(self):
            self.recovery_snapshots = 0

        def metrics(self):
            return _zero_session_metrics()

        def profile_metrics(self):
            return HybridProfileMetrics(0, 0, 0)

        def recovery_metrics(self):
            self.recovery_snapshots += 1
            if self.recovery_snapshots == 1:
                return RecoveryMetrics(
                    health=AcquisitionHealth.HEALTHY,
                    timeout_count=0,
                    connection_loss_count=0,
                    connection_establishment_failure_count=0,
                    ambiguous_request_count=0,
                    reconnect_attempts=0,
                    successful_reconnects=0,
                    failed_reconnects=0,
                    completed_recoveries=0,
                    retry_budget_exhausted=0,
                    acquisitions_abandoned=0,
                    connection_generations_created=1,
                    recovery_event_capacity=512,
                )
            return RecoveryMetrics(
                health=AcquisitionHealth.HEALTHY,
                timeout_count=513,
                connection_loss_count=0,
                connection_establishment_failure_count=0,
                ambiguous_request_count=0,
                reconnect_attempts=513,
                successful_reconnects=513,
                failed_reconnects=0,
                completed_recoveries=513,
                retry_budget_exhausted=0,
                acquisitions_abandoned=0,
                connection_generations_created=514,
                events=tuple(_recovery_event(index) for index in range(1, 513)),
                recovery_event_capacity=512,
                recovery_events_recorded=513,
                recovery_events_dropped=1,
            )

        async def async_run_profile(self, _duration, *, sample_sink):
            now = utc_now()
            for offset in (0.0, 0.1):
                sample_sink.append(
                    {
                        "at": (now + timedelta(seconds=offset)).isoformat(),
                        "acquisition_health": "healthy",
                        "profile_freshness": {
                            "known": 1,
                            "required": 1,
                            "median_age_seconds": 1.0,
                            "max_age_seconds": 1.0,
                            "max_age_seconds_raw": 1.0,
                            "worst_register": 0,
                        },
                    }
                )

    phase = await _run_profile_phase(
        RolledRecoveryClient(),
        name="rolled_recovery_history",
        target_seconds=20.0,
        duration_seconds=0.1,
    )

    assert phase["recovery_metrics"]["event_retention"] == {
        "capacity": 512,
        "recorded": 513,
        "retained": 512,
        "unretained": 1,
        "history_complete": False,
        "lifetime_dropped": 1,
    }
    assert phase["freshness_target_met"] is True
    assert phase["transport_recovery_safe"] is False
    assert phase["target_met"] is False


@pytest.mark.asyncio
async def test_short_only_schema_v5_provenance_and_intentional_shutdown_health():
    class QualificationClient:
        def __init__(self):
            self.profile = EnergyFlowReadProfile(
                frozenset({1, 2, 3}),
                GridTopology.SINGLE_PHASE,
                LoadLayout.STANDARD,
            )
            self.recovery_policy = None
            self.acquisition_health = AcquisitionHealth.DEGRADED
            self.last_profile_request_block = None
            self.target = None

        async def async_connect(self):
            return None

        async def async_close(self):
            self.acquisition_health = AcquisitionHealth.DEGRADED

        async def async_read_profile(self):
            self.acquisition_health = AcquisitionHealth.HEALTHY
            return SimpleNamespace(duration_ms=1000.0)

        async def async_run_profile(self, _duration, *, sample_sink):
            now = utc_now()
            for offset in (0, 0.1):
                sample_sink.append({
                    "at": (now + timedelta(seconds=offset)).isoformat(),
                    "acquisition_health": "healthy",
                    "profile_freshness": {
                        "known": len(self.profile.required_registers),
                        "required": len(self.profile.required_registers),
                        "median_age_seconds": 2.0,
                        "max_age_seconds": 2.0,
                        "worst_register": 0,
                    },
                })

        def set_freshness_target(self, target):
            self.target = target

        def metrics(self):
            return _zero_session_metrics()

        def profile_metrics(self):
            return HybridProfileMetrics(0, 0, 0)

        def recovery_metrics(self):
            return RecoveryMetrics(
                health=self.acquisition_health,
                timeout_count=0,
                connection_loss_count=0,
                connection_establishment_failure_count=0,
                ambiguous_request_count=0,
                reconnect_attempts=0,
                successful_reconnects=0,
                failed_reconnects=0,
                completed_recoveries=0,
                retry_budget_exhausted=0,
                acquisitions_abandoned=0,
                connection_generations_created=1,
            )

    client = QualificationClient()
    report = await execute_profile_validation(
        client,
        targets=(10.0,),
        short_runs=1,
        short_seconds=0.01,
        burn_seconds=0,
        forced_samples=1,
        implementation_revision="a" * 40,
    )

    assert report["schema_version"] == 9
    assert report["validation_version"] == "9.0"
    assert report["provenance"] == {
        "implementation_revision": "a" * 40,
        "revision_source": "operator_supplied",
        "profile_definition_version": 1,
        "diagnostic_schema_version": 4,
        "run_mode": "critical_profile_timeout_diagnostics",
        "request_timeout_seconds": 3,
        "drain_timeout_seconds": 3,
        "reply_timeout_seconds": 3,
        "split_request_deadlines": False,
        "tcp_keepalive_enabled": True,
        "tcp_keepalive_idle_seconds": 60,
        "receive_inactivity_timeout_seconds": 900.0,
        "freshness_aggregation": {
            "mode": "streaming_bounded",
            "quantile_method": (
                "deterministic_algorithm_r_reservoir_nearest_rank"
            ),
            "quantile_capacity": 16384,
            "violation_episode_capacity": 4096,
            "raw_samples_retained": 0,
            "duration_clock": "monotonic",
        },
    }
    assert [phase["name"] for phase in report["phases"]] == ["short_1"]
    assert report["phases"][0]["target_met"] is True
    assert report["phases"][0]["freshness_retention"] == {
        "raw_samples_retained": 0,
        "quantile_capacity": 16384,
        "violation_episode_capacity": 4096,
        "duration_clock": "monotonic_with_utc_fallback",
        "utc_fallback_intervals": 1,
        "clock_regressions": 0,
    }
    assert report["forced_profile_refresh"]["target_seconds"] == 10.0
    assert report["forced_profile_refresh"]["five_second_interval_consumed_percent"] is None
    assert report["forced_profile_refresh"]["request_diagnostics"]["available"] is False
    assert report["terminal_shutdown"] == {
        "intentional": True,
        "operational_health_before_shutdown": "healthy",
        "health_after_shutdown": "degraded",
        "connection_generations_created": 1,
    }


def test_private_target_loader_returns_values_without_serializing_them():
    private = {
        "LUXPOWER_HOST": "192.0.2.44",
        "LUXPOWER_PORT": "8000",
        "LUXPOWER_DONGLE_SERIAL": "PRIVATE001",
        "LUXPOWER_INVERTER_SERIAL": "PRIVATE002",
    }

    loaded = _load_private_target(private)
    assert loaded == ("192.0.2.44", 8000, "PRIVATE001", "PRIVATE002")
    public_report = {
        "safety": {"read_only": True, "permitted_function_codes": [4]},
        "profile": {"required_registers": [0, 170]},
    }
    serialized = json.dumps(public_report)
    assert not any(value in serialized for value in private.values())


def test_live_revision_requires_matching_clean_checkout(monkeypatch, tmp_path):
    expected = "a" * 40

    def clean_run(command, **_kwargs):
        output = expected + "\n" if "rev-parse" in command else ""
        return SimpleNamespace(returncode=0, stdout=output)

    monkeypatch.setattr(subprocess, "run", clean_run)
    assert (
        _verify_live_source_revision(expected, repository_root=tmp_path)
        == expected
    )

    def dirty_run(command, **_kwargs):
        output = expected + "\n" if "rev-parse" in command else " M source.py\n"
        return SimpleNamespace(returncode=0, stdout=output)

    monkeypatch.setattr(subprocess, "run", dirty_run)
    with pytest.raises(ValueError, match="clean Git checkout"):
        _verify_live_source_revision(expected, repository_root=tmp_path)


def test_profile_validation_cli_requires_explicit_read_only_confirmation(tmp_path):
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(Path(__file__).parents[1]),
            "LUXPOWER_HOST": "192.0.2.44",
            "LUXPOWER_DONGLE_SERIAL": "PRIVATE001",
            "LUXPOWER_INVERTER_SERIAL": "PRIVATE002",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "luxpower.profile_validation",
            "--pv-strings",
            "1,2,3",
            "--grid-topology",
            "single_phase",
            "--load-layout",
            "standard",
            "--capability-provenance",
            "operator_configuration",
        ],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "requires --confirm-read-only" in result.stderr
    assert "PRIVATE001" not in result.stdout + result.stderr
    assert "PRIVATE002" not in result.stdout + result.stderr


def test_profile_modules_import_without_home_assistant():
    code = """
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'homeassistant' or name.startswith('homeassistant.'):
        raise AssertionError(name)
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from luxpower import EnergyFlowReadProfile, GridTopology, LoadLayout
from luxpower.hybrid import LuxPowerHybridReadClient
profile = EnergyFlowReadProfile(
    frozenset({1, 2, 3}), GridTopology.SINGLE_PHASE, LoadLayout.STANDARD
)
assert len(profile.read_blocks) == 2
assert not any('write' in name for name in dir(LuxPowerHybridReadClient) if not name.startswith('_'))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).parents[1],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1])},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("missing", ["LUXPOWER_HOST", "LUXPOWER_DONGLE_SERIAL"])
def test_private_target_loader_rejects_missing_values(missing):
    values = {
        "LUXPOWER_HOST": "192.0.2.44",
        "LUXPOWER_DONGLE_SERIAL": "PRIVATE001",
        "LUXPOWER_INVERTER_SERIAL": "PRIVATE002",
    }
    del values[missing]

    with pytest.raises(ValueError, match=missing):
        _load_private_target(values)
