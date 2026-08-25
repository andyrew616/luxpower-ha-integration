"""Tests for read-only critical-profile live validation helpers."""

import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from datetime import timedelta

import pytest

from custom_components.lxp_modbus.classes.read_session import LuxReadSessionMetrics
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
)
from custom_components.lxp_modbus.recovery import (
    AcquisitionHealth,
    RecoveryFailureKind,
    RecoveryMetrics,
)
from custom_components.lxp_modbus.observation import utc_now
from luxpower.hybrid import HybridProfileMetrics

from luxpower.profile_validation import (
    _load_private_target,
    _nearest_rank_p95,
    _nearest_rank_p99,
    _time_beyond_target,
    _time_beyond_target_by_health_state,
    _time_beyond_target_by_recovery_episode,
    _violation_episode_summary,
    _write_private_report,
    aggregate_qualification_reports,
    execute_profile_validation,
    summarize_profile_samples,
)
from luxpower.hybrid import _latency_summary


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
    events = [{
        "episode_started_at": "2026-08-25T11:59:59.900000+00:00",
        "ended_at": "2026-08-25T12:00:00.200000+00:00",
    }]
    assert _time_beyond_target_by_recovery_episode(samples, 5.0, events) == {
        "recovery_episode": 0.125,
        "normal_operation": 0.0,
    }


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
    assert _violation_episode_summary(samples, 10.0, [])["count"] == 1


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


def test_private_report_permissions(tmp_path):
    output = tmp_path / "qualification.json"

    _write_private_report(output, '{"safety": "read-only"}')

    assert output.read_text(encoding="utf-8") == '{"safety": "read-only"}\n'
    assert output.stat().st_mode & 0o777 == 0o600


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
async def test_short_only_schema_v3_and_intentional_shutdown_health():
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
    )

    assert report["schema_version"] == 3
    assert report["validation_version"] == "3.1"
    assert [phase["name"] for phase in report["phases"]] == ["short_1"]
    assert report["phases"][0]["target_met"] is True
    assert report["forced_profile_refresh"]["target_seconds"] == 10.0
    assert report["forced_profile_refresh"]["five_second_interval_consumed_percent"] is None
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
