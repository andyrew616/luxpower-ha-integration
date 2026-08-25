"""Tests for read-only critical-profile live validation helpers."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from luxpower.profile_validation import (
    _load_private_target,
    _nearest_rank_p95,
    _time_beyond_target,
    summarize_profile_samples,
)


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
        "max_worst_age_seconds": 4.0,
        "worst_register_sample_counts": {"0": 2, "170": 2},
    }


def test_time_beyond_target_uses_actual_sample_intervals():
    samples = [
        {
            "at": "2026-08-25T12:00:00+00:00",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 5.1
            },
        },
        {
            "at": "2026-08-25T12:00:00.125000+00:00",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 4.0
            },
        },
        {
            "at": "2026-08-25T12:00:00.240000+00:00",
            "profile_freshness": {
                "known": 1, "required": 1, "max_age_seconds": 4.1
            },
        },
    ]

    assert _time_beyond_target(samples, 5.0) == 0.125


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
