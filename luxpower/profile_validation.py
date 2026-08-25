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
import statistics
import sys
import time
from typing import Mapping, Sequence

from custom_components.lxp_modbus.exceptions import LuxPowerCommunicationError
from custom_components.lxp_modbus.observation import utc_now
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    profile_block_details,
)
from luxpower.hybrid import LuxPowerHybridReadClient, _metrics_delta

PROFILE_VALIDATION_SCHEMA_VERSION = 1
PROFILE_VALIDATION_VERSION = "1.0"


def _nearest_rank_p95(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def summarize_profile_samples(samples: Sequence[Mapping[str, object]]) -> dict:
    """Summarize independently sampled worst-required-register freshness."""
    freshness = [sample["profile_freshness"] for sample in samples]
    complete = [
        item
        for item in freshness
        if item["known"] == item["required"] and item["max_age_seconds"] is not None
    ]
    ages = [float(item["max_age_seconds"]) for item in complete]
    outliers: dict[str, int] = {}
    for item in complete:
        register = str(item["worst_register"])
        outliers[register] = outliers.get(register, 0) + 1
    return {
        "samples": len(samples),
        "complete_samples": len(complete),
        "median_worst_age_seconds": round(statistics.median(ages), 3) if ages else None,
        "p95_worst_age_seconds": round(_nearest_rank_p95(ages), 3) if ages else None,
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
            freshness["max_age_seconds"] is None
            or freshness["known"] != freshness["required"]
            or freshness["max_age_seconds"] > target_seconds
        )
        if stale:
            total += (
                datetime.fromisoformat(following["at"])
                - datetime.fromisoformat(current["at"])
            ).total_seconds()
    return total


async def _run_profile_phase(
    client: LuxPowerHybridReadClient,
    *,
    name: str,
    target_seconds: float,
    duration_seconds: float,
) -> dict:
    before_session = client.metrics()
    before_profile = client.profile_metrics()
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
        item["profile_freshness"]["max_age_seconds"] is None
        or item["profile_freshness"]["known"]
        != item["profile_freshness"]["required"]
        or item["profile_freshness"]["max_age_seconds"] > target_seconds
        for item in samples
    )
    unsafe_events = sum(
        session_delta[field]
        for field in (
            "request_timeouts",
            "connection_losses",
            "invalid_frames",
            "observation_queue_drops",
        )
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
        "target_met": bool(samples and not error and not unsafe_events and not violations),
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
) -> dict:
    """Run progressive profile validation and stop at the first failed gate."""
    if not targets or tuple(sorted(targets, reverse=True)) != tuple(targets):
        raise ValueError("targets must run slowest to fastest")
    if min(targets) <= 0 or min(short_runs, forced_samples) <= 0:
        raise ValueError("targets and sample counts must be positive")
    if min(short_seconds, burn_seconds, safety_margin_seconds) <= 0:
        raise ValueError("durations and safety margin must be positive")
    if client.profile is None:
        raise ValueError("profile validation requires a configured profile")

    report = {
        "schema_version": PROFILE_VALIDATION_SCHEMA_VERSION,
        "validation_version": PROFILE_VALIDATION_VERSION,
        "started_at": utc_now().isoformat(),
        "safety": {
            "read_only": True,
            "permitted_function_codes": [4],
            "writes_exposed": False,
        },
        "profile": {
            "name": client.profile.name.value,
            "grid_topology": client.profile.grid_topology.value,
            "active_pv_strings": sorted(client.profile.active_pv_strings),
            "load_layout": client.profile.load_layout.value,
            "required_registers": sorted(client.profile.required_registers),
            "blocks": list(profile_block_details(client.profile)),
        },
        "phases": [],
    }
    await client.async_connect()
    stable = True
    try:
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
            "five_second_interval_consumed_percent": round(
                100 * statistics.median(forced_durations) / targets[0], 3
            ),
            "five_second_p95_timing_headroom_seconds": round(
                targets[0] - forced_p95, 3
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
            target_phases = []
            for index in range(short_runs):
                phase = await _run_profile_phase(
                    client,
                    name=f"short_{index + 1}",
                    target_seconds=target,
                    duration_seconds=short_seconds,
                )
                phase["request_trigger_age_seconds"] = round(trigger, 3)
                report["phases"].append(phase)
                target_phases.append(phase)
                if not phase["target_met"]:
                    stable = False
                    break
            if not stable:
                break
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
        await client.async_close()
    report["final_session_metrics"] = asdict(client.metrics())
    report["final_profile_metrics"] = asdict(client.profile_metrics())
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


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="LuxPower critical energy-flow profile READ-ONLY validation"
    )
    parser.add_argument("--confirm-read-only", action="store_true")
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
    parser.add_argument("--output", type=Path)
    return parser


async def _async_main(arguments: argparse.Namespace) -> int:
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    host, port, dongle, inverter = _load_private_target(os.environ)
    profile = EnergyFlowReadProfile(
        active_pv_strings=arguments.pv_strings,
        grid_topology=arguments.grid_topology,
        load_layout=arguments.load_layout,
    )
    client = LuxPowerHybridReadClient(
        host, dongle, inverter, port=port, profile=profile
    )
    report = await execute_profile_validation(
        client,
        targets=arguments.targets,
        short_runs=arguments.short_runs,
        short_seconds=arguments.short_seconds,
        burn_seconds=arguments.burn_seconds,
    )
    report["profile"]["capability_provenance"] = arguments.capability_provenance
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if arguments.output:
        arguments.output.write_text(serialized + "\n", encoding="utf-8")
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
