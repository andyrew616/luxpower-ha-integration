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
from custom_components.lxp_modbus.recovery import RecoveryPolicy
from luxpower.hybrid import LuxPowerHybridReadClient, _metrics_delta

PROFILE_VALIDATION_SCHEMA_VERSION = 3
PROFILE_VALIDATION_VERSION = "3.0"


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


def _time_beyond_target_by_health_state(
    samples: Sequence[Mapping[str, object]], target_seconds: float
) -> dict[str, float]:
    """Partition stale time by instantaneous state without claiming causality."""
    totals = {"while_recovering": 0.0, "outside_recovering": 0.0}
    for current, following in zip(samples, samples[1:]):
        freshness = current["profile_freshness"]
        stale = bool(
            freshness["max_age_seconds"] is None
            or freshness["known"] != freshness["required"]
            or freshness["max_age_seconds"] > target_seconds
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
            freshness["max_age_seconds"] is None
            or freshness["known"] != freshness["required"]
            or freshness["max_age_seconds"] > target_seconds
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
        "transport_recovery_safe": recovery_safe,
        "freshness_target_met": freshness_met,
        "target_met": bool(recovery_safe and freshness_met),
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
    report["final_recovery_metrics"] = asdict(client.recovery_metrics())
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
