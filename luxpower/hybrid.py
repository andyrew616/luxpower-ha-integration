"""Experimental read-only hybrid telemetry over the frame-aware Lux session."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Mapping, Sequence

from custom_components.lxp_modbus.classes.read_session import (
    LuxReadSession,
    LuxReadSessionMetrics,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.const import TOTAL_REGISTERS
from custom_components.lxp_modbus.exceptions import LuxPowerCommunicationError
from custom_components.lxp_modbus.observation import require_aware_utc, utc_now
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    input_register_group,
    input_registers_for_group,
)

HYBRID_SCHEMA_VERSION = 1
HYBRID_VERSION = "1.0"
HARDWARE_READ_BLOCK_SIZE = 40


@dataclass(frozen=True)
class InputReadBlock:
    """One experimentally proven aligned FC4 input-register block."""

    start: int
    count: int

    @property
    def end(self) -> int:
        return self.start + self.count - 1

    def addresses(self) -> range:
        return range(self.start, self.end + 1)


OPERATIONAL_READ_BLOCKS = tuple(
    InputReadBlock(start, HARDWARE_READ_BLOCK_SIZE)
    for start in range(0, 240, HARDWARE_READ_BLOCK_SIZE)
)
FULL_INPUT_READ_BLOCKS = tuple(
    InputReadBlock(
        start,
        min(HARDWARE_READ_BLOCK_SIZE, TOTAL_REGISTERS - start),
    )
    for start in range(0, TOTAL_REGISTERS, HARDWARE_READ_BLOCK_SIZE)
)


def _validate_operational_blocks() -> None:
    covered = {
        register
        for block in OPERATIONAL_READ_BLOCKS
        for register in block.addresses()
    }
    missing = input_registers_for_group(TelemetryGroup.OPERATIONAL) - covered
    if missing:
        raise RuntimeError(f"operational blocks miss registers: {sorted(missing)}")


_validate_operational_blocks()


@dataclass(frozen=True)
class HybridRefreshResult:
    """Outcome of one stale-driven operational refresh pass."""

    requested_blocks: tuple[InputReadBlock, ...]
    fresh_blocks_skipped: tuple[InputReadBlock, ...]
    duration_ms: float


class LuxPowerHybridReadClient:
    """Experimental persistent FC4 client with freshness-driven read suppression.

    This API exposes no write operation and is not wired into Home Assistant.
    """

    def __init__(
        self,
        host: str,
        dongle_serial: str,
        inverter_serial: str,
        *,
        port: int = 8000,
        freshness_target: timedelta = timedelta(seconds=5),
        full_scan_interval: timedelta = timedelta(seconds=60),
        session: LuxReadSession | None = None,
    ) -> None:
        if freshness_target.total_seconds() <= 0:
            raise ValueError("freshness_target must be positive")
        if full_scan_interval.total_seconds() <= 0:
            raise ValueError("full_scan_interval must be positive")
        self._session = session or LuxReadSession(
            host,
            dongle_serial,
            inverter_serial,
            port=port,
        )
        self._freshness_target = freshness_target
        self._full_scan_interval = full_scan_interval
        self._last_full_scan_completed_at: datetime | None = None

    async def async_connect(self) -> None:
        await self._session.async_connect()

    async def async_close(self) -> None:
        await self._session.async_close()

    async def async_passive(self, seconds: float) -> None:
        """Receive and route frames without sending a request."""
        if seconds <= 0:
            raise ValueError("passive duration must be positive")
        await asyncio.sleep(seconds)

    def snapshot(self) -> LuxReadSessionSnapshot:
        return self._session.snapshot()

    def metrics(self) -> LuxReadSessionMetrics:
        return self._session.metrics()

    def drain_observations(self):
        """Return queued sanitized observation objects for measurement."""
        return self._session.drain_observations()

    def set_freshness_target(self, target: timedelta) -> None:
        """Change only the experimental stale threshold between bounded phases."""
        if target.total_seconds() <= 0:
            raise ValueError("freshness target must be positive")
        self._freshness_target = target

    async def async_refresh_operational(self) -> HybridRefreshResult:
        """Read only operational blocks not already sufficiently fresh."""
        started = time.monotonic()
        requested: list[InputReadBlock] = []
        skipped: list[InputReadBlock] = []
        for block in OPERATIONAL_READ_BLOCKS:
            # Re-snapshot before every block: an unsolicited frame routed while a
            # previous request was pending may have made this block fresh.
            if self._block_is_fresh(block, self._session.snapshot(), utc_now()):
                skipped.append(block)
                continue
            await self._session.async_read_input(block.start, block.count)
            requested.append(block)
        return HybridRefreshResult(
            requested_blocks=tuple(requested),
            fresh_blocks_skipped=tuple(skipped),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def async_read_operational(self) -> HybridRefreshResult:
        """Force one six-block routing validation independent of freshness."""
        started = time.monotonic()
        for block in OPERATIONAL_READ_BLOCKS:
            await self._session.async_read_input(block.start, block.count)
        return HybridRefreshResult(
            requested_blocks=OPERATIONAL_READ_BLOCKS,
            fresh_blocks_skipped=(),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    async def async_full_scan(self) -> LuxReadSessionSnapshot:
        """Explicitly retain the proven aligned 0-749 full-scan capability."""
        for block in FULL_INPUT_READ_BLOCKS:
            await self._session.async_read_input(block.start, block.count)
        self._last_full_scan_completed_at = utc_now()
        return self._session.snapshot()

    async def async_run_hybrid(
        self,
        duration: float,
        *,
        include_full_scan: bool = True,
        sample_interval: float = 0.1,
        sample_sink: list[dict] | None = None,
    ) -> list[dict]:
        """Run a bounded hybrid experiment with independent freshness samples."""
        if duration <= 0:
            raise ValueError("duration must be positive")
        if sample_interval <= 0:
            raise ValueError("sample_interval must be positive")
        deadline = time.monotonic() + duration
        samples = sample_sink if sample_sink is not None else []

        async def monitor_freshness() -> None:
            while time.monotonic() < deadline:
                now = utc_now()
                samples.append({
                    "at": now.isoformat(),
                    "operational_freshness": self._freshness_summary(
                        self._session.snapshot(), now
                    ),
                })
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                await asyncio.sleep(min(sample_interval, remaining))

        monitor = asyncio.create_task(monitor_freshness())
        try:
            while time.monotonic() < deadline:
                now = utc_now()
                full_due = bool(
                    include_full_scan
                    and (
                        self._last_full_scan_completed_at is None
                        or now - self._last_full_scan_completed_at
                        >= self._full_scan_interval
                    )
                )
                if full_due:
                    await self.async_full_scan()
                else:
                    await self.async_refresh_operational()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.25, remaining))
        finally:
            monitor.cancel()
            try:
                await monitor
            except asyncio.CancelledError:
                pass
        return samples

    def _block_is_fresh(
        self,
        block: InputReadBlock,
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> bool:
        threshold = self._freshness_target
        observations = snapshot.observed_at.input_registers
        return all(
            register in observations
            and now - observations[register] <= threshold
            for register in block.addresses()
            if input_register_group(register) is TelemetryGroup.OPERATIONAL
        )

    @staticmethod
    def _freshness_summary(
        snapshot: LuxReadSessionSnapshot,
        now: datetime,
    ) -> dict:
        operational = input_registers_for_group(TelemetryGroup.OPERATIONAL)
        ages = [
            (now - snapshot.observed_at.input_registers[register]).total_seconds()
            for register in operational
            if register in snapshot.observed_at.input_registers
        ]
        return {
            "known": len(ages),
            "required": len(operational),
            "median_age_seconds": round(statistics.median(ages), 3) if ages else None,
            "max_age_seconds": round(max(ages), 3) if ages else None,
        }


def _metrics_delta(
    before: LuxReadSessionMetrics, after: LuxReadSessionMetrics
) -> dict:
    fields = (
        "bytes_received",
        "frames_received",
        "validated_fc4_frames",
        "expected_fc4_responses",
        "unmatched_fc4_observations",
        "duplicate_fc4_frames",
        "invalid_frames",
        "function_193_frames",
        "explicit_requests",
        "request_timeouts",
        "connection_losses",
        "operational_registers_expected",
        "operational_registers_unmatched",
        "observation_queue_drops",
    )
    delta = {name: getattr(after, name) - getattr(before, name) for name in fields}
    new_latency_count = (
        after.request_latency_samples_total
        - before.request_latency_samples_total
    )
    latencies = (
        after.request_latencies_ms[-new_latency_count:]
        if new_latency_count else ()
    )
    delta["request_latency_ms"] = {
        "samples": len(latencies),
        "samples_total": new_latency_count,
        "truncated": new_latency_count > len(latencies),
        "mean": round(statistics.fmean(latencies), 3) if latencies else None,
        "median": round(statistics.median(latencies), 3) if latencies else None,
        "min": round(min(latencies), 3) if latencies else None,
        "max": round(max(latencies), 3) if latencies else None,
    }
    return delta


def _range_summary(snapshot: LuxReadSessionSnapshot) -> list[dict]:
    addresses = sorted(snapshot.observed_at.input_registers)
    if not addresses:
        return []
    ranges: list[dict] = []
    start = previous = addresses[0]
    for address in addresses[1:]:
        if address != previous + 1:
            ranges.append({"start": start, "end": previous, "count": previous - start + 1})
            start = address
        previous = address
    ranges.append({"start": start, "end": previous, "count": previous - start + 1})
    return ranges


def _observation_summary(observations) -> dict:
    """Summarize routed observations without serials, packets, or values."""
    ordered = sorted(observations, key=lambda item: item.observed_at)
    intervals = [
        (later.observed_at - earlier.observed_at).total_seconds()
        for earlier, later in zip(ordered, ordered[1:])
    ]
    return {
        "count": len(ordered),
        "explicit": sum(item.explicit_response for item in ordered),
        "unmatched": sum(not item.explicit_response for item in ordered),
        "duplicates": sum(item.duplicate for item in ordered),
        "ranges": [
            {
                "start": item.register_start,
                "count": item.register_count,
                "end": item.register_end,
                "explicit_response": item.explicit_response,
                "duplicate": item.duplicate,
            }
            for item in ordered
        ],
        "interval_seconds": {
            "samples": len(intervals),
            "median": round(statistics.median(intervals), 3) if intervals else None,
            "min": round(min(intervals), 3) if intervals else None,
            "max": round(max(intervals), 3) if intervals else None,
        },
    }


async def execute_live_validation(
    client: LuxPowerHybridReadClient,
    *,
    passive_seconds: float,
    hybrid_targets: Sequence[float],
    hybrid_seconds: float,
) -> dict:
    """Run bounded passive, explicit-routing, and progressive hybrid phases."""
    report = {
        "schema_version": HYBRID_SCHEMA_VERSION,
        "hybrid_version": HYBRID_VERSION,
        "started_at": utc_now().isoformat(),
        "safety": {
            "read_only": True,
            "permitted_function_codes": [4],
            "writes_exposed": False,
        },
        "configuration": {
            "passive_seconds": passive_seconds,
            "operational_blocks": [asdict(block) for block in OPERATIONAL_READ_BLOCKS],
            "hybrid_targets_seconds": list(hybrid_targets),
            "hybrid_seconds_per_target": hybrid_seconds,
        },
        "phases": [],
    }
    await client.async_connect()
    try:
        client.drain_observations()
        before = client.metrics()
        await client.async_passive(passive_seconds)
        after = client.metrics()
        passive_observations = client.drain_observations()
        report["phases"].append({
            "name": "passive",
            "metrics": _metrics_delta(before, after),
            "observed_ranges": _range_summary(client.snapshot()),
            "observations": _observation_summary(passive_observations),
        })

        client.drain_observations()
        before = client.metrics()
        explicit_started = time.monotonic()
        explicit = await client.async_read_operational()
        after = client.metrics()
        explicit_observations = client.drain_observations()
        explicit_phase = {
            "name": "explicit_operational",
            "duration_ms": round((time.monotonic() - explicit_started) * 1000, 3),
            "requested_blocks": [asdict(block) for block in explicit.requested_blocks],
            "fresh_blocks_skipped": [asdict(block) for block in explicit.fresh_blocks_skipped],
            "metrics": _metrics_delta(before, after),
            "observed_ranges": _range_summary(client.snapshot()),
            "observations": _observation_summary(explicit_observations),
        }
        report["phases"].append(explicit_phase)

        stable = not any(
            explicit_phase["metrics"][name]
            for name in ("request_timeouts", "connection_losses", "invalid_frames")
        )

        if stable:
            client.drain_observations()
            before = client.metrics()
            full_started = time.monotonic()
            full_error = None
            try:
                await client.async_full_scan()
            except LuxPowerCommunicationError as exc:
                full_error = type(exc).__name__
            full_duration = time.monotonic() - full_started
            after = client.metrics()
            full_phase = {
                "name": "frame_aware_full_scan",
                "duration_seconds": round(full_duration, 3),
                "requested_blocks": len(FULL_INPUT_READ_BLOCKS),
                "metrics": _metrics_delta(before, after),
                "observations": _observation_summary(client.drain_observations()),
                "observed_registers": len(client.snapshot().input_registers),
                "status": "failed" if full_error else "success",
                "error": full_error,
            }
            report["phases"].append(full_phase)
            stable = not any(
                full_phase["metrics"][name]
                for name in ("request_timeouts", "connection_losses", "invalid_frames")
            ) and full_phase["observed_registers"] == TOTAL_REGISTERS and not full_error

        for target in hybrid_targets:
            if not stable:
                break
            client.set_freshness_target(timedelta(seconds=target))
            client.drain_observations()
            before = client.metrics()
            phase_started = time.monotonic()
            samples: list[dict] = []
            phase_error = None
            try:
                await client.async_run_hybrid(
                    hybrid_seconds,
                    include_full_scan=False,
                    sample_sink=samples,
                )
            except LuxPowerCommunicationError as exc:
                phase_error = type(exc).__name__
            actual_duration = time.monotonic() - phase_started
            after = client.metrics()
            observations = client.drain_observations()
            delta = _metrics_delta(before, after)
            freshness = [sample["operational_freshness"] for sample in samples]
            phase = {
                "name": "hybrid",
                "target_seconds": target,
                "duration_seconds": hybrid_seconds,
                "actual_duration_seconds": round(actual_duration, 3),
                "fast_path_only": True,
                "full_scan_validated_separately": True,
                "status": "failed" if phase_error else "success",
                "error": phase_error,
                "metrics": delta,
                "samples": samples,
                "observations": _observation_summary(observations),
                "max_observed_operational_age_seconds": max(
                    (
                        item["max_age_seconds"]
                        for item in freshness
                        if item["max_age_seconds"] is not None
                    ),
                    default=None,
                ),
            }
            operational_observations = (
                delta["operational_registers_expected"]
                + delta["operational_registers_unmatched"]
            )
            phase["operational_receptions_by_route"] = {
                "explicit": delta["operational_registers_expected"],
                "unsolicited": delta["operational_registers_unmatched"],
                "unsolicited_reception_percent": (
                    round(
                        100 * delta["operational_registers_unmatched"]
                        / operational_observations,
                        3,
                    )
                    if operational_observations else None
                ),
            }
            phase["explicit_requests_per_minute"] = round(
                delta["explicit_requests"] * 60 / actual_duration, 3
            )
            ordered_max_ages = sorted(
                item["max_age_seconds"]
                for item in freshness
                if item["max_age_seconds"] is not None
            )
            p95_index = max(0, int(len(ordered_max_ages) * 0.95 + 0.999) - 1)
            phase["sampled_worst_register_age_seconds"] = {
                "samples": len(ordered_max_ages),
                "median": (
                    round(statistics.median(ordered_max_ages), 3)
                    if ordered_max_ages else None
                ),
                "p95": ordered_max_ages[p95_index] if ordered_max_ages else None,
                "max": max(ordered_max_ages) if ordered_max_ages else None,
                "sampling_interval_seconds": 0.1,
            }
            phase["target_met"] = bool(
                freshness
                and phase_error is None
                and all(item["known"] == item["required"] for item in freshness)
                and phase["max_observed_operational_age_seconds"] is not None
                and phase["max_observed_operational_age_seconds"] <= target
            )
            report["phases"].append(phase)
            stable = not any(
                delta[name]
                for name in ("request_timeouts", "connection_losses", "invalid_frames")
            ) and phase["target_met"] and phase_error is None
    finally:
        await client.async_close()
    report["final_metrics"] = asdict(client.metrics())
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
    if not targets or any(item <= 0 for item in targets):
        raise argparse.ArgumentTypeError("targets must be positive")
    if tuple(sorted(targets, reverse=True)) != targets:
        raise argparse.ArgumentTypeError("targets must run slowest to fastest")
    return targets


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Experimental LuxPower frame-aware READ-ONLY validation"
    )
    parser.add_argument("--confirm-read-only", action="store_true")
    parser.add_argument("--passive-seconds", type=float, default=30)
    parser.add_argument("--hybrid-targets", type=_parse_targets, default=(5.0, 3.0, 2.0))
    parser.add_argument("--hybrid-seconds", type=float, default=60)
    parser.add_argument("--output", type=Path)
    return parser


async def _async_main(arguments: argparse.Namespace) -> int:
    if not arguments.confirm_read_only:
        raise ValueError("live execution requires --confirm-read-only")
    if arguments.passive_seconds <= 0 or arguments.hybrid_seconds <= 0:
        raise ValueError("phase durations must be positive")
    host, port, dongle, inverter = _load_private_target(os.environ)
    client = LuxPowerHybridReadClient(host, dongle, inverter, port=port)
    report = await execute_live_validation(
        client,
        passive_seconds=arguments.passive_seconds,
        hybrid_targets=arguments.hybrid_targets,
        hybrid_seconds=arguments.hybrid_seconds,
    )
    serialized = json.dumps(report, indent=2, sort_keys=True)
    if arguments.output:
        arguments.output.write_text(serialized + "\n", encoding="utf-8")
    print("LuxPower frame-aware READ-ONLY validation completed", file=sys.stderr)
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
