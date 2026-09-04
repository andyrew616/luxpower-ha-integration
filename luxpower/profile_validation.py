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
import random
import re
import statistics
import subprocess
import sys
import time
from typing import Mapping, Sequence

from custom_components.lxp_modbus.classes.read_session import LuxReadSession
from custom_components.lxp_modbus.const import READ_TIMEOUT
from custom_components.lxp_modbus.observation import utc_now
from luxpower.qualified import (
    ENERGY_FLOW_PROFILE_DEFINITION_VERSION,
    EnergyFlowReadProfile,
    GridTopology,
    LoadLayout,
    LuxPowerCommunicationError,
    RecoveryPolicy,
    _QualificationLuxReadClient as LuxPowerHybridReadClient,
    profile_block_details,
)
from custom_components.lxp_modbus.timeout_diagnostics import (
    LuxReadDiagnosticJournal,
    LuxReadDiagnosticsSnapshot,
    LuxReadPurpose,
    LuxReadRequestDiagnostic,
    LuxReadRequestOutcome,
)
from luxpower.hybrid import _latency_summary, _metrics_delta

PROFILE_VALIDATION_SCHEMA_VERSION = 9
PROFILE_VALIDATION_VERSION = "9.0"
PROFILE_FRESHNESS_QUANTILE_CAPACITY = 16_384
PROFILE_VIOLATION_EPISODE_CAPACITY = 4_096
PROFILE_FRESHNESS_RESERVOIR_SEED = 0


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


class _BoundedReservoir:
    """Deterministic Algorithm-R reservoir for bounded distribution evidence."""

    def __init__(self, capacity: int, *, seed: int) -> None:
        if capacity <= 0:
            raise ValueError("reservoir capacity must be positive")
        self.capacity = capacity
        self._random = random.Random(seed)
        self._values: list[float] = []
        self.seen = 0

    def add(self, value: float) -> None:
        self.seen += 1
        if len(self._values) < self.capacity:
            self._values.append(float(value))
            return
        replacement = self._random.randrange(self.seen)
        if replacement < self.capacity:
            self._values[replacement] = float(value)

    @property
    def values(self) -> tuple[float, ...]:
        return tuple(self._values)

    @property
    def exact(self) -> bool:
        return self.seen <= self.capacity


class StreamingProfileFreshnessAggregator:
    """Bounded append-only freshness evidence with exact threshold accounting.

    Percentiles use a deterministic bounded reservoir once a phase exceeds the
    configured capacity. Maximum age, sample/violation counts, sampled stale
    duration, health duration, and retained violation episodes remain exact.
    """

    QUANTILE_METHOD = "deterministic_algorithm_r_reservoir_nearest_rank"

    def __init__(
        self,
        target_seconds: float,
        *,
        quantile_capacity: int = PROFILE_FRESHNESS_QUANTILE_CAPACITY,
        episode_capacity: int = PROFILE_VIOLATION_EPISODE_CAPACITY,
        reservoir_seed: int = PROFILE_FRESHNESS_RESERVOIR_SEED,
    ) -> None:
        if target_seconds <= 0:
            raise ValueError("freshness target must be positive")
        if episode_capacity <= 0:
            raise ValueError("episode capacity must be positive")
        self.target_seconds = float(target_seconds)
        self.quantile_capacity = quantile_capacity
        self.episode_capacity = episode_capacity
        self.reservoir_seed = reservoir_seed
        self._ages = _BoundedReservoir(quantile_capacity, seed=reservoir_seed)
        self._intervals = _BoundedReservoir(
            quantile_capacity, seed=reservoir_seed + 1
        )
        self._samples = 0
        self._complete_samples = 0
        self._violations = 0
        self._max_age: float | None = None
        self._worst_register_counts: dict[str, int] = {}
        self._health_seconds = {
            "healthy": 0.0,
            "recovering": 0.0,
            "degraded": 0.0,
        }
        self._stale_seconds = 0.0
        self._stale_by_health = {
            "while_recovering": 0.0,
            "outside_recovering": 0.0,
        }
        self._episodes: list[dict[str, object]] = []
        self._episode_count = 0
        self._episodes_dropped = 0
        self._longest_episode_seconds = 0.0
        self._maximum_interval_seconds: float | None = None
        self._active_episode: dict[str, object] | None = None
        self._last_sample: Mapping[str, object] | None = None
        self._last_at: datetime | None = None
        self._last_monotonic: float | None = None
        self._utc_fallback_intervals = 0
        self._clock_regressions = 0

    @staticmethod
    def _health(sample: Mapping[str, object]) -> str:
        health = str(sample.get("acquisition_health", "degraded"))
        return health if health in ("healthy", "recovering", "degraded") else "degraded"

    def _is_stale(self, sample: Mapping[str, object]) -> bool:
        freshness = sample["profile_freshness"]
        age = _maximum_age(freshness)
        return bool(
            age is None
            or freshness["known"] != freshness["required"]
            or age > self.target_seconds
        )

    def _retain_episode(self, episode: Mapping[str, object]) -> None:
        self._episode_count += 1
        duration = float(episode["duration_seconds"])
        self._longest_episode_seconds = max(
            self._longest_episode_seconds, duration
        )
        if len(self._episodes) < self.episode_capacity:
            self._episodes.append(dict(episode))
        else:
            self._episodes_dropped += 1

    def _close_active_episode(self) -> None:
        if self._active_episode is not None:
            self._retain_episode(self._active_episode)
            self._active_episode = None

    def _integrate_interval(
        self,
        current: Mapping[str, object],
        started: datetime,
        ended: datetime,
        duration: float,
    ) -> None:
        if duration < 0:
            self._clock_regressions += 1
            duration = 0.0
        self._intervals.add(duration)
        self._maximum_interval_seconds = (
            duration
            if self._maximum_interval_seconds is None
            else max(self._maximum_interval_seconds, duration)
        )
        health = self._health(current)
        self._health_seconds[health] += duration
        if not self._is_stale(current):
            self._close_active_episode()
            return

        self._stale_seconds += duration
        bucket = (
            "while_recovering" if health == "recovering" else "outside_recovering"
        )
        self._stale_by_health[bucket] += duration
        freshness = current["profile_freshness"]
        age = _maximum_age(freshness)
        if self._active_episode is None:
            self._active_episode = {
                "started_at": started,
                "ended_at": ended,
                "duration_seconds": 0.0,
                "maximum_age_seconds": float(age) if age is not None else None,
            }
        self._active_episode["ended_at"] = ended
        self._active_episode["duration_seconds"] = (
            float(self._active_episode["duration_seconds"]) + duration
        )
        if age is not None:
            previous = self._active_episode["maximum_age_seconds"]
            self._active_episode["maximum_age_seconds"] = max(
                float(previous) if previous is not None else float(age),
                float(age),
            )

    def append(self, sample: Mapping[str, object]) -> None:
        """Consume the same append-only sample contract used by the live monitor."""
        sampled_at = datetime.fromisoformat(str(sample["at"]))
        sampled_monotonic = sample.get("monotonic_seconds")
        monotonic_value = (
            float(sampled_monotonic) if sampled_monotonic is not None else None
        )
        if self._last_sample is not None and self._last_at is not None:
            if monotonic_value is not None and self._last_monotonic is not None:
                duration = monotonic_value - self._last_monotonic
            else:
                duration = (sampled_at - self._last_at).total_seconds()
                self._utc_fallback_intervals += 1
            self._integrate_interval(
                self._last_sample, self._last_at, sampled_at, duration
            )

        self._samples += 1
        freshness = sample["profile_freshness"]
        age = _maximum_age(freshness)
        complete = bool(
            freshness["known"] == freshness["required"] and age is not None
        )
        if complete:
            self._complete_samples += 1
            numeric_age = float(age)
            self._ages.add(numeric_age)
            self._max_age = (
                numeric_age
                if self._max_age is None
                else max(self._max_age, numeric_age)
            )
            register = str(freshness["worst_register"])
            self._worst_register_counts[register] = (
                self._worst_register_counts.get(register, 0) + 1
            )
        if self._is_stale(sample):
            self._violations += 1
        self._last_sample = sample
        self._last_at = sampled_at
        self._last_monotonic = monotonic_value

    @staticmethod
    def _recovery_ranges(
        recovery_events: Sequence[Mapping[str, object]],
    ) -> tuple[tuple[datetime, datetime, str], ...]:
        return tuple(
            (
                datetime.fromisoformat(str(event["episode_started_at"])),
                datetime.fromisoformat(str(event["ended_at"])),
                getattr(event["failure_kind"], "value", str(event["failure_kind"])),
            )
            for event in recovery_events
        )

    def _serialized_episodes(
        self,
        recovery_events: Sequence[Mapping[str, object]],
    ) -> tuple[list[dict], bool, int, int]:
        active = dict(self._active_episode) if self._active_episode is not None else None
        episodes = [dict(episode) for episode in self._episodes]
        effective_dropped = self._episodes_dropped
        active_retained = False
        final_stale = bool(
            self._last_sample is not None and self._is_stale(self._last_sample)
        )
        if active is None and final_stale and self._last_at is not None:
            freshness = self._last_sample["profile_freshness"]
            age = _maximum_age(freshness)
            active = {
                "started_at": self._last_at,
                "ended_at": self._last_at,
                "duration_seconds": 0.0,
                "maximum_age_seconds": float(age) if age is not None else None,
            }
        effective_episode_count = self._episode_count + int(active is not None)
        if active is not None:
            if len(episodes) < self.episode_capacity:
                episodes.append(active)
                active_retained = True
            else:
                effective_dropped += 1
        recovery_ranges = self._recovery_ranges(recovery_events)
        serialized = []
        for index, episode in enumerate(episodes):
            causes = sorted({
                failure_kind
                for recovery_start, recovery_end, failure_kind in recovery_ranges
                if recovery_start <= episode["ended_at"]
                and recovery_end >= episode["started_at"]
            })
            maximum_age = episode["maximum_age_seconds"]
            serialized.append({
                "started_at": episode["started_at"].isoformat(),
                "ended_at": episode["ended_at"].isoformat(),
                "duration_seconds": round(float(episode["duration_seconds"]), 3),
                "maximum_age_seconds": (
                    round(float(maximum_age), 3)
                    if maximum_age is not None
                    else None
                ),
                "cause": causes or ["undetermined_non_recovery"],
                "right_censored": bool(
                    final_stale
                    and active_retained
                    and index == len(episodes) - 1
                ),
            })
        return (
            serialized,
            final_stale,
            effective_dropped,
            effective_episode_count,
        )

    def finalize(
        self,
        recovery_events: Sequence[Mapping[str, object]],
    ) -> dict[str, object]:
        """Return bounded phase evidence without retaining raw sample dictionaries."""
        age_values = self._ages.values
        (
            episodes,
            ended_stale,
            effective_episodes_dropped,
            episode_count,
        ) = self._serialized_episodes(recovery_events)
        recovery_ranges = self._recovery_ranges(recovery_events)
        recovery_stale_seconds = 0.0
        for episode in episodes:
            episode_start = datetime.fromisoformat(episode["started_at"])
            episode_end = datetime.fromisoformat(episode["ended_at"])
            for recovery_start, recovery_end, _ in recovery_ranges:
                overlap = (
                    min(episode_end, recovery_end)
                    - max(episode_start, recovery_start)
                ).total_seconds()
                recovery_stale_seconds += max(0.0, overlap)
        recovery_stale_seconds = min(recovery_stale_seconds, self._stale_seconds)
        interval_values = self._intervals.values
        median_age = statistics.median(age_values) if age_values else None
        p95_age = _nearest_rank_p95(age_values)
        p99_age = (
            _nearest_rank_p99(age_values)
            if self._complete_samples >= 100 and len(age_values) >= 100
            else None
        )
        retained_episode_count = len(episodes)
        attribution_complete = effective_episodes_dropped == 0
        return {
            "freshness": {
                "samples": self._samples,
                "complete_samples": self._complete_samples,
                "median_worst_age_seconds": (
                    round(median_age, 3) if median_age is not None else None
                ),
                "p95_worst_age_seconds": (
                    round(p95_age, 3) if p95_age is not None else None
                ),
                "p99_worst_age_seconds": (
                    round(p99_age, 3) if p99_age is not None else None
                ),
                "max_worst_age_seconds": (
                    round(self._max_age, 3) if self._max_age is not None else None
                ),
                "worst_register_sample_counts": dict(self._worst_register_counts),
                "quantile_estimation": {
                    "method": self.QUANTILE_METHOD,
                    "capacity": self.quantile_capacity,
                    "samples_seen": self._ages.seen,
                    "samples_retained": len(age_values),
                    "exact": self._ages.exact,
                    "seed": self.reservoir_seed,
                },
            },
            "stale_threshold_violations": self._violations,
            "sampled_time_beyond_target_seconds": round(self._stale_seconds, 3),
            "sampled_time_beyond_target_by_health_state_seconds": {
                name: round(value, 3)
                for name, value in self._stale_by_health.items()
            },
            "sampled_time_by_health_state_seconds": {
                name: round(value, 3) for name, value in self._health_seconds.items()
            },
            "sampled_time_beyond_target_attribution_seconds": {
                "recovery_episode": (
                    round(recovery_stale_seconds, 3)
                    if attribution_complete
                    else None
                ),
                "normal_operation": (
                    round(self._stale_seconds - recovery_stale_seconds, 3)
                    if attribution_complete
                    else None
                ),
                "method": "continuous_interval_overlap",
                "complete": attribution_complete,
            },
            "violation_episodes": {
                "count": episode_count,
                "retained_count": retained_episode_count,
                "episodes_dropped": effective_episodes_dropped,
                "ended_stale": ended_stale,
                "right_censored_episode_count": int(ended_stale),
                "longest_duration_seconds": round(
                    max(
                        self._longest_episode_seconds,
                        float(self._active_episode["duration_seconds"])
                        if self._active_episode is not None
                        else 0.0,
                    ),
                    3,
                ),
                "sampling_interval_seconds": {
                    "median": (
                        round(statistics.median(interval_values), 6)
                        if interval_values
                        else None
                    ),
                    "max": (
                        round(self._maximum_interval_seconds, 6)
                        if self._maximum_interval_seconds is not None
                        else None
                    ),
                    "quantile_exact": self._intervals.exact,
                },
                "episodes": episodes,
            },
            "evidence_complete": bool(
                effective_episodes_dropped == 0 and self._clock_regressions == 0
            ),
            "bounded_retention": {
                "raw_samples_retained": 0,
                "quantile_capacity": self.quantile_capacity,
                "violation_episode_capacity": self.episode_capacity,
                "duration_clock": (
                    "monotonic"
                    if self._utc_fallback_intervals == 0
                    else "monotonic_with_utc_fallback"
                ),
                "utc_fallback_intervals": self._utc_fallback_intervals,
                "clock_regressions": self._clock_regressions,
            },
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


def _time_by_health_state(
    samples: Sequence[Mapping[str, object]],
) -> dict[str, float]:
    """Integrate all sampled operational time by acquisition health."""
    totals = {"healthy": 0.0, "recovering": 0.0, "degraded": 0.0}
    for current, following in zip(samples, samples[1:]):
        state = str(current.get("acquisition_health", "degraded"))
        if state not in totals:
            state = "degraded"
        totals[state] += (
            datetime.fromisoformat(str(following["at"]))
            - datetime.fromisoformat(str(current["at"]))
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
    final_sample_stale = False
    if samples:
        final_freshness = samples[-1]["profile_freshness"]
        final_sample_stale = bool(
            _maximum_age(final_freshness) is None
            or final_freshness["known"] != final_freshness["required"]
            or _maximum_age(final_freshness) > target_seconds
        )
    right_censored = active is not None and final_sample_stale
    if active is not None:
        episodes.append(active)

    serialized = []
    for index, episode in enumerate(episodes):
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
            "right_censored": bool(right_censored and index == len(episodes) - 1),
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
        "connection_dial_attempts",
        "failed_connection_dial_attempts",
    )
    result = {name: getattr(after, name) - getattr(before, name) for name in fields}
    events_recorded = (
        after.recovery_events_recorded - before.recovery_events_recorded
    )
    retained = min(max(events_recorded, 0), len(after.events))
    unretained = max(events_recorded - retained, 0)
    new_events = after.events[-retained:] if retained else ()
    result["events"] = [asdict(event) for event in new_events]
    result["event_retention"] = {
        "capacity": after.recovery_event_capacity,
        "recorded": events_recorded,
        "retained": retained,
        "unretained": unretained,
        "history_complete": unretained == 0,
        "lifetime_dropped": after.recovery_events_dropped,
    }
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
    reply_budgets = {
        request.reply_timeout_budget_ms / 1000 for request in requests
    }
    reply_timeout_seconds = (
        next(iter(reply_budgets)) if len(reply_budgets) == 1 else None
    )
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
            reply_timeout_seconds=reply_timeout_seconds,
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
        if int(report.get("schema_version", 0)) not in (3, 4, 5, 6, 7, 8, 9):
            raise ValueError(
                "qualification aggregation requires schema-v3 through v9 reports"
            )
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
    report_schemas: set[int] = set()
    provenance_reports = [
        report for report in reports if int(report.get("schema_version", 0)) >= 5
    ]
    if provenance_reports:
        report_schemas = {
            int(report.get("schema_version", 0)) for report in provenance_reports
        }
        if len(provenance_reports) != len(reports) or len(report_schemas) != 1:
            raise ValueError(
                "schema-v5+ qualification cannot aggregate across schemas"
            )
        provenance_keys = [
            "implementation_revision",
            "profile_definition_version",
            "drain_timeout_seconds",
            "reply_timeout_seconds",
            "split_request_deadlines",
        ]
        if report_schemas in ({6}, {7}, {8}, {9}):
            provenance_keys.extend(
                (
                    "tcp_keepalive_enabled",
                    "tcp_keepalive_idle_seconds",
                    "receive_inactivity_timeout_seconds",
                )
            )
            for report in provenance_reports:
                provenance = report.get("provenance")
                missing = [
                    key
                    for key in provenance_keys
                    if not isinstance(provenance, Mapping) or key not in provenance
                ]
                if missing:
                    raise ValueError(
                        "schema-v6+ qualification is missing required provenance"
                    )
        if report_schemas in ({8}, {9}):
            for report in provenance_reports:
                provenance = report.get("provenance")
                aggregation = (
                    provenance.get("freshness_aggregation")
                    if isinstance(provenance, Mapping)
                    else None
                )
                required_aggregation_fields = {
                    "mode",
                    "quantile_method",
                    "quantile_capacity",
                    "violation_episode_capacity",
                    "raw_samples_retained",
                    "duration_clock",
                }
                if (
                    not isinstance(provenance, Mapping)
                    or not isinstance(aggregation, Mapping)
                    or not required_aggregation_fields <= set(aggregation)
                ):
                    raise ValueError(
                        "schema-v8+ qualification is missing freshness aggregation provenance"
                    )
        provenance_sets = {
            tuple(report["provenance"][key] for key in provenance_keys)
            for report in provenance_reports
        }
        profile_signatures = {
            json.dumps(report.get("profile"), sort_keys=True, separators=(",", ":"))
            for report in provenance_reports
        }
        recovery_signatures = {
            json.dumps(
                report.get("recovery_policy"),
                sort_keys=True,
                separators=(",", ":"),
            )
            for report in provenance_reports
            if int(report.get("schema_version", 0)) >= 7
        }
        freshness_aggregation_signatures = {
            json.dumps(
                report.get("provenance", {}).get("freshness_aggregation"),
                sort_keys=True,
                separators=(",", ":"),
            )
            for report in provenance_reports
            if int(report.get("schema_version", 0)) >= 8
        }
        if (
            len(provenance_sets) != 1
            or len(profile_signatures) != 1
            or len(recovery_signatures) > 1
            or len(freshness_aggregation_signatures) > 1
        ):
            raise ValueError(
                "qualification reports use different revisions, profiles, "
                "or deadline/liveness configuration"
            )
        aggregate_reply_timeout = float(
            provenance_reports[0]["provenance"]["reply_timeout_seconds"]
        )
    else:
        aggregate_reply_timeout = READ_TIMEOUT

    runtime = sum(float(phase["actual_duration_seconds"]) for _, phase in phases)
    established_session_fields = (
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
    liveness_session_fields = (
        "tcp_keepalive_applied_connections",
        "tcp_keepalive_idle_applied_connections",
        "tcp_keepalive_configuration_failures",
        "tcp_keepalive_configuration_unavailable",
        "receive_inactivity_timeouts",
    )
    for report, phase in phases:
        if int(report["schema_version"]) >= 6 and any(
            field not in phase["session_metrics"]
            for field in liveness_session_fields
        ):
            raise ValueError(
                "schema-v6 qualification is missing required liveness metrics"
            )
        if int(report["schema_version"]) >= 9:
            retention = phase.get("recovery_metrics", {}).get("event_retention")
            required_retention_fields = {
                "capacity",
                "recorded",
                "retained",
                "unretained",
                "history_complete",
                "lifetime_dropped",
            }
            if (
                not isinstance(retention, Mapping)
                or not required_retention_fields <= set(retention)
            ):
                raise ValueError(
                    "schema-v9 qualification is missing recovery-event retention"
                )
    session_totals = {
        field: sum(int(phase["session_metrics"][field]) for _, phase in phases)
        for field in established_session_fields
    }
    session_totals.update(
        {
            field: sum(
                int(phase["session_metrics"][field])
                if int(report["schema_version"]) >= 6
                else int(phase["session_metrics"].get(field, 0))
                for report, phase in phases
            )
            for field in liveness_session_fields
        }
    )
    recovery_fields = (
        "reconnect_attempts",
        "successful_reconnects",
        "failed_reconnects",
        "retry_budget_exhausted",
        "acquisitions_abandoned",
        "connection_dial_attempts",
        "failed_connection_dial_attempts",
    )
    recovery_totals = {
        field: sum(
            int(phase["recovery_metrics"].get(field, 0))
            for _, phase in phases
        )
        for field in recovery_fields
    }
    recovery_event_retention = (
        {
            "recorded": sum(
                int(phase["recovery_metrics"]["event_retention"]["recorded"])
                for _, phase in phases
            ),
            "retained": sum(
                int(phase["recovery_metrics"]["event_retention"]["retained"])
                for _, phase in phases
            ),
            "unretained": sum(
                int(phase["recovery_metrics"]["event_retention"]["unretained"])
                for _, phase in phases
            ),
            "history_complete": all(
                bool(
                    phase["recovery_metrics"]["event_retention"][
                        "history_complete"
                    ]
                )
                for _, phase in phases
            ),
        }
        if report_schemas == {9}
        else None
    )
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
    health_state_duration_seconds = {
        state: round(
            sum(
                float(
                    phase.get("sampled_time_by_health_state_seconds", {}).get(
                        state, 0
                    )
                )
                for _, phase in phases
            ),
            3,
        )
        for state in ("healthy", "recovering", "degraded")
    }
    right_censored_episode_count = sum(
        int(phase["violation_episodes"].get("right_censored_episode_count", 0))
        if int(report["schema_version"]) >= 8
        else sum(
            bool(episode.get("right_censored", False))
            for episode in phase["violation_episodes"].get("episodes", [])
        )
        for report, phase in phases
    )
    runs_ending_stale = sum(
        bool(phase["violation_episodes"].get("ended_stale", False))
        if int(report["schema_version"]) >= 8
        else any(
            bool(episode.get("right_censored", False))
            for episode in phase["violation_episodes"].get("episodes", [])
        )
        for report, phase in phases
    )
    evidence_complete = all(
        bool(phase.get("freshness_evidence_complete", False))
        if int(report["schema_version"]) >= 8
        else True
        for report, phase in phases
    )
    episode_details_dropped = sum(
        int(phase["violation_episodes"].get("episodes_dropped", 0))
        for _, phase in phases
    )
    return {
        "schema_version": (
            4
            if report_schemas in ({8}, {9})
            else 3
            if report_schemas == {7}
            else 2
        ),
        "source_report_schema_version": (
            next(iter({int(report["schema_version"]) for report, _ in phases}))
            if len({int(report["schema_version"]) for report, _ in phases}) == 1
            else sorted({int(report["schema_version"]) for report, _ in phases})
        ),
        "target_seconds": targets.pop(),
        "deadline_configuration": (
            {
                "drain_timeout_seconds": provenance_reports[0]["provenance"][
                    "drain_timeout_seconds"
                ],
                "reply_timeout_seconds": aggregate_reply_timeout,
                "split_request_deadlines": provenance_reports[0]["provenance"][
                    "split_request_deadlines"
                ],
            }
            if provenance_reports
            else {
                "drain_timeout_seconds": READ_TIMEOUT,
                "reply_timeout_seconds": READ_TIMEOUT,
                "split_request_deadlines": False,
            }
        ),
        "liveness_configuration": (
            {
                "tcp_keepalive_enabled": provenance_reports[0]["provenance"].get(
                    "tcp_keepalive_enabled"
                ),
                "tcp_keepalive_idle_seconds": provenance_reports[0][
                    "provenance"
                ].get("tcp_keepalive_idle_seconds"),
                "receive_inactivity_timeout_seconds": provenance_reports[0][
                    "provenance"
                ].get("receive_inactivity_timeout_seconds"),
            }
            if report_schemas in ({6}, {7}, {8}, {9})
            else None
        ),
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
        "recovery_event_retention": recovery_event_retention,
        "health_state_duration_seconds": health_state_duration_seconds,
        "observation_delivery_complete": bool(
            session_totals["observation_queue_drops"] == 0
        ),
        "explicit_requests_avoided_unsolicited": avoided,
        "observed_rates": {
            "timeouts_per_explicit_request": (
                round(timeouts / attempts, 9) if attempts else None
            ),
            "timeouts_per_hour": round(timeouts * 3600 / runtime, 6),
            "reconnects_per_hour": round(reconnects * 3600 / runtime, 6),
        },
        "request_latency_ms": _latency_summary(
            latency_values,
            samples_total=latency_samples_total,
            reply_timeout_seconds=aggregate_reply_timeout,
        ),
        "freshness": {
            "strict_target_met": all(bool(phase["target_met"]) for _, phase in phases),
            "evidence_complete": evidence_complete,
            "episode_details_dropped": episode_details_dropped,
            "violating_samples": violations,
            "sampled_time_beyond_target_seconds": round(stale_duration, 3),
            "longest_violation_episode_seconds": round(longest, 3),
            "right_censored_episode_count": right_censored_episode_count,
            "runs_ending_stale": runs_ending_stale,
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
    freshness_evidence = StreamingProfileFreshnessAggregator(target_seconds)
    error = None
    started = time.monotonic()
    try:
        await client.async_run_profile(
            duration_seconds, sample_sink=freshness_evidence
        )
    except LuxPowerCommunicationError as exc:
        error = type(exc).__name__
    actual_duration = time.monotonic() - started
    after_session = client.metrics()
    after_profile = client.profile_metrics()
    after_recovery = client.recovery_metrics()
    after_diagnostics = _client_diagnostics(client)
    session_delta = _metrics_delta(
        before_session,
        after_session,
        reply_timeout_seconds=getattr(
            client, "reply_timeout_seconds", READ_TIMEOUT
        ),
    )
    attempted = (
        after_profile.explicit_requests_attempted
        - before_profile.explicit_requests_attempted
    )
    avoided = (
        after_profile.explicit_requests_avoided_unsolicited
        - before_profile.explicit_requests_avoided_unsolicited
    )
    unsafe_events = session_delta["invalid_frames"]
    observation_delivery_complete = bool(
        session_delta["observation_queue_drops"] == 0
    )
    recovery_delta = _recovery_metrics_delta(before_recovery, after_recovery)
    streamed = freshness_evidence.finalize(recovery_delta["events"])
    freshness = streamed["freshness"]
    violations = streamed["stale_threshold_violations"]
    recovery_safe = bool(
        not error
        and not unsafe_events
        and recovery_delta["event_retention"]["history_complete"]
        and recovery_delta["failed_reconnects"] == 0
        and recovery_delta["retry_budget_exhausted"] == 0
        and all(
            event["outcome"] == "profile_recovered"
            for event in recovery_delta["events"]
        )
    )
    freshness_met = bool(freshness["samples"] and not violations)
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
        "sampled_time_beyond_target_seconds": streamed[
            "sampled_time_beyond_target_seconds"
        ],
        "sampled_time_beyond_target_by_health_state_seconds": streamed[
            "sampled_time_beyond_target_by_health_state_seconds"
        ],
        "sampled_time_by_health_state_seconds": streamed[
            "sampled_time_by_health_state_seconds"
        ],
        "sampled_time_beyond_target_attribution_seconds": streamed[
            "sampled_time_beyond_target_attribution_seconds"
        ],
        "violation_episodes": streamed["violation_episodes"],
        "freshness_evidence_complete": streamed["evidence_complete"],
        "freshness_retention": streamed["bounded_retention"],
        "transport_recovery_safe": recovery_safe,
        "observation_delivery_complete": observation_delivery_complete,
        "freshness_target_met": freshness_met,
        "target_met": bool(
            recovery_safe
            and observation_delivery_complete
            and freshness_met
            and streamed["evidence_complete"]
        ),
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
            "drain_timeout_seconds": getattr(
                client, "drain_timeout_seconds", READ_TIMEOUT
            ),
            "reply_timeout_seconds": getattr(
                client, "reply_timeout_seconds", READ_TIMEOUT
            ),
            "split_request_deadlines": getattr(
                client, "split_request_deadlines", False
            ),
            "tcp_keepalive_enabled": getattr(
                client, "tcp_keepalive_enabled", True
            ),
            "tcp_keepalive_idle_seconds": getattr(
                client, "tcp_keepalive_idle_seconds", 60
            ),
            "receive_inactivity_timeout_seconds": getattr(
                client, "receive_inactivity_timeout_seconds", 900.0
            ),
            "freshness_aggregation": {
                "mode": "streaming_bounded",
                "quantile_method": (
                    StreamingProfileFreshnessAggregator.QUANTILE_METHOD
                ),
                "quantile_capacity": PROFILE_FRESHNESS_QUANTILE_CAPACITY,
                "violation_episode_capacity": (
                    PROFILE_VIOLATION_EPISODE_CAPACITY
                ),
                "raw_samples_retained": 0,
                "duration_clock": "monotonic",
            },
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


def _validate_deadline_options(
    drain_timeout_seconds: float | None,
    reply_timeout_seconds: float | None,
) -> None:
    """Require an explicit, fully attributable split for live qualification."""
    if (drain_timeout_seconds is None) != (reply_timeout_seconds is None):
        raise ValueError(
            "drain and reply timeout options must be supplied together"
        )


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
    parser.add_argument(
        "--drain-timeout-seconds",
        type=float,
        help=(
            "experimental independent writer-drain deadline; requires "
            "--reply-timeout-seconds"
        ),
    )
    parser.add_argument(
        "--reply-timeout-seconds",
        type=float,
        help=(
            "experimental independent correlated-reply deadline; requires "
            "--drain-timeout-seconds"
        ),
    )
    parser.add_argument("--enable-recovery", action="store_true")
    parser.add_argument("--recovery-window-seconds", type=float, default=300)
    parser.add_argument("--recovery-window-attempts", type=int, default=2)
    parser.add_argument(
        "--recovery-connection-attempts",
        type=int,
        default=3,
        help="bounded TCP dial attempts inside one recovery episode",
    )
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
            max_connection_attempts_per_reconnect=(
                arguments.recovery_connection_attempts
            ),
            rolling_window_seconds=arguments.recovery_window_seconds,
            initial_cooldown_seconds=arguments.recovery_initial_cooldown,
            repeated_cooldown_seconds=arguments.recovery_repeated_cooldown,
        )
        if arguments.enable_recovery
        else None
    )
    _validate_deadline_options(
        arguments.drain_timeout_seconds,
        arguments.reply_timeout_seconds,
    )
    session = LuxReadSession(
        host,
        dongle,
        inverter,
        port=port,
        drain_timeout=arguments.drain_timeout_seconds,
        reply_timeout=arguments.reply_timeout_seconds,
    )
    client = LuxPowerHybridReadClient(
        host,
        dongle,
        inverter,
        port=port,
        profile=profile,
        session=session,
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
