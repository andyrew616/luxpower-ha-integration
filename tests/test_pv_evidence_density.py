"""Block-0 density is independent of truthful 20-second presentation freshness."""
from datetime import UTC, datetime, timedelta

import pytest
from test_hybrid_telemetry import FakeSession

from custom_components.lxp_modbus.exceptions import LuxPowerReadTimeoutError
from custom_components.lxp_modbus.read_profiles import (
    DiagnosticReadProfile,
    InputReadBlock,
)
from luxpower.hybrid import LuxPowerHybridReadClient

BASE = datetime(2026, 9, 29, tzinfo=UTC)


def client(session, enabled=True):
    return LuxPowerHybridReadClient('192.0.2.1', 'TESTDONGLE', 'TESTINV001',
        session=session, profile=DiagnosticReadProfile(), freshness_target=timedelta(seconds=20),
        block0_evidence_density=enabled)


@pytest.mark.asyncio
@pytest.mark.parametrize('enabled,expected', [(False, {0, 1}), (True, {0, 1, 2})])
async def test_demonstrated_three_cell_pattern(monkeypatch, enabled, expected):
    clock = [BASE + timedelta(seconds=1)]
    monkeypatch.setattr('test_hybrid_telemetry.utc_now', lambda: clock[0])
    monkeypatch.setattr('luxpower.hybrid.utc_now', lambda: clock[0])
    session = FakeSession()
    owner = client(session, enabled)
    cells = set()
    for seconds, unsolicited in [(1, True), (22, False), (31, True), (42, False)]:
        clock[0] = BASE + timedelta(seconds=seconds)
        if unsolicited:
            session.observe_unsolicited(InputReadBlock(0, 40))
            session.observe_unsolicited(InputReadBlock(80, 40))
        else:
            await owner.async_refresh_profile()
        cells.add(int((session.observed[7]-BASE).total_seconds()//20))
    assert cells == expected
    assert owner._freshness_target == timedelta(seconds=20)
    assert session.reads == ([(80, 40), (0, 40), (0, 40)] if enabled else [(0, 40), (80, 40)])


@pytest.mark.asyncio
@pytest.mark.parametrize('old,now,refresh', [
    (19.999999, 20, True), (20, 20.000001, True),
    (39.999999, 40, True), (40, 59.999999, True),
    (1799.999999, 1800, True), (1800, 1800.000001, True)])
async def test_utc_cell_and_half_hour_edges(monkeypatch, old, now, refresh):
    clock = [BASE + timedelta(seconds=old)]
    monkeypatch.setattr('test_hybrid_telemetry.utc_now', lambda: clock[0])
    monkeypatch.setattr('luxpower.hybrid.utc_now', lambda: clock[0])
    session = FakeSession((InputReadBlock(0, 40), InputReadBlock(80, 40)))
    clock[0] = BASE + timedelta(seconds=now)
    await client(session).async_refresh_profile()
    assert session.reads == ([(0, 40)] if refresh else [])


@pytest.mark.asyncio
async def test_cached_refresh_does_not_advance_observation_or_loop(monkeypatch):
    clock = [BASE + timedelta(seconds=31)]
    monkeypatch.setattr('test_hybrid_telemetry.utc_now', lambda: clock[0])
    monkeypatch.setattr('luxpower.hybrid.utc_now', lambda: clock[0])
    session = FakeSession((InputReadBlock(0, 40), InputReadBlock(80, 40)))
    before = dict(session.observed)
    async def cached(start, count, **kwargs):
        session.reads.append((start, count))
    session.async_read_input = cached
    clock[0] = BASE + timedelta(seconds=42)
    await client(session).async_refresh_profile()
    assert session.reads == [(0, 40)]
    assert session.observed == before
    assert int((session.observed[7]-BASE).total_seconds()//20) == 1


@pytest.mark.asyncio
async def test_failed_refresh_never_promotes_timestamp(monkeypatch):
    clock = [BASE + timedelta(seconds=31)]
    monkeypatch.setattr('test_hybrid_telemetry.utc_now', lambda: clock[0])
    monkeypatch.setattr('luxpower.hybrid.utc_now', lambda: clock[0])
    session = FakeSession((InputReadBlock(0, 40), InputReadBlock(80, 40)))
    before = dict(session.observed)
    async def timeout(*args, **kwargs):
        raise LuxPowerReadTimeoutError('simulated failure')
    session.async_read_input = timeout
    clock[0] = BASE + timedelta(seconds=42)
    with pytest.raises(LuxPowerReadTimeoutError):
        await client(session).async_refresh_profile()
    assert session.observed == before


@pytest.mark.asyncio
async def test_pv_acceptance_follows_slow_block80_before_profile_publication(monkeypatch):
    clock = [BASE + timedelta(seconds=31)]
    monkeypatch.setattr('test_hybrid_telemetry.utc_now', lambda: clock[0])
    monkeypatch.setattr('luxpower.hybrid.utc_now', lambda: clock[0])
    session = FakeSession((InputReadBlock(0, 40), InputReadBlock(80, 40)))
    original = session.async_read_input
    async def reply(start, count, **kwargs):
        clock[0] += timedelta(seconds=.5)
        await original(start, count, **kwargs)
    session.async_read_input = reply
    owner = client(session)
    clock[0] = BASE + timedelta(seconds=52)
    await owner.async_refresh_profile()
    assert session.reads == [(80,40),(0,40)]
    assert session.observed[7] > session.observed[114]
    assert session.observed[7] == clock[0]
    count = len(session.reads)
    clock[0] += timedelta(seconds=.1)
    await owner.async_refresh_profile()
    assert len(session.reads) == count, 'same successfully acquired cell is not polled again'
