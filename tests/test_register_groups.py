"""Tests for Home Assistant-independent semantic register classification."""

from custom_components.lxp_modbus.const import TOTAL_REGISTERS
from custom_components.lxp_modbus.constants.battery_registers import (
    B_CYCLE_COUNT,
    B_SOH_SOC,
)
from custom_components.lxp_modbus.constants.input_registers import (
    I_EPV1_ALL_L,
    I_EPV1_DAY,
    I_PCHARGE,
    I_PLOAD,
    I_PPV1,
    I_SOC_SOH,
    I_TINNER,
)
from luxpower import (
    TelemetryGroup,
    battery_register_group,
    group_input_registers,
    holding_register_group,
    input_register_group,
    input_registers_for_group,
)


def test_important_operational_registers_are_classified_from_documented_meaning():
    """Control-relevant state and power values belong to the operational group."""
    for register in (I_PPV1, I_PCHARGE, I_SOC_SOH, I_PLOAD):
        assert input_register_group(register) is TelemetryGroup.OPERATIONAL


def test_slower_semantic_groups_are_explicit():
    """Temperature and energy values are not conflated with operational power."""
    assert input_register_group(I_TINNER) is TelemetryGroup.DIAGNOSTIC
    assert input_register_group(I_EPV1_DAY) is TelemetryGroup.ENERGY_COUNTER
    assert input_register_group(I_EPV1_ALL_L) is TelemetryGroup.ENERGY_COUNTER
    assert holding_register_group(21) is TelemetryGroup.CONFIGURATION
    assert battery_register_group(B_SOH_SOC) is TelemetryGroup.OPERATIONAL
    assert battery_register_group(B_CYCLE_COUNT) is TelemetryGroup.ENERGY_COUNTER


def test_unknown_registers_are_preserved_instead_of_omitted():
    """Undefined and future addresses remain visible as unclassified data."""
    values = {I_PPV1: 1000, 68: 123, TOTAL_REGISTERS - 1: 456, 900: 789}

    grouped = group_input_registers(values)

    assert grouped[TelemetryGroup.OPERATIONAL] == {I_PPV1: 1000}
    assert grouped[TelemetryGroup.UNCLASSIFIED] == {
        68: 123,
        TOTAL_REGISTERS - 1: 456,
        900: 789,
    }
    assert sum(len(group) for group in grouped.values()) == len(values)


def test_classification_is_deterministic_and_covers_the_normal_scan():
    """Every ordinary input address receives one stable classification."""
    first = {
        group: input_registers_for_group(group)
        for group in TelemetryGroup
    }
    second = {
        group: input_registers_for_group(group)
        for group in TelemetryGroup
    }

    assert first == second
    assert set().union(*first.values()) == set(range(TOTAL_REGISTERS))
    assert sum(len(registers) for registers in first.values()) == TOTAL_REGISTERS
    assert 68 in first[TelemetryGroup.UNCLASSIFIED]
    assert TOTAL_REGISTERS - 1 in first[TelemetryGroup.UNCLASSIFIED]


def test_grouping_is_pure_metadata_over_an_existing_snapshot():
    """Grouping returns detached values and cannot initiate or reshape requests."""
    values = {125: 1, 0: 2, 68: 3}

    first = group_input_registers(values)
    second = group_input_registers(values)
    first[TelemetryGroup.OPERATIONAL][0] = 999

    assert values == {125: 1, 0: 2, 68: 3}
    assert second == group_input_registers(values)
