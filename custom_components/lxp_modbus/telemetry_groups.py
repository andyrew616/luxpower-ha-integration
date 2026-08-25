"""Semantic LuxPower register groups for read-only telemetry consumers.

The groups describe what a register represents.  They deliberately do not define
polling intervals or Modbus ranges; the existing poller continues to read the same
blocks at the same cadence.  Registers without sufficiently documented semantics
remain explicitly unclassified.
"""

from enum import Enum
from typing import Mapping, TypeVar

from .const import TOTAL_REGISTERS
from .constants import battery_registers as battery
from .constants import input_registers as input_register


class TelemetryGroup(str, Enum):
    """A semantic category that does not promise a particular polling cadence."""

    OPERATIONAL = "operational"
    DIAGNOSTIC = "diagnostic"
    ENERGY_COUNTER = "energy_counter"
    CONFIGURATION = "configuration"
    METADATA = "metadata"
    UNCLASSIFIED = "unclassified"


_OPERATIONAL_INPUT_REGISTERS = frozenset({
    input_register.I_STATE,
    input_register.I_VPV1,
    input_register.I_VPV2,
    input_register.I_VPV3,
    input_register.I_VBAT,
    input_register.I_SOC_SOH,
    input_register.I_PPV1,
    input_register.I_PPV2,
    input_register.I_PPV3,
    input_register.I_PCHARGE,
    input_register.I_PDISCHARGE,
    input_register.I_VAC_R,
    input_register.I_VAC_S,
    input_register.I_VAC_T,
    input_register.I_FAC,
    input_register.I_PINV,
    input_register.I_PREC,
    input_register.I_IINV_RMS,
    input_register.I_PF,
    input_register.I_VEPS_R,
    input_register.I_VEPS_S,
    input_register.I_VEPS_T,
    input_register.I_FEPS,
    input_register.I_PEPS,
    input_register.I_SEPS,
    input_register.I_PTOGRID,
    input_register.I_PTOUSER,
    input_register.I_VBUS1,
    input_register.I_VBUS2,
    input_register.I_AC_INPUT_TYPE_FLAGS,
    input_register.I_BMS_MAX_CHG_CURR,
    input_register.I_BMS_MAX_DISCHG_CURR,
    input_register.I_BMS_CHARGE_VOLT_REF,
    input_register.I_BMS_DISCHG_CUT_VOLT,
    input_register.I_BMS_BAT_STATUS_INV,
    input_register.I_BMS_BAT_CURRENT,
    input_register.I_INV_BAT_VOLT_SAMPLE,
    input_register.I_ONGRID_LOAD_POWER,
    input_register.I_VBUS_P,
    input_register.I_GEN_VOLT,
    input_register.I_GEN_FREQ,
    input_register.I_GEN_POWER,
    input_register.I_EPS_VOLT_L1N,
    input_register.I_EPS_VOLT_L2N,
    input_register.I_PEPS_L1N,
    input_register.I_PEPS_L2N,
    input_register.I_SEPS_L1N,
    input_register.I_SEPS_L2N,
    input_register.I_QINV,
    input_register.I_AC_COUPLE_POWER,
    input_register.I_PLOAD,
    input_register.I_PINV_S,
    input_register.I_PINV_T,
    input_register.I_PREC_S,
    input_register.I_PREC_T,
    input_register.I_PTOGRID_S,
    input_register.I_PTOGRID_T,
    input_register.I_PTOUSER_S,
    input_register.I_PTOUSER_T,
    input_register.I_GEN_POWER_S,
    input_register.I_GEN_POWER_T,
    input_register.I_IINV_RMS_S,
    input_register.I_IINV_RMS_T,
    input_register.I_PF_S,
    input_register.I_GRID_VOLT_L1N,
    input_register.I_GRID_VOLT_L2N,
    input_register.I_GEN_VOLT_L1N,
    input_register.I_GEN_VOLT_L2N,
    input_register.I_PINV_L1N,
    input_register.I_PINV_L2N,
    input_register.I_PREC_L1N,
    input_register.I_PREC_L2N,
    input_register.I_PTOGRID_L1N,
    input_register.I_PTOGRID_L2N,
    input_register.I_PTOUSER_L1N,
    input_register.I_PTOUSER_L2N,
    input_register.I_PF_T,
    input_register.I_AC_COUPLE_POWER_S,
    input_register.I_AC_COUPLE_POWER_T,
    input_register.I_ONGRID_LOAD_POWER_S,
    input_register.I_ONGRID_LOAD_POWER_T,
    input_register.I_REMAINING_CHARGE_TIME,
    input_register.I_VPV4,
    input_register.I_VPV5,
    input_register.I_VPV6,
    input_register.I_PPV4,
    input_register.I_PPV5,
    input_register.I_PPV6,
    input_register.I_SMART_LOAD_POWER,
})

_DIAGNOSTIC_INPUT_REGISTERS = frozenset({
    input_register.I_INTERNAL_FAULT,
    input_register.I_FAULT_CODE_L,
    input_register.I_FAULT_CODE_H,
    input_register.I_WARNING_CODE_L,
    input_register.I_WARNING_CODE_H,
    input_register.I_TINNER,
    input_register.I_TRADIATOR1,
    input_register.I_TRADIATOR2,
    input_register.I_TBAT,
    input_register.I_AUTO_TEST_STATUS,
    input_register.I_BMS_BAT_STATUS_0,
    input_register.I_BMS_BAT_STATUS_1,
    input_register.I_BMS_BAT_STATUS_2,
    input_register.I_BMS_BAT_STATUS_3,
    input_register.I_BMS_BAT_STATUS_4,
    input_register.I_BMS_BAT_STATUS_5,
    input_register.I_BMS_BAT_STATUS_6,
    input_register.I_BMS_BAT_STATUS_7,
    input_register.I_BMS_BAT_STATUS_8,
    input_register.I_BMS_BAT_STATUS_9,
    input_register.I_BMS_FAULT_CODE,
    input_register.I_BMS_WARNING_CODE,
    input_register.I_BMS_MAX_CELL_VOLT,
    input_register.I_BMS_MIN_CELL_VOLT,
    input_register.I_BMS_MAX_CELL_TEMP,
    input_register.I_BMS_MIN_CELL_TEMP,
    input_register.I_BMS_FW_UPDATE_STATE,
    input_register.I_MASTER_SLAVE_PARALLEL_STATUS,
    input_register.I_SWITCH_STATE,
    input_register.I_EXCEPTION_REASON_1,
    input_register.I_EXCEPTION_REASON_2,
    input_register.I_CHG_DISCHG_DISABLE_REASON,
    input_register.I_TEMP_NTC_FOR_INDC,
    input_register.I_TEMP_NTC_FOR_DCDCL,
    input_register.I_TEMP_NTC_FOR_DCDCH,
})

_ENERGY_COUNTER_INPUT_REGISTERS = frozenset({
    input_register.I_EPV1_DAY,
    input_register.I_EPV2_DAY,
    input_register.I_EPV3_DAY,
    input_register.I_EINV_DAY,
    input_register.I_EREC_DAY,
    input_register.I_ECHG_DAY,
    input_register.I_EDISCHG_DAY,
    input_register.I_EEPS_DAY,
    input_register.I_ETOGRID_DAY,
    input_register.I_ETOUSER_DAY,
    input_register.I_EPV1_ALL_L,
    input_register.I_EPV1_ALL_H,
    input_register.I_EPV2_ALL_L,
    input_register.I_EPV2_ALL_H,
    input_register.I_EPV3_ALL_L,
    input_register.I_EPV3_ALL_H,
    input_register.I_EINV_ALL_L,
    input_register.I_EINV_ALL_H,
    input_register.I_EREC_ALL_L,
    input_register.I_EREC_ALL_H,
    input_register.I_ECHG_ALL_L,
    input_register.I_ECHG_ALL_H,
    input_register.I_EDISCHG_ALL_L,
    input_register.I_EDISCHG_ALL_H,
    input_register.I_EEPS_ALL_L,
    input_register.I_EEPS_ALL_H,
    input_register.I_ETOGRID_ALL_L,
    input_register.I_ETOGRID_ALL_H,
    input_register.I_ETOUSER_ALL_L,
    input_register.I_ETOUSER_ALL_H,
    input_register.I_RUNNING_TIME_L,
    input_register.I_RUNNING_TIME_H,
    input_register.I_BMS_CYCLE_COUNT,
    input_register.I_EGEN_DAY,
    input_register.I_EGEN_ALL_L,
    input_register.I_EGEN_ALL_H,
    input_register.I_EEPS_L1N_DAY,
    input_register.I_EEPS_L2N_DAY,
    input_register.I_EEPS_L1N_ALL_L,
    input_register.I_EEPS_L1N_ALL_H,
    input_register.I_EEPS_L2N_ALL_L,
    input_register.I_EEPS_L2N_ALL_H,
    input_register.I_ELOAD_DAY,
    input_register.I_ELOAD_ALL_L,
    input_register.I_ELOAD_ALL_H,
    input_register.I_EPV4_DAY,
    input_register.I_EPV4_ALL_L,
    input_register.I_EPV4_ALL_H,
    input_register.I_EPV5_DAY,
    input_register.I_EPV5_ALL_L,
    input_register.I_EPV5_ALL_H,
    input_register.I_EPV6_DAY,
    input_register.I_EPV6_ALL_L,
    input_register.I_EPV6_ALL_H,
})

_METADATA_INPUT_REGISTERS = frozenset({
    input_register.I_W_AUTO_TEST_LIMIT,
    input_register.I_UW_AUTO_TEST_DEFAULT_TIME,
    input_register.I_UW_AUTO_TEST_TRIP_VALUE,
    input_register.I_UW_AUTO_TEST_TRIP_TIME,
    input_register.I_BAT_TYPE_AND_BRAND,
    input_register.I_BAT_PARALLEL_NUM,
    input_register.I_BAT_CAPACITY,
    input_register.I_SERIAL_NUMBER_0_3,
    input_register.I_SERIAL_NUMBER_4_5,
    input_register.I_SERIAL_NUMBER_6_7,
    input_register.I_SERIAL_NUMBER_8_9,
})

_EXPLICIT_INPUT_GROUPS = {
    TelemetryGroup.OPERATIONAL: _OPERATIONAL_INPUT_REGISTERS,
    TelemetryGroup.DIAGNOSTIC: _DIAGNOSTIC_INPUT_REGISTERS,
    TelemetryGroup.ENERGY_COUNTER: _ENERGY_COUNTER_INPUT_REGISTERS,
    TelemetryGroup.METADATA: _METADATA_INPUT_REGISTERS,
}


def _build_input_group_map() -> dict[int, TelemetryGroup]:
    groups: dict[int, TelemetryGroup] = {}
    for group, registers in _EXPLICIT_INPUT_GROUPS.items():
        for register in registers:
            if register in groups:
                raise RuntimeError(f"input register {register} belongs to multiple groups")
            groups[register] = group
    for register in range(TOTAL_REGISTERS):
        groups.setdefault(register, TelemetryGroup.UNCLASSIFIED)
    return groups


_INPUT_GROUP_BY_REGISTER = _build_input_group_map()

_BATTERY_GROUP_BY_REGISTER = {
    battery.B_CAPACITY: TelemetryGroup.METADATA,
    battery.B_MAX_CHARGE_CURRENT: TelemetryGroup.OPERATIONAL,
    battery.B_MAX_DISCHARGE_CURRENT: TelemetryGroup.OPERATIONAL,
    battery.B_VOLTAGE: TelemetryGroup.OPERATIONAL,
    battery.B_CURRENT: TelemetryGroup.OPERATIONAL,
    # SOC and SOH share one packed register, so the more time-sensitive use wins.
    battery.B_SOH_SOC: TelemetryGroup.OPERATIONAL,
    battery.B_CYCLE_COUNT: TelemetryGroup.ENERGY_COUNTER,
    battery.B_MAX_CELL_TEMP: TelemetryGroup.DIAGNOSTIC,
    battery.B_MIN_CELL_TEMP: TelemetryGroup.DIAGNOSTIC,
    battery.B_MAX_CELL_VOLTAGE: TelemetryGroup.DIAGNOSTIC,
    battery.B_MIN_CELL_VOLTAGE: TelemetryGroup.DIAGNOSTIC,
    battery.B_TEMP_CELLS: TelemetryGroup.DIAGNOSTIC,
    battery.B_VOLTAGE_CELLS: TelemetryGroup.DIAGNOSTIC,
    battery.B_FIRMWARE: TelemetryGroup.METADATA,
    "serial": TelemetryGroup.METADATA,
}

RegisterValue = TypeVar("RegisterValue")


def input_register_group(register: int) -> TelemetryGroup:
    """Return the documented semantic group for an input-register address."""
    return _INPUT_GROUP_BY_REGISTER.get(register, TelemetryGroup.UNCLASSIFIED)


def holding_register_group(register: int) -> TelemetryGroup:
    """Classify holding registers as configuration without inferring their meaning."""
    return (
        TelemetryGroup.CONFIGURATION
        if isinstance(register, int) and register >= 0
        else TelemetryGroup.UNCLASSIFIED
    )


def battery_register_group(register: int | str) -> TelemetryGroup:
    """Return the semantic group for a decoded battery-block value."""
    return _BATTERY_GROUP_BY_REGISTER.get(register, TelemetryGroup.UNCLASSIFIED)


def input_registers_for_group(group: TelemetryGroup) -> frozenset[int]:
    """Return all addresses in the ordinary 0-749 scan assigned to ``group``."""
    return frozenset(
        register for register, assigned_group in _INPUT_GROUP_BY_REGISTER.items()
        if assigned_group is group
    )


def group_input_registers(
    registers: Mapping[int, RegisterValue],
) -> dict[TelemetryGroup, dict[int, RegisterValue]]:
    """Group a snapshot deterministically while retaining every supplied register."""
    grouped = {group: {} for group in TelemetryGroup}
    for register in sorted(registers):
        grouped[input_register_group(register)][register] = registers[register]
    return grouped
