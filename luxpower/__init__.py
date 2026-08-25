"""Public, Home Assistant-independent LuxPower read client."""

from custom_components.lxp_modbus.classes.read_client import (
    LuxPowerReadClient,
    LuxPowerTelemetry,
)
from custom_components.lxp_modbus.observation import LuxPowerObservationTimes
from custom_components.lxp_modbus.telemetry_groups import (
    TelemetryGroup,
    battery_register_group,
    group_input_registers,
    holding_register_group,
    input_register_group,
    input_registers_for_group,
)
from custom_components.lxp_modbus.exceptions import (
    LuxPowerAmbiguousRequestError,
    LuxPowerCommunicationError,
    LuxPowerConnectionError,
    LuxPowerConnectionLostError,
    LuxPowerError,
    LuxPowerRecoveryExhaustedError,
    LuxPowerReadRejectedError,
    LuxPowerReadTimeoutError,
    LuxPowerSessionClosedError,
)
from custom_components.lxp_modbus.classes.read_session import (
    LuxObservationSource,
    LuxReadObservation,
    LuxReadSession,
    LuxReadSessionMetrics,
    LuxReadSessionSnapshot,
)
from custom_components.lxp_modbus.read_profiles import (
    EnergyFlowReadProfile,
    EnergyFlowSnapshot,
    GridTopology,
    InputReadBlock,
    LoadLayout,
    ObservedProfileValue,
    ProfileField,
    ReadProfileName,
    plan_aligned_input_blocks,
    profile_block_details,
)
from custom_components.lxp_modbus.recovery import (
    AcquisitionHealth,
    RecoveryEvent,
    RecoveryFailureKind,
    RecoveryMetrics,
    RecoveryPolicy,
)

__all__ = [
    "AcquisitionHealth",
    "LuxPowerAmbiguousRequestError",
    "LuxPowerCommunicationError",
    "LuxPowerConnectionError",
    "LuxPowerConnectionLostError",
    "LuxPowerError",
    "LuxPowerRecoveryExhaustedError",
    "LuxPowerReadRejectedError",
    "LuxPowerReadTimeoutError",
    "LuxPowerSessionClosedError",
    "LuxReadObservation",
    "LuxObservationSource",
    "LuxReadSession",
    "LuxReadSessionMetrics",
    "LuxReadSessionSnapshot",
    "LuxPowerReadClient",
    "LuxPowerObservationTimes",
    "LuxPowerTelemetry",
    "TelemetryGroup",
    "battery_register_group",
    "group_input_registers",
    "holding_register_group",
    "input_register_group",
    "input_registers_for_group",
    "EnergyFlowReadProfile",
    "EnergyFlowSnapshot",
    "GridTopology",
    "InputReadBlock",
    "LoadLayout",
    "ObservedProfileValue",
    "ProfileField",
    "ReadProfileName",
    "plan_aligned_input_blocks",
    "profile_block_details",
    "RecoveryEvent",
    "RecoveryFailureKind",
    "RecoveryMetrics",
    "RecoveryPolicy",
]
