"""Lazy compatibility exports for Home Assistant-independent LuxPower clients.

Production users of the qualified FC4 path import :mod:`luxpower.qualified`.
Keeping these historic root exports lazy prevents that supported import from
loading the repository's separate legacy transport or write-capable modules.
"""

from importlib import import_module


_EXPORT_MODULES = {
    "custom_components.lxp_modbus.classes.read_client": (
        "LuxPowerReadClient",
        "LuxPowerTelemetry",
    ),
    "custom_components.lxp_modbus.observation": ("LuxPowerObservationTimes",),
    "custom_components.lxp_modbus.telemetry_groups": (
        "TelemetryGroup",
        "battery_register_group",
        "group_input_registers",
        "holding_register_group",
        "input_register_group",
        "input_registers_for_group",
    ),
    "custom_components.lxp_modbus.exceptions": (
        "LuxPowerAmbiguousRequestError",
        "LuxPowerCommunicationError",
        "LuxPowerConnectionError",
        "LuxPowerConnectionLostError",
        "LuxPowerError",
        "LuxPowerRecoveryExhaustedError",
        "LuxPowerReadRejectedError",
        "LuxPowerReadTimeoutError",
        "LuxPowerSessionClosedError",
    ),
    "custom_components.lxp_modbus.classes.read_session": (
        "LuxObservationSubscription",
        "LuxObservationSubscriptionSnapshot",
        "LuxObservationSource",
        "LuxReadObservation",
        "LuxReadSession",
        "LuxReadSessionMetrics",
        "LuxReadSessionSnapshot",
    ),
    "custom_components.lxp_modbus.read_profiles": (
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
    ),
    "custom_components.lxp_modbus.recovery": (
        "AcquisitionHealth",
        "RecoveryEvent",
        "RecoveryFailureKind",
        "RecoveryMetrics",
        "RecoveryPolicy",
    ),
    "custom_components.lxp_modbus.timeout_diagnostics": (
        "LuxDiagnosticEventKind",
        "LuxReadDiagnosticEvent",
        "LuxReadDiagnosticsSnapshot",
        "LuxReadPurpose",
        "LuxReadRequestContext",
        "LuxReadRequestDiagnostic",
        "LuxReadRequestOutcome",
        "LuxTimeoutDiagnosticEpisode",
    ),
}

_EXPORT_INDEX = {
    name: module_name
    for module_name, names in _EXPORT_MODULES.items()
    for name in names
}


def __getattr__(name: str):
    """Resolve historic root exports without loading them on package import."""
    module_name = _EXPORT_INDEX.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))

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
    "LuxObservationSubscription",
    "LuxObservationSubscriptionSnapshot",
    "LuxObservationSource",
    "LuxReadSession",
    "LuxReadSessionMetrics",
    "LuxReadSessionSnapshot",
    "LuxDiagnosticEventKind",
    "LuxReadDiagnosticEvent",
    "LuxReadDiagnosticsSnapshot",
    "LuxReadPurpose",
    "LuxReadRequestContext",
    "LuxReadRequestDiagnostic",
    "LuxReadRequestOutcome",
    "LuxTimeoutDiagnosticEpisode",
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
