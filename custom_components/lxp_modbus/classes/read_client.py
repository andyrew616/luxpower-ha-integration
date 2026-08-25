"""Home Assistant-independent, read-only LuxPower client API."""

import asyncio
from dataclasses import dataclass
from typing import Mapping

from ..const import (
    DEFAULT_CONNECTION_RETRIES,
    DEFAULT_PORT,
    DEFAULT_REGISTER_BLOCK_SIZE,
)
from .modbus_client import LxpModbusApiClient

RegisterMap = Mapping[int, int]
BatteryRegisterMap = Mapping[int | str, int | str]


@dataclass(frozen=True)
class LuxPowerTelemetry:
    """Decoded LuxPower register data returned by a single polling cycle."""

    input_registers: RegisterMap
    holding_registers: RegisterMap
    batteries: Mapping[str, BatteryRegisterMap]

    @classmethod
    def from_register_data(cls, data: dict) -> "LuxPowerTelemetry":
        """Create a detached telemetry snapshot from the protocol client's data."""
        return cls(
            input_registers=dict(data.get("input", {})),
            holding_registers=dict(data.get("hold", {})),
            batteries={
                serial: dict(registers)
                for serial, registers in data.get("battery", {}).items()
            },
        )


class LuxPowerReadClient:
    """Supported read-only facade over the existing LuxPower protocol client.

    The facade deliberately exposes no register-write operation. Each call uses the
    existing polling, TCP lifecycle, retry, validation, recovery, and cache behavior.
    """

    __slots__ = ("_client",)

    def __init__(
        self,
        host: str,
        dongle_serial: str,
        inverter_serial: str,
        *,
        port: int = DEFAULT_PORT,
        block_size: int = DEFAULT_REGISTER_BLOCK_SIZE,
        connection_retries: int = DEFAULT_CONNECTION_RETRIES,
        skip_initial_data: bool = True,
        request_battery_data: bool = False,
        battery_serials_configured: bool = False,
    ) -> None:
        """Initialize a read-only client for one LuxPower inverter."""
        self._client = LxpModbusApiClient(
            host=host,
            port=port,
            dongle_serial=dongle_serial,
            inverter_serial=inverter_serial,
            lock=asyncio.Lock(),
            block_size=block_size,
            connection_retries=connection_retries,
            skip_initial_data=skip_initial_data,
            request_battery_data=request_battery_data,
            battery_serials_configured=battery_serials_configured,
        )

    async def async_read(self) -> LuxPowerTelemetry:
        """Poll the inverter once and return a decoded telemetry snapshot."""
        data = await self._client.async_get_data()
        return LuxPowerTelemetry.from_register_data(data)
