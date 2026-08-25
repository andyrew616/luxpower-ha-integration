"""Home Assistant adapter for the reusable LuxPower client layer."""

import asyncio
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr, entity_registry as er

from .classes.modbus_client import LxpModbusApiClient
from .const import (
    CONF_BATTERY_ENTITIES,
    CONF_CONNECTION_RETRIES,
    CONF_DONGLE_SERIAL,
    CONF_HOST,
    CONF_INVERTER_SERIAL,
    CONF_POLL_INTERVAL,
    CONF_PORT,
    CONF_READ_ONLY,
    CONF_REGISTER_BLOCK_SIZE,
    DEFAULT_BATTERY_ENTITIES,
    DEFAULT_CONNECTION_RETRIES,
    DEFAULT_READ_ONLY,
    DEFAULT_REGISTER_BLOCK_SIZE,
    DOMAIN,
)
from .coordinator import LxpModbusDataUpdateCoordinator
from .ha_const import PLATFORMS

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up the LuxPower Modbus component from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    host = entry.data[CONF_HOST]
    port = entry.data[CONF_PORT]
    dongle_serial = entry.data[CONF_DONGLE_SERIAL]
    inverter_serial = entry.data[CONF_INVERTER_SERIAL]
    poll_interval = entry.data[CONF_POLL_INTERVAL]

    battery_entities = entry.data.get(
        CONF_BATTERY_ENTITIES, DEFAULT_BATTERY_ENTITIES
    ).replace(" ", "").split(",")
    request_battery_data = bool(battery_entities) and "none" not in battery_entities
    battery_serials_configured = any(
        value not in ("", "none", "auto") for value in battery_entities
    )

    lock = asyncio.Lock()
    block_size = entry.data.get(
        CONF_REGISTER_BLOCK_SIZE, DEFAULT_REGISTER_BLOCK_SIZE
    )
    connection_retries = entry.data.get(
        CONF_CONNECTION_RETRIES, DEFAULT_CONNECTION_RETRIES
    )
    api_client = LxpModbusApiClient(
        host,
        port,
        dongle_serial,
        inverter_serial,
        lock,
        block_size,
        connection_retries,
        request_battery_data=request_battery_data,
        battery_serials_configured=battery_serials_configured,
    )

    coordinator = LxpModbusDataUpdateCoordinator(
        hass,
        entry,
        api_client,
        poll_interval,
    )

    hass.data[DOMAIN][entry.entry_id] = {
        "coordinator": coordinator,
        "settings": {**entry.data, **entry.options},
        "lock": lock,
        "write_lock": asyncio.Lock(),
        "api_client": api_client,
    }

    await coordinator.async_config_entry_first_refresh()

    settings = hass.data[DOMAIN][entry.entry_id]["settings"]
    is_read_only = settings.get(CONF_READ_ONLY, DEFAULT_READ_ONLY)

    if is_read_only:
        _LOGGER.info(
            "Read-only mode enabled. Loading sensor and binary_sensor platforms only."
        )
        platforms_to_load = [Platform.SENSOR, Platform.BINARY_SENSOR]
    else:
        platforms_to_load = PLATFORMS

    hass.data[DOMAIN][entry.entry_id]["platforms"] = platforms_to_load
    await hass.config_entries.async_forward_entry_setups(entry, platforms_to_load)

    if not is_read_only:
        _async_prune_empty_devices(hass, entry)

    return True


@callback
def _async_prune_empty_devices(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Drop sub-devices left behind when a device_group is renamed or removed."""
    dev_reg = dr.async_get(hass)
    ent_reg = er.async_get(hass)
    main_device_id = (DOMAIN, entry.entry_id)

    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        if main_device_id in device.identifiers:
            continue
        if er.async_entries_for_device(
            ent_reg, device.id, include_disabled_entities=True
        ):
            continue
        _LOGGER.debug("Removing empty sub-device '%s'", device.name)
        dev_reg.async_remove_device(device.id)


async def async_remove_config_entry_device(
    hass: HomeAssistant, entry: ConfigEntry, device: dr.DeviceEntry
) -> bool:
    """Let the user delete a sub-device from the UI once it has no entities left."""
    ent_reg = er.async_get(hass)
    return not er.async_entries_for_device(
        ent_reg, device.id, include_disabled_entities=True
    )


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    loaded_platforms = hass.data[DOMAIN][entry.entry_id].get("platforms", PLATFORMS)
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, loaded_platforms
    )

    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok
