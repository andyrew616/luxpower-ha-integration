"""Tests for the Home Assistant adapter boundary."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.const import Platform

from custom_components.lxp_modbus.const import (
    CONF_BATTERY_ENTITIES,
    CONF_CONNECTION_RETRIES,
    CONF_DONGLE_SERIAL,
    CONF_HOST,
    CONF_INVERTER_SERIAL,
    CONF_POLL_INTERVAL,
    CONF_PORT,
    CONF_READ_ONLY,
    CONF_REGISTER_BLOCK_SIZE,
    DOMAIN,
)
from custom_components.lxp_modbus.ha_adapter import (
    async_remove_config_entry_device,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.lxp_modbus.ha_const import PLATFORMS


@pytest.fixture
def hass():
    """Provide the Home Assistant calls used during config entry setup."""
    instance = MagicMock()
    instance.data = {}
    instance.config_entries.async_forward_entry_setups = AsyncMock()
    return instance


def make_entry(*, read_only=False):
    """Build a representative config entry."""
    entry = MagicMock()
    entry.entry_id = "entry-id"
    entry.data = {
        CONF_HOST: "192.0.2.1",
        CONF_PORT: 8000,
        CONF_DONGLE_SERIAL: "DG00000001",
        CONF_INVERTER_SERIAL: "0000000001",
        CONF_POLL_INTERVAL: 60,
        CONF_READ_ONLY: read_only,
        CONF_REGISTER_BLOCK_SIZE: 125,
        CONF_CONNECTION_RETRIES: 3,
        CONF_BATTERY_ENTITIES: "auto",
    }
    entry.options = {}
    return entry


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("read_only", "expected_platforms"),
    [
        (False, PLATFORMS),
        (True, [Platform.SENSOR, Platform.BINARY_SENSOR]),
    ],
)
async def test_setup_preserves_client_options_and_platform_selection(
    hass, read_only, expected_platforms
):
    """The extracted adapter must preserve the existing HA setup semantics."""
    entry = make_entry(read_only=read_only)
    api_client = MagicMock()
    coordinator = MagicMock()
    coordinator.async_config_entry_first_refresh = AsyncMock()

    with patch(
        "custom_components.lxp_modbus.ha_adapter.LxpModbusApiClient",
        return_value=api_client,
    ) as client_class, patch(
        "custom_components.lxp_modbus.ha_adapter.LxpModbusDataUpdateCoordinator",
        return_value=coordinator,
    ) as coordinator_class, patch(
        "custom_components.lxp_modbus.ha_adapter._async_prune_empty_devices"
    ) as prune:
        assert await async_setup_entry(hass, entry) is True

    client_class.assert_called_once()
    assert client_class.call_args.args[:4] == (
        "192.0.2.1",
        8000,
        "DG00000001",
        "0000000001",
    )
    assert client_class.call_args.args[5:] == (125, 3)
    assert client_class.call_args.kwargs == {
        "request_battery_data": True,
        "battery_serials_configured": False,
    }
    coordinator_class.assert_called_once_with(hass, entry, api_client, 60)
    coordinator.async_config_entry_first_refresh.assert_awaited_once_with()
    hass.config_entries.async_forward_entry_setups.assert_awaited_once_with(
        entry, expected_platforms
    )
    assert hass.data[DOMAIN][entry.entry_id]["api_client"] is api_client
    assert hass.data[DOMAIN][entry.entry_id]["platforms"] == expected_platforms
    assert prune.call_count == (0 if read_only else 1)


@pytest.mark.asyncio
async def test_package_entrypoints_delegate_to_ha_adapter():
    """Keep HA's conventional package entry points while imports remain lazy."""
    import custom_components.lxp_modbus as integration

    hass = MagicMock()
    entry = MagicMock()
    device = MagicMock()

    with patch(
        "custom_components.lxp_modbus.ha_adapter.async_setup_entry",
        AsyncMock(return_value=True),
    ) as setup, patch(
        "custom_components.lxp_modbus.ha_adapter.async_unload_entry",
        AsyncMock(return_value=True),
    ) as unload, patch(
        "custom_components.lxp_modbus.ha_adapter.async_remove_config_entry_device",
        AsyncMock(return_value=False),
    ) as remove:
        assert await integration.async_setup_entry(hass, entry) is True
        assert await integration.async_unload_entry(hass, entry) is True
        assert (
            await integration.async_remove_config_entry_device(hass, entry, device)
            is False
        )

    setup.assert_awaited_once_with(hass, entry)
    unload.assert_awaited_once_with(hass, entry)
    remove.assert_awaited_once_with(hass, entry, device)


@pytest.mark.asyncio
async def test_unload_uses_loaded_platforms_and_removes_runtime_data(hass):
    """Preserve the adapter's successful unload cleanup behavior."""
    entry = make_entry()
    loaded_platforms = [Platform.SENSOR]
    hass.data = {
        DOMAIN: {entry.entry_id: {"platforms": loaded_platforms}}
    }
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

    assert await async_unload_entry(hass, entry) is True

    hass.config_entries.async_unload_platforms.assert_awaited_once_with(
        entry, loaded_platforms
    )
    assert entry.entry_id not in hass.data[DOMAIN]


@pytest.mark.asyncio
@pytest.mark.parametrize(("has_entities", "expected"), [(False, True), (True, False)])
async def test_device_removal_requires_an_empty_device(has_entities, expected):
    """Preserve HA's protection against removing devices that own entities."""
    hass = MagicMock()
    entry = MagicMock()
    device = MagicMock()
    entity_registry = MagicMock()
    entities = [MagicMock()] if has_entities else []

    with patch(
        "custom_components.lxp_modbus.ha_adapter.er.async_get",
        return_value=entity_registry,
    ), patch(
        "custom_components.lxp_modbus.ha_adapter.er.async_entries_for_device",
        return_value=entities,
    ) as entries_for_device:
        result = await async_remove_config_entry_device(hass, entry, device)

    assert result is expected
    entries_for_device.assert_called_once_with(
        entity_registry, device.id, include_disabled_entities=True
    )
