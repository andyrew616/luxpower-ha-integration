"""Home Assistant entry points with lazy adapter imports.

Keeping Home Assistant imports behind these calls allows the reusable LuxPower
client modules to be imported in environments where Home Assistant is absent.
"""


async def async_setup_entry(hass, entry) -> bool:
    """Set up the LuxPower integration from a Home Assistant config entry."""
    from .ha_adapter import async_setup_entry as adapter_setup_entry

    return await adapter_setup_entry(hass, entry)


async def async_remove_config_entry_device(hass, entry, device) -> bool:
    """Delegate config-entry device removal to the Home Assistant adapter."""
    from .ha_adapter import (
        async_remove_config_entry_device as adapter_remove_config_entry_device,
    )

    return await adapter_remove_config_entry_device(hass, entry, device)


async def async_unload_entry(hass, entry) -> bool:
    """Unload a Home Assistant config entry through the adapter."""
    from .ha_adapter import async_unload_entry as adapter_unload_entry

    return await adapter_unload_entry(hass, entry)
