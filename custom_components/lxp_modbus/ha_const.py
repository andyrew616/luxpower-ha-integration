"""Constants specific to the Home Assistant adapter."""

from typing import Final

from homeassistant.const import Platform

PLATFORMS: Final = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.NUMBER,
    Platform.TIME,
    Platform.SELECT,
    Platform.BUTTON,
    Platform.SWITCH,
]
