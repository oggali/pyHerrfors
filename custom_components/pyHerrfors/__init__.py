"""The pyherrfors integration."""
# custom_components/pyHerrfors/__init__.py
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import (DOMAIN,CONF_USAGE_PLACE, CONF_CUSTOMER_NUMBER, CONF_MARGINAL_PRICE, CONF_API_KEY)
from .coordinator import HerrforsDataUpdateCoordinator
from .services import async_setup_services

_LOGGER = logging.getLogger(__name__)
PLATFORMS = [Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Home Assistant Herrfors electricity consumption from a config entry."""

    herrfors_coordinator = HerrforsDataUpdateCoordinator(hass, entry)

    # TODO 2. Validate the API connection (and authentication)

    # entry.runtime_data = MyAPI(...)

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = herrfors_coordinator

    await herrfors_coordinator.async_config_entry_first_refresh()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    async_setup_services(hass)

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok

