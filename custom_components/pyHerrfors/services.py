"""PyHerrfors service"""
# custom_components/pyHerrfors/coordinator.py

from __future__ import annotations

import logging
from datetime import date, datetime
from functools import partial
from typing import Final
from homeassistant.helpers import config_validation as cv, entity_platform

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
    callback,
)
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import selector
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import HerrforsDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


ATTR_DATE: Final = "Date"

ENERGY_PRICES_SERVICE_NAME: Final = "get_consumed_energy_and_prices"
FORCE_CHECK_LATEST_MONTH = "force_check_latest_month"

SERVICE_SCHEMA: Final = vol.Schema(
    {
        vol.Required(ATTR_DATE): str
    }
)

# async def async_register_services(hass):
#     """Register public services."""
#     platform = entity_platform.async_get_current_platform()
#
#     platform.async_register_entity_service(
#         ENERGY_PRICES_SERVICE_NAME,
#         SERVICE_SCHEMA,
#         "get_specific_day_consumption",
#     )

    # hass.services.async_register(
    #     DOMAIN,
    #     SERVICE_SET_SMARTHOME_MODE,
    #     set_smarthome_mode,
    #     SERVICE_SCHEMA,
    # )

def __get_date(date_input: str | None) -> date | datetime:
    """Get date."""
    if not date_input:
        return dt_util.now()

    if value := dt_util.parse_datetime(date_input):
        return value

    raise ServiceValidationError(
        translation_domain=DOMAIN,
        translation_key="invalid_date",
        translation_placeholders={
            "date": date_input,
        },
    )

#
# def __serialize_prices(prices) -> ServiceResponse:
#     """Serialize prices."""
#     return {
#         "prices": [
#             {"timestamp": dt.isoformat(), "price": price}
#             for dt, price in prices.items()
#         ]
#     }
#
#
def __get_coordinator(hass: HomeAssistant, call: ServiceCall) -> HerrforsDataUpdateCoordinator:
    """Get the coordinator from the entry."""
    # entry_id: str = call.data[ATTR_CONFIG_ENTRY]
    config_entry = hass.config_entries.async_entries(domain=DOMAIN)[0]
    entry: ConfigEntry | None = hass.config_entries.async_get_entry(config_entry.entry_id)
    coordinator = hass.data[DOMAIN][config_entry.entry_id]

    if not entry:
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="invalid_config_entry",
            translation_placeholders={
                "config_entry": config_entry.entry_id,
            },
        )
    if entry.state != ConfigEntryState.LOADED:
        raise ServiceValidationError(
            translation_domain=DOMAIN,
            translation_key="unloaded_config_entry",
            translation_placeholders={
                "config_entry": entry.title,
            },
        )

    # coordinator: HerrforsDataUpdateCoordinator = hass.data[DOMAIN][entry_id]
    return coordinator


async def __get_specific_day_consumption(    call: ServiceCall,
    *,
    hass: HomeAssistant
) -> ServiceResponse:
    # config_entry = hass.config_entries.async_entries(domain=DOMAIN)[0]
    coordinator = __get_coordinator(hass, call)
    __date = __get_date(call.data.get(ATTR_DATE))

    data_ = await coordinator.get_specific_day_consumption(__date)

    return data_

async def __force_check_latest_month(    call: ServiceCall,
    *,
    hass: HomeAssistant
) -> ServiceResponse:

    coordinator = __get_coordinator(hass, call)

    await coordinator.force_check_latest_month()

    return

@callback
def async_setup_services(hass: HomeAssistant) -> None:
    """Set up Herrfors services."""
    # config_entry =  hass.config_entries.async_entries(domain=DOMAIN)
    hass.services.async_register(
        DOMAIN,
        ENERGY_PRICES_SERVICE_NAME,
        partial(__get_specific_day_consumption, hass=hass),
        schema=SERVICE_SCHEMA,
    )

    hass.services.async_register(
        DOMAIN,
        FORCE_CHECK_LATEST_MONTH,
        partial(__force_check_latest_month, hass=hass),

    )
