# custom_components/pyherrfors/config_flow.py
from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from .const import (DOMAIN,CONF_USAGE_PLACE, CONF_CUSTOMER_NUMBER, CONF_MARGINAL_PRICE, CONF_API_KEY)
from .client import Herrfors

_LOGGER = logging.getLogger(__name__)

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USAGE_PLACE): str, # usage_place
        vol.Required(CONF_CUSTOMER_NUMBER): str,  # customer_number
        vol.Optional(CONF_MARGINAL_PRICE): float, # marginal_price
        vol.Optional(CONF_API_KEY): str, # entso-e api key
    }
)


class PlaceholderHub:
    """Placeholder class to make tests pass.
    """

    def __init__(self, usage_place, customer_number) -> None:
        """Initialize."""

        self.host = Herrfors(usage_place, customer_number)

    async def authenticate(self,usage_place, customer_number) -> bool:
        """Test if we can authenticate with the host."""
        auth = await self.host.login(usage_place, customer_number)
        await self.host.logout()
        return auth


async def validate_input(hass: HomeAssistant, data: dict[str, Any]) -> dict[str, Any]:
    """Validate the user input allows us to connect.

    Data has the keys from STEP_USER_DATA_SCHEMA with values provided by the user.
    """

    hub = PlaceholderHub(data[CONF_USAGE_PLACE], data[CONF_CUSTOMER_NUMBER])

    if not await hub.authenticate(data[CONF_USAGE_PLACE], data[CONF_CUSTOMER_NUMBER]):
        raise InvalidAuth

    # If you cannot connect:
    # throw CannotConnect
    # If the authentication is wrong:
    # InvalidAuth

    # Return info that you want to store in the config entry.
    return {"title": "Herrfors"}


class HerrforsConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Home Assistant Herrfors electricity consumption."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                info = await validate_input(self.hass, user_input)
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except Exception:
                _LOGGER.exception("Unexpected exception")
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(title=info["title"], data=user_input)

        return self.async_show_form(
            step_id="user", data_schema=STEP_USER_DATA_SCHEMA, errors=errors
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""

