# custom_components/pyHerrfors/coordinator.py
from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
import datetime
import logging
from .client import Herrfors

from .const import (SENSOR_TYPES,DOMAIN,CONF_USAGE_PLACE, CONF_CUSTOMER_NUMBER, CONF_MARGINAL_PRICE, CONF_API_KEY)

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
_LOGGER.setLevel(logging.INFO)

class HerrforsDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching data from the API."""

    def __init__(self, hass, config_entry):
        """Initialize the data updater."""
        self.config_entry = config_entry
        self.api = Herrfors(
            usage_place=config_entry.data[CONF_USAGE_PLACE],
            customer_number=config_entry.data[CONF_CUSTOMER_NUMBER],
            apikey=config_entry.data.get(CONF_API_KEY, None),
            marginal_price=config_entry.data.get(CONF_MARGINAL_PRICE, 0)
        )
        # update_interval = datetime.timedelta(days=1)
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=datetime.timedelta(hours=1),
            # update_method=self._async_update_data,
            always_update=False
        )

    async def _async_setup(self):
        """Set up the coordinator

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """
        self._device = await self.api.login()
        await self.api.logout()

    async def _async_update_data(self,force=False):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up data.
        """
        try:
            _LOGGER.info("Running update from API")
            await self.api.update_latest_month(poll_always=force)
            return self.api
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def get_specific_day_consumption(self,date):
        _LOGGER.info(f"Fetching day {date} consumption from API")
        try:
            date_consumption = await self.api.get_specific_day_consumption(date)
            return date_consumption
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def force_check_latest_month(self):
        _LOGGER.info("Running force update from API")
        try:
            return await self._async_update_data(True)
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err
