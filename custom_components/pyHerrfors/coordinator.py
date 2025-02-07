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
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=datetime.timedelta(hours=1),
            update_method=self._async_update_data,
            always_update=True
        )

    def get_update_interval(self):
        now =  datetime.datetime.now()
        # Define morning hours (e.g., 6 AM to 10 AM)
        morning_start =  datetime.datetime(now.year, now.month, now.day, 7)
        morning_end =  datetime.datetime(now.year, now.month, now.day, 14)

        if morning_start <= now <= morning_end:
            return datetime.timedelta(minutes=15)  # More frequent updates in the morning
        else:
            return datetime.timedelta(hours=1)  # Default update interval

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
        if (len(getattr(self.api,"year_consumption")) == len(getattr(self.api,"year_prices")) and
                getattr(self.api,"latest_day") == (datetime.datetime.now().date() - datetime.timedelta(days=1))):
            _LOGGER.debug("Year consumption and latest day have been updated")
        else:
            try:
                _LOGGER.debug("Running update from API")
                await self.api.update_latest_month(poll_always=force)
                return self.api
            except Exception as err:
                raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def update_data(self):
        _LOGGER.info("Running update data from sensor")
        return await self._async_update_data()


    async def get_specific_day_consumption(self,date):
        _LOGGER.info(f"Fetching day {date} consumption from API")
        try:
            date_consumption = await self.api.get_specific_day_consumption(date)
            return date_consumption
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def force_check_latest_month(self):
        _LOGGER.info("Running force update from API")
        return await self._async_update_data(True)

    async def force_update_current_year(self, day_level=False):
        _LOGGER.info("Running force update from API for current year")

        try:
            await self.api.force_update_current_year(day_level=day_level)
            return
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

