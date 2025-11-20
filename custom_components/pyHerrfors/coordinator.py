# custom_components/pyHerrfors/coordinator.py
from types import NoneType

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
import datetime
import os
import logging
from .client import Herrfors

from .const import (SENSOR_TYPES,DOMAIN,CONF_EMAIL, CONF_PASSWORD, CONF_MARGINAL_PRICE, CONF_API_KEY)

_LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
_LOGGER.setLevel(logging.INFO)


class HerrforsDataUpdateCoordinator(DataUpdateCoordinator):
    """Class to manage fetching data from the API."""

    def __init__(self, hass, config_entry):
        """Initialize the data updater."""

        os.environ["TOKEN_FILE"] = hass.config.path("../share/herrfors_token.json")
        os.environ["DB_FILE"] = hass.config.path("../share/herrfors_data.db")

        self.config_entry = config_entry
        self.api = Herrfors(
            email=config_entry.data[CONF_EMAIL],
            password=config_entry.data[CONF_PASSWORD],
            apikey=config_entry.data.get(CONF_API_KEY, None),
            marginal_price=config_entry.data.get(CONF_MARGINAL_PRICE, 0)
        )
        self.last_update = datetime.datetime.now()
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            # update_interval=datetime.timedelta(hours=10),
            update_method=self._async_update_data,
            always_update=False
        )

    def get_update_interval(self):
        now = datetime.datetime.now()
        # Define morning hours (e.g., 6 AM to 10 AM)
        morning_start = datetime.datetime(now.year, now.month, now.day, 7)
        morning_end = datetime.datetime(now.year, now.month, now.day, 11)

        if self.last_update is None:
            _LOGGER.info(f"update since there wasn't last updated")
            return True
        elif (len(getattr(self.api, "year_consumption", 1)) == len(getattr(self.api, "year_prices", 2)) and
                        getattr(self.api,"latest_day",datetime.datetime(1920,1,1)) == (datetime.datetime.now().date() - datetime.timedelta(days=1))):
            _LOGGER.info("Year consumption and latest day have been updated already")
            return False

        elif morning_start <= now <= morning_end:
            _LOGGER.info(f"update interval 15 min")
            return True
        else:
            if datetime.timedelta(minutes=50) < (now - self.last_update) < datetime.timedelta(hours=1, minutes=20) and now > morning_end:
                _LOGGER.info(f"update interval 1 hour")
                return True  # Default update interval
            else:
                _LOGGER.info(f"update only once in every hour")
                return False

    async def _async_setup(self):
        """Set up the coordinator

        This is the place to set up your coordinator,
        or to load data, that only needs to be loaded once.

        This method will be called automatically during
        coordinator.async_config_entry_first_refresh.
        """
        self._device = await self.api.login()
        await self.api.logout()

    async def _async_update_data(self,force=False,*_):
        """Fetch data from API endpoint.

        This is the place to pre-process the data to lookup tables
        so entities can quickly look up data.
        """
        try:
            _LOGGER.info("Try to update data from API")
            await self.api.update_latest_month(poll_always=force)
            self.last_update = datetime.datetime.now()
            self.async_update_listeners() # notifies all listeners(e.g. sensors) to update their data
            return self.api
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

    async def update_data(self,*_):
        _LOGGER.info("Running update data from sensor")
        if self.get_update_interval():
            _LOGGER.info("Refresh sensor data")
            await self._async_update_data()
            self.async_update_listeners()
            return self.api
        else:
            _LOGGER.info("Return api data")
            return self.api


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
            self.async_update_listeners()
            return self.api
        except Exception as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err

