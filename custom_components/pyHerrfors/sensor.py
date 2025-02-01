# custom_components/pyHerrfors/sensor.py
from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback
from homeassistant.helpers.event import async_track_time_change, async_track_utc_time_change
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
import datetime
import logging
from .coordinator import HerrforsDataUpdateCoordinator
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from .const import (SENSOR_TYPES,DOMAIN,CONF_USAGE_PLACE, CONF_CUSTOMER_NUMBER, CONF_MARGINAL_PRICE, CONF_API_KEY)

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback):
    """Set up the Herrfors sensor platform."""
    coordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities(
        HerrforsSensor(coordinator, sensor_type) for sensor_type in SENSOR_TYPES
    )

    await coordinator.async_config_entry_first_refresh()


    # # Schedule daily updates at 8 AM
    # async def async_update_at_8am(now):
    #     _LOGGER.debug("Scheduled update triggered at 8 AM")
    #     await coordinator.async_refresh()
    #
    # async_track_time_change(hass, async_update_at_8am, hour=6, minute=0, second=0)


class HerrforsSensor(CoordinatorEntity, SensorEntity):
    """Representation of a Herrfors sensor."""

    def __init__(self, coordinator: HerrforsDataUpdateCoordinator, sensor_type):
        self.coordinator = coordinator
        self._sensor_type = sensor_type
        # The Id used for addressing the entity in the ui, recorder history etc.
        self.entity_id = f"{DOMAIN}.{sensor_type}"
        # unique id in .storage file for ui configuration.
        self._attr_unique_id = f"{DOMAIN}.{sensor_type}"
        self._attr_name = f"{DOMAIN}.{sensor_type}"

        if sensor_type not in  ["latest_day","latest_month"]:
            self._attr_suggested_display_precision = 3

        self._attr_device_info = DeviceInfo(
            entry_type=DeviceEntryType.SERVICE,
            identifiers={
                (
                    DOMAIN,
                    f"{coordinator.config_entry.entry_id}",
                )
            },
            manufacturer="Herrfors",
            model="",
            name="Herrfors",
        )

        # self._update_job = HassJob(self.async_schedule_update_ha_state)
        self._unsub_update = None

        super().__init__(coordinator, sensor_type)

    @property
    def name(self):
        """Return the name of the sensor."""
        return f"{DOMAIN} {SENSOR_TYPES[self._sensor_type]}"

    @property
    def native_value(self):
        """Return the state of the sensor."""
        # _LOGGER.debug(f"Returning for {self._sensor_type} data {getattr(self.coordinator.data, self._sensor_type)} ")

        _LOGGER.info(f"Returning for {self._sensor_type} data {getattr(self.coordinator.data, self._sensor_type)} ")
        return getattr(self.coordinator.data, self._sensor_type)

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        self.async_on_remove(
            self.coordinator.async_add_listener(self.async_write_ha_state)
        )

    async def async_update(self):
        """Update the entity. Only used by the generic entity update service."""
        await self.coordinator.async_request_refresh()
        # await self.coordinator.update_data()

    @property
    def should_poll(self):
        """No polling needed."""
        return False

    @property
    def extra_state_attributes(self):
        attributes = {}
        # attributes = {'custom_extra_attribute':'testing_extra_attribute'}
        if self._sensor_type =="latest_day" and getattr(self.coordinator.data, 'day_group_calculations') is not None:
            attributes['day_group_calculations'] = getattr(self.coordinator.data, 'day_group_calculations').to_json(orient='records')

        if self._sensor_type =="latest_month" and getattr(self.coordinator.data, 'month_group_calculations') is not None:
            attributes['month_group_calculations'] = getattr(self.coordinator.data, 'month_group_calculations').to_json(orient='records')

        return attributes