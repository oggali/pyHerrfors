# custom_components/pyHerrfors/sensor.py
from homeassistant.components.sensor import SensorEntity
from homeassistant.core import callback
from homeassistant.helpers.event import async_track_time_change, async_track_utc_time_change, async_track_time_interval
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
import datetime
import logging
from .coordinator import HerrforsDataUpdateCoordinator
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.components.sensor import SensorStateClass, SensorDeviceClass
from homeassistant.const import UnitOfEnergy, CURRENCY_EURO
from .const import (SENSOR_TYPES,DOMAIN,CONF_EMAIL, CONF_PASSWORD, CONF_MARGINAL_PRICE, CONF_API_KEY)
from datetime import timedelta

# SCAN_INTERVAL = timedelta(minutes=15)

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

    # define sensor update call function and interval or other related trigger
    async_track_time_interval(
        hass, coordinator.update_data, datetime.timedelta(minutes=15)
    )


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

        if sensor_type == "latest_day":
            self._attr_device_class = SensorDeviceClass.DATE

        if "price_euro" in sensor_type or "savings_eur" in sensor_type:
            self._attr_state_class = SensorStateClass.TOTAL
            self._attr_native_unit_of_measurement = CURRENCY_EURO
        elif "consumption" in sensor_type:
            self._attr_state_class = SensorStateClass.TOTAL
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_device_class = SensorDeviceClass.ENERGY
        elif "avg" in sensor_type and "price" in sensor_type:
            self._attr_state_class = SensorStateClass.TOTAL
            self._attr_native_unit_of_measurement = "c/kWh"
        else:
            self._attr_native_unit_of_measurement = None



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

        super().__init__(coordinator)
        # self.coordinator.update_data()
        # self._attr_native_value = self.native_value


    @property
    def name(self):
        """Return the name of the sensor."""
        return f"{DOMAIN} {SENSOR_TYPES[self._sensor_type]}"

    @property
    def native_value(self):
        """Return the state of the sensor."""
        snapshot = self.coordinator.snapshot
        value = snapshot.get_sensor_value(self._sensor_type)
        if value is not None:
            return value
        data = self.coordinator.data
        if data is not None:
            return getattr(data, self._sensor_type, None)
        return None

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        return self.coordinator.last_update_success

    async def async_added_to_hass(self):
        """When entity is added to hass."""
        self.async_on_remove(
            self.coordinator.async_add_listener(self.async_write_ha_state)
        )

    def update(self):
        self.coordinator.async_request_refresh()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        _LOGGER.debug(f"Sensor {self._sensor_type} _handle_coordinator_update function call")
        self.async_write_ha_state()

    @property
    def should_poll(self):
        """No polling needed."""
        return False

    @property
    def extra_state_attributes(self):
        attributes = {}
        data = self.coordinator.data
        snapshot = self.coordinator.snapshot
        day_group = (
            snapshot.day_group_calculations
            if snapshot is not None
            else getattr(data, "day_group_calculations", None)
        )
        month_group = (
            snapshot.month_group_calculations
            if snapshot is not None
            else getattr(data, "month_group_calculations", None)
        )
        day_detail = (
            snapshot.latest_day_detail
            if snapshot is not None
            else getattr(data, "latest_day_electricity_price_consumption_calculations", None)
        )

        if self._sensor_type == "latest_day" and day_group is not None:
            attributes['day'] = day_group.reset_index()['timestamp_tz'].apply(lambda x:x.isoformat()).to_list()
            attributes['consumption_sum'] = day_group['consumption_sum'].to_list()
            attributes['avg_khw_price_with_alv'] = day_group['day_avg_khw_price_with_alv'].to_list()
            attributes['electricity_price_euro'] = day_group['price_marg_alv_euro_sum'].to_list()
            attributes['avg_spot_price_with_vat'] = day_group['prices_cent_vat_avg'].to_list()

            if day_detail is not None:
                attributes['timestamp_tz'] = day_detail['timestamp_tz'].apply(lambda x:x.isoformat()).to_list()
                attributes['consumption'] = day_detail['consumption'].to_list()
                attributes['price_marginal_alv'] = day_detail['price_marginal_alv'].to_list()
                attributes['consumption_price_marginal_alv'] = day_detail['price_marg_alv'].to_list()

        if self._sensor_type == "latest_month" and month_group is not None:
            attributes['month_group_calculations'] = month_group.to_json(orient='records')
            attributes['month'] = month_group.reset_index()['timestamp_tz'].apply(lambda x: x.isoformat()).to_list()
            attributes['consumption_sum'] = month_group['consumption_sum'].to_list()
            attributes['avg_khw_price_with_alv'] = month_group['month_avg_khw_price_with_alv'].to_list()
            attributes['electricity_price_euro'] = month_group['price_marg_alv_euro_sum'].to_list()
            attributes['avg_spot_price_with_vat'] = month_group['prices_cent_vat_avg'].to_list()

        return attributes