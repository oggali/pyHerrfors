"""Constants for the pyherrfors integration."""
# custom_components/pyHerrfors/const.py
from datetime import timedelta

DEFAULT_SCAN_INTERVAL = timedelta(hours=1)
DOMAIN = "pyherrfors"
SENSOR_TYPES = {
    "latest_day": "Latest Day",
    "latest_day_electricity_consumption_sum": "Latest Day Electricity Consumption sum",
    "latest_day_electricity_price_euro": "Latest Day Electricity Price Euro",
    "latest_day_avg_khw_price_with_vat": "Latest Day avg khw price with vat",
    "latest_day_optimization_savings_eur": "Latest Day optimization_savings_eur",
    "latest_day_optimization_efficiency": "Latest Day optimization_efficiency",
    "latest_day_avg_price_with_vat": "Latest Day avg price with vat",
    "latest_day_avg_price_by_avg_spot": "Latest Day avg price by avg spot",
    "latest_day_avg_spot_price_with_vat": "Latest Day avg spot price with vat",
    "latest_month": "Latest Month",
    "latest_month_electricity_consumption": "Latest Month electricity consumption",
    "latest_month_electricity_price_euro": "Latest Month electricity price euro",
    "latest_month_optimization_savings_eur": "Latest Month optimization savings euro",
    "latest_month_optimization_efficiency": "Latest Month optimization efficiency",
    "latest_month_avg_price_with_vat": "Latest Month avg price with vat",
    "latest_month_avg_price_by_avg_spot": "Latest Month avg price by avg spot",
    "latest_month_avg_spot_price_with_vat": "Latest Month avg spot price with vat",
    "latest_month_avg_khw_price_with_vat": "Latest Month avg khw price with vat"
}

CONF_USAGE_PLACE = "Usage_Place"
CONF_CUSTOMER_NUMBER = "Customer_meter_number"
CONF_MARGINAL_PRICE = "Electricity_Marginal_Price"
CONF_API_KEY = " Entso-E api_key"