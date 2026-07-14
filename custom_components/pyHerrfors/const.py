"""Constants for the pyherrfors integration."""
# custom_components/pyHerrfors/const.py
import datetime

DOMAIN = "pyherrfors"

# Portal API
PORTAL_BASE_URL = "https://portal.herrfors.fi"
PORTAL_READINGS_URL = f"{PORTAL_BASE_URL}/api/charts/readings"
PORTAL_CHARTS_REFERER = f"{PORTAL_BASE_URL}/fi-FI/charts"
PORTAL_CLIENT_ID = "6212c91e-f646-4a74-b3ce-38a4a3df2d9d"
CO_ID = "60754370"

# Data resolution cutoff (YYYYMMDD) — 60-min before, 15-min after
RESOLUTION_CUTOFF_DATE = 20251001

# Latest available consumption day
LATEST_DAY_CUTOFF_HOUR = 6

# Finnish VAT schedule
NORMAL_VAT = 0.255
EARLIER_NORMAL_VAT = 0.24
DISCOUNT_VAT = 0.1
DISCOUNT_VAT_START = "2022-12-1"
DISCOUNT_VAT_END = "2023-4-30"
NEW_NORMAL_VAT_START = "2024-09-1"

# Storage paths (overridable via env in db.py / session.py)
DEFAULT_TOKEN_FILE = "/share/herrfors_token.json"
DEFAULT_DB_FILE = "/share/herrfors_data.db"


def days_later_for_latest(now=None):
    if now is None:
        now = datetime.datetime.now()
    return 2 if now.hour < LATEST_DAY_CUTOFF_HOUR else 1


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

# CONF_USAGE_PLACE = "Usage_Place"
# CONF_CUSTOMER_NUMBER = "Customer_meter_number"
CONF_EMAIL = "Email"
CONF_PASSWORD = "<PASSWORD>"
CONF_MARGINAL_PRICE = "Electricity_Marginal_Price"
CONF_API_KEY = " Entso-E api_key"