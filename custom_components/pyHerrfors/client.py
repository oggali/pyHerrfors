# custom_components/pyHerrfors/client.py
import datetime
import pandas as pd
import asyncio
from entsoe import EntsoePandasClient
import functools
import logging
import json

from .const import (
    CO_ID,
    PORTAL_READINGS_URL,
    RESOLUTION_CUTOFF_DATE,
    SENSOR_TYPES,
    build_readings_params,
    consumption_params_for_day,
    days_later_for_latest,
    resolve_time_step,
)
from .dates import cached_dates, date_range, filter_date_range, upsert_by_date
from .db import get_all_from_db_as_df, insert_to_db
from .models import HerrforsSnapshot, PeriodSummary
from .pricing import add_vat_to_prices, apply_price_calculations, group_price_calculations
from .session import HerrforsSession

logger = logging.getLogger(__name__)

# Re-export for callers that imported from client
__all__ = ["Herrfors", "get_all_from_db_as_df"]


class Herrfors:

    def __init__(self, email, password, apikey=None, marginal_price=None):
        """

        """
        self.single_day_consumption = None
        self.email = email
        self.password = password
        self._portal_session = HerrforsSession(email, password)
        self.time_step = None
        self.apikey = apikey
        self.marginal_price = marginal_price
        self.resolution = None

        self.latest_day = None
        self.latest_month = None
        self._days_later = None
        self.consumption_params = None
        self.latest_day_electricity_consumption = None
        self.latest_day_electricity_prices = None

        self.latest_day_electricity_price_consumption_calculations = None
        self.latest_day_electricity_consumption_sum = None
        self.latest_day_electricity_price_euro = None
        self.latest_day_optimization_savings_eur = None
        self.latest_day_optimization_efficiency = None
        self.latest_day_avg_price_with_vat = None
        self.latest_day_avg_price_by_avg_spot = None
        self.latest_day_avg_spot_price_with_vat = None
        self.latest_day_avg_khw_price_with_vat = None

        self.latest_month_electricity_consumption = None
        self.latest_month_electricity_price_euro = None
        self.latest_month_optimization_savings_eur = None
        self.latest_month_optimization_efficiency = None
        self.latest_month_avg_price_with_vat = None
        self.latest_month_avg_price_by_avg_spot = None
        self.latest_month_avg_spot_price_with_vat = None
        self.latest_month_avg_khw_price_with_vat = None

        self.month_consumption = None
        self.month_prices = None

        self.year_consumption = None
        self.year_prices = None

        self.grouped_calculations = None

        self.month_group_calculations = None
        self.day_group_calculations = None
        self.year_group_calculations = None
        self.snapshot = HerrforsSnapshot()

    @property
    def session(self):
        return self._portal_session.session

    @property
    def session_token(self):
        return self._portal_session.session_token

    @property
    def login_time(self):
        return self._portal_session.login_time

    @property
    def toke_exp(self):
        return self._portal_session.token_exp

    @property
    def headers(self):
        return self._portal_session.headers

    async def __aexit__(self, *err):
        await self._portal_session.close()

    def get_session_token(self, email=None, password=None):
        return self._portal_session.get_session_token(email, password)

    async def logout(self):
        await self._portal_session.close()
        return

    def __getattr__(self, name):
        if name in SENSOR_TYPES:
            return self.snapshot.get_sensor_value(name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def _get_latest_day(self, update_self=True):
        """
        Determines the latest day to be considered based on the current hour and calculates related parameters.

        Calculates the date difference depending on whether the current hour is less than
        LATEST_DAY_CUTOFF_HOUR, determines two days prior; otherwise, one day prior. Constructs the date
        in string format and initializes related attributes for energy consumption and pricing.

        Attributes
        ----------
        latest_day: str
            The date of the day considered as latest in the format 'YYYY-MM-DD'.
        latest_day_params: dict
            Contains the year, month, and day components of the `latest_day`.
        latest_day_electricity_consumption: NoneType
            Initialized as None, meant to store electricity consumption data for the latest day.
        latest_day_electricity_prices: NoneType
            Initialized as None, meant to store electricity prices data for the latest day.

        :return: None
        """

        self._days_later = days_later_for_latest()

        # latest day check
        latest_day = datetime.date.today() - datetime.timedelta(days=self._days_later)
        previous_day = datetime.date.today() - datetime.timedelta(days=(self._days_later+1))
        if (self.latest_day is None or latest_day != self.latest_day) and update_self:

            self.latest_day = latest_day
            self.consumption_params = consumption_params_for_day(latest_day)
            if previous_day != latest_day - datetime.timedelta(days=1):
                self.consumption_params["previous_date-year"] = previous_day.year
                self.consumption_params["previous_date-month"] = previous_day.month
                self.consumption_params["previous_date-day"] = previous_day.day
                self.consumption_params["previous_date"] = previous_day.strftime("%Y-%m-%d")
            self.latest_day_electricity_consumption = None
            self.latest_day_electricity_prices = None
        return latest_day

    def _check_session(self):
        return self._portal_session.check_session()

    async def update_latest_day(self):
        """
        Updates the latest day consumption and calculates the
        average price for that day. If the user session is not
        established, it logs in to create a session before proceeding.

        :raises RuntimeError: If authentication fails during login,
            this will lead to an inability to establish a session for the
            operation.
        """

        if self._check_session():
            self._get_latest_day()
            # delete always from year_prices because fetching these again is fairly fast
            if self.year_prices is not None:
                if not self.year_prices.empty:
                    self.year_prices = self.year_prices[self.year_prices['date'] != self.latest_day]
            await self.calculate_avg_price(granularity='D')
            logger.info(f"Day {self.latest_day} Electricity consumption was {self.latest_day_electricity_consumption_sum} kWh"
                        f" Cost was {self.latest_day_electricity_price_euro} € with avg price {self.latest_day_avg_khw_price_with_vat} c/kWh")
            await self.logout()
        else:
            logger.info("Session and token is not valid, so we need to wait")

    async def force_update_current_year(self, day_level=False):
        self.year_prices = None
        self.year_consumption = None
        self.day_group_calculations = None
        self.month_group_calculations = None
        self.year_group_calculations = None
        logger.info("Force updating current year")

        self._get_latest_day()

        if self._check_session():
            calc_month = 10  # new client supports only from beginning of November
            import calendar
            while calc_month <= self.latest_day.month:

                res = calendar.monthrange(int(self.latest_day.year), month=int(calc_month))
                start_day = datetime.date(year=int(self.latest_day.year), month=int(calc_month), day=1)
                last_day = datetime.date(year=int(self.latest_day.year), month=int(calc_month), day=res[1])
                if calc_month == self.latest_day.month:
                    last_day = self.latest_day

                logger.info(f"Month {int(self.latest_day.year)}-{int(calc_month)} calculating dates between {start_day} and {last_day}")

                month_df, month_prices = await asyncio.gather(self.get_specific_month_consumption(start_day, last_day),
                                                              self.get_electricity_prices(self.apikey, start_day, last_day))

                if day_level:
                    logger.info("Update day level calculations also")

                    fetch_day = start_day

                    while fetch_day <= last_day:
                        logger.debug(f"Calculating day level avg for {fetch_day}")
                        if fetch_day not in month_df['timestamp_tz'].dt.date.values:
                            logger.info(f"day {fetch_day} not found yet from consumption info, so let's try again later")

                        else:

                            await self.calculate_avg_price(consumption=month_df[month_df['timestamp_tz'].dt.date == fetch_day],
                                                           prices=month_prices[month_prices['timestamp_tz'].dt.date == fetch_day],
                                                           granularity='D')
                        fetch_day = fetch_day + datetime.timedelta(days=1)

                if self.year_prices is not None:
                    self.year_prices = pd.concat([self.year_prices, month_prices], axis=0)
                else:
                    self.year_prices = month_prices

                await self.calculate_avg_price(consumption=month_df, prices=month_prices, granularity='ME')

                logger.info(
                    f"Month {int(self.latest_day.year)}-{int(calc_month)} Electricity consumption is {self.latest_month_electricity_consumption} kWh"
                    f" Cost is {self.latest_month_electricity_price_euro} € with avg price {self.latest_month_avg_khw_price_with_vat} c/kWh")

                calc_month += 1
            await self.update_latest_month(True)
            await self.calculate_avg_price(consumption=self.year_consumption, prices=self.year_prices, granularity='YE')

        await self.logout()

    async def update_latest_month(self, poll_always=False):
        latest_day = self._get_latest_day(update_self=False)

        if self.year_consumption is None:
            poll_always = True

        if self.year_consumption is not None:
            if latest_day not in self.year_consumption['date'].values and datetime.datetime.now().hour > 9:
                logger.info(f"Latest day {latest_day} not found from memory, so let's try to fetch it")
                poll_always = True

        if self.year_prices is not None and self.year_consumption is not None and not poll_always:
            # check if dataframes are same size, if not then there is data missing, and we can try to poll missing ones
            if len(self.year_consumption) < len(self.year_prices):
                logger.info(f"Prices df size:{len(self.year_prices)} Consumption df size:{len(self.year_consumption)}")
                logger.info("Let's try to fill missing days")
                poll_always = True

        if poll_always or (7 < datetime.datetime.now().hour <= 8):
            logger.info(f"It's now {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} and we start polling data.")

            if self._check_session():
                start_day = datetime.date(year=latest_day.year, month=latest_day.month, day=1)

                last_day = latest_day
                self.latest_month = f"{latest_day.year}/{latest_day.month}"

                # delete always from year_prices because fetching these again is fairly fast
                if self.year_prices is not None:
                    if not self.year_prices.empty:
                        self.year_prices = self.year_prices[self.year_prices['timestamp_tz'].dt.month.values != latest_day.month]

                logger.info(f"Month {self.latest_month} calculating dates between {start_day} and {last_day}")

                month_df, month_prices = await asyncio.gather(self.get_specific_month_consumption(start_day, last_day),
                                                              self.get_electricity_prices(self.apikey, start_day, last_day))
                await self.logout()
                if latest_day not in month_df['timestamp_tz'].dt.date.values:
                    logger.info(f"Latest day {latest_day} not found yet, so let's try again later")
                    self.latest_day = max(month_df['timestamp_tz'].dt.date.values)

                else:
                    logger.info(f"Update self.latest_day to {latest_day}")
                    self.latest_day = latest_day

                if month_prices is not None:
                    self.year_prices = pd.concat([self.year_prices, month_prices], axis=0)
                    month_price_size = len(month_prices)
                else:
                    self.year_prices = month_prices
                    month_price_size = 0

                self.month_consumption = month_df
                self.month_prices = month_prices

                logger.info(f"Month {self.latest_month} prices df size is {month_price_size} and consumption df size is {len(month_df)}")

                await self.calculate_avg_price(consumption=month_df, prices=month_prices, granularity='ME')

                logger.info(
                    f"Month {self.latest_month} Electricity consumption is {self.latest_month_electricity_consumption} kWh"
                    f" Cost is {self.latest_month_electricity_price_euro} € with avg price {self.latest_month_avg_khw_price_with_vat} c/kWh")

                if month_prices is None and self.month_prices is not None:
                    month_prices = self.month_prices
                    self.year_prices = pd.concat([self.year_prices, month_prices], axis=0)

                # filter self.latest_day prices and consumption from months dataframes
                self.latest_day_electricity_prices = month_prices[month_prices['timestamp_tz'].dt.date == self.latest_day]
                self.latest_day_electricity_consumption = month_df[month_df['timestamp_tz'].dt.date == self.latest_day]
                await self.calculate_avg_price(consumption=self.latest_day_electricity_consumption,
                                               prices=self.latest_day_electricity_prices,
                                               granularity='D')
                logger.info(
                    f"Day {self.latest_day} Electricity consumption was {self.latest_day_electricity_consumption_sum} kWh"
                    f" Cost was {self.latest_day_electricity_price_euro} € with avg price {self.latest_day_avg_khw_price_with_vat} c/kWh")
                logger.info(f"day group size: {len(self.day_group_calculations)} month group size: {len(self.month_group_calculations)}")

                await self.calculate_avg_price(consumption=self.year_consumption, prices=self.year_prices, granularity='YE')

                await self.logout()

        else:
            logger.info(f"We dont' poll data from API every hour, it's now {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return

    async def get_latest_day_consumption(self):

        return await self.get_consumption(None)

    async def get_specific_month_consumption(self, start_day=None, last_day=None, month=None):

        if start_day is None:
            import calendar
            year, month_num = map(int, month.split("/"))
            res = calendar.monthrange(year, month=month_num)
            start_day = datetime.date(year=year, month=month_num, day=1)
            last_day = datetime.date(year=year, month=month_num, day=res[1])

        known_dates = cached_dates(self.year_consumption)
        missing_days = [
            day for day in date_range(start_day, last_day) if day not in known_dates
        ]

        if missing_days:
            results = await asyncio.gather(
                *(self.get_specific_day_consumption(day) for day in missing_days)
            )
            self.year_consumption = upsert_by_date(self.year_consumption, results)

        return filter_date_range(self.year_consumption, start_day, last_day)

    async def get_specific_day_consumption(self, date):

        if not isinstance(date, datetime.date):
            date = datetime.date(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]))

        self.consumption_params = consumption_params_for_day(date)

        if date < datetime.date.today():

            return await self.get_consumption(self.consumption_params)
        else:
            raise ValueError

    async def get_consumption(self, params=None):
        """
        Returns consumption based on parameters provided, if no parameters are provided then newest daily ones are returned
        :param params: the parameters to pass to the request,
        give date-year to get monthly consumption for given year,
        give date-month to get daily consumptions for given month and year,
        date-day to get daily hourly consumption

        :return: the consumption data
        """

        params_given = True
        granularity = ''
        if params is None:
            self._get_latest_day()

            params = self.consumption_params
            logger.info(f"No parameter given, get {self.latest_day} days consumption")
            params_given = False

        else:
            if params.get('date-day', 'false') != 'false':
                get_param = f"{params.get('date-year')}-{params.get('date-month')}-{params.get('date-day')} date"
                granularity = 'D'

            elif params.get('date-month', 'false') != 'false':
                get_param = f"{params.get('date-year')}-{params.get('date-month')} month"
                granularity = 'M'
            else:
                get_param = f"{params.get('date-year')} year"
                granularity = 'Y'
            logger.info(f"Getting consumption for {params.get('date')}")

        if granularity == "D" or not params_given:
            day = datetime.date(
                int(params.get("date-year")),
                int(params.get("date-month")),
                int(params.get("date-day")),
            )
            time_step = resolve_time_step(day)
            self.resolution = time_step
            logger.info(f"Time step used is {time_step}")

            if params.get("previous_date-year", None) is None:
                previous_day = day - datetime.timedelta(days=1)
                params["previous_date-year"] = previous_day.year
                params["previous_date-month"] = previous_day.month
                params["previous_date-day"] = previous_day.day
                params["previous_date"] = previous_day.strftime("%Y-%m-%d")

            requests_params = build_readings_params(
                day,
                previous_day=datetime.date(
                    int(params.get("previous_date-year")),
                    int(params.get("previous_date-month")),
                    int(params.get("previous_date-day")),
                ),
            )
        else:
            time_step = resolve_time_step(
                datetime.date.fromisoformat(params.get("date"))
            )
            self.resolution = time_step
            logger.info(f"Time step used is {time_step}")
            requests_params = {
                "coId": CO_ID,
                "from": f"{params.get('previous_date', params.get('date'))}T22:00:00.000Z",
                "to": f"{params.get('date')}T22:00:00.000Z",
                "price": "true",
                "temp": "false",
                "timeStep": str(time_step),
            }

        url = PORTAL_READINGS_URL

        logger.info(f"Parameters to request: {requests_params}")

        if self._check_session():

            r = await self.session.get(url, headers=self.headers, params=requests_params, timeout=30)

            r_txt = await r.text()
            if r.status != 200:
                raise EnvironmentError(r)

            data = json.loads(r_txt)
            consumption_data = data['values']

            logger.debug(f"Get data: {consumption_data}")

            # sum elements inside list for check
            check_sum = len(consumption_data)

            consumption_data_df = pd.DataFrame(consumption_data)

            # transform date column from str to timestamp
            consumption_data_df['timestamp_tz'] = pd.to_datetime(consumption_data_df['date']).apply(lambda x: x.tz_convert('EET'))

            # convert timestamp timezone to EET and extract date only to own column
            consumption_data_df['date'] = consumption_data_df['timestamp_tz'].apply(lambda x: x.tz_convert('EET').date())

            if check_sum > 0:

                # Validate consumption data matches 96 values for time step 15 and 24 values for time step 60
                if len(consumption_data_df) < 96 and time_step == 15 and granularity == 'D':
                    raise ValueError(
                        f"Invalid consumption data. Expected exactly 96 or 24 hourly consumption values for {consumption_data_df}")

                elif len(consumption_data_df) < 24 and time_step == 60 and granularity == 'D':
                    raise ValueError(
                        f"Invalid consumption data. Expected exactly 96 or 24 hourly consumption values for {consumption_data_df}")

                else:
                    consumption_df = consumption_data_df

                if not params_given:

                    self.latest_day_electricity_consumption = consumption_df
                else:

                    if granularity == 'D':

                        self.single_day_consumption = consumption_df
                    elif granularity == 'M':
                        # todo handle month days
                        consumption_df = pd.DataFrame({'days': [], 'consumption': consumption_data_df})

                    else:
                        # todo handle year months
                        consumption_df = pd.DataFrame({'months': [], 'consumption': consumption_data_df})

                return consumption_df
            else:
                logger.info(f"No data available for given parameters: {params}")
                if granularity == 'D':
                    empty_columns = ['timestamp_tz', 'consumption']
                    if self.single_day_consumption is not None:
                        empty_columns = self.single_day_consumption.columns
                    self.single_day_consumption = pd.DataFrame(columns=empty_columns)

                return self.single_day_consumption
        else:
            logger.info(f"No session available")

    async def get_specific_day_avg_price(self, date):

        await self.get_specific_day_consumption(date)
        return await self.calculate_avg_price(consumption=self.single_day_consumption,
                                              prices=self.get_specific_day_prices(date),
                                              granularity='D')

    def _persist_grouped_calculations(self, grouped, granularity, granularity_name):
        self.grouped_calculations = grouped

        if granularity == "ME":
            grouped["month"] = f"{grouped.index[0].year}-{grouped.index[0].month}"
            insert_to_db(grouped, "month_group_calculations")
            if self.month_group_calculations is not None:
                self.month_group_calculations = self.month_group_calculations[
                    self.month_group_calculations["month"].values != grouped["month"].values
                ]
                self.month_group_calculations = pd.concat(
                    [self.month_group_calculations, grouped], axis=0
                )
            else:
                self.month_group_calculations = grouped

        if granularity == "D":
            grouped["date"] = (
                f"{grouped.index[0].year}-{grouped.index[0].month}-{grouped.index[0].day}"
            )
            insert_to_db(grouped, "day_group_calculations")
            if self.day_group_calculations is not None:
                self.day_group_calculations = self.day_group_calculations[
                    self.day_group_calculations["date"].values != grouped["date"].values
                ]
                self.day_group_calculations = pd.concat(
                    [self.day_group_calculations, grouped], axis=0
                )
            else:
                self.day_group_calculations = grouped

        if granularity_name == "year":
            grouped["year"] = f"{grouped.index[0].year}"
            insert_to_db(grouped, "year_group_calculations", "year")
            self.year_group_calculations = grouped

    def group_calculations(self, price_calculations, granularity):
        grouped, granularity_name = group_price_calculations(
            price_calculations, granularity
        )
        self._persist_grouped_calculations(grouped, granularity, granularity_name)
        return grouped

    def refresh_snapshot(self):
        day = None
        if self.latest_day_electricity_consumption_sum is not None:
            day = PeriodSummary(
                label=self.latest_day,
                consumption_kwh=self.latest_day_electricity_consumption_sum,
                price_euro=self.latest_day_electricity_price_euro,
                avg_kwh_price_vat=self.latest_day_avg_khw_price_with_vat,
                optimization_savings_eur=self.latest_day_optimization_savings_eur,
                optimization_efficiency=self.latest_day_optimization_efficiency,
                avg_price_with_vat=self.latest_day_avg_price_with_vat,
                avg_price_by_avg_spot=self.latest_day_avg_price_by_avg_spot,
                avg_spot_price_with_vat=self.latest_day_avg_spot_price_with_vat,
            )

        month = None
        if self.latest_month_electricity_consumption is not None:
            month = PeriodSummary(
                label=self.latest_month,
                consumption_kwh=self.latest_month_electricity_consumption,
                price_euro=self.latest_month_electricity_price_euro,
                avg_kwh_price_vat=self.latest_month_avg_khw_price_with_vat,
                optimization_savings_eur=self.latest_month_optimization_savings_eur,
                optimization_efficiency=self.latest_month_optimization_efficiency,
                avg_price_with_vat=self.latest_month_avg_price_with_vat,
                avg_price_by_avg_spot=self.latest_month_avg_price_by_avg_spot,
                avg_spot_price_with_vat=self.latest_month_avg_spot_price_with_vat,
            )

        self.snapshot = HerrforsSnapshot(
            latest_day=self.latest_day,
            latest_month=self.latest_month,
            day=day,
            month=month,
            day_group_calculations=self.day_group_calculations,
            month_group_calculations=self.month_group_calculations,
            latest_day_detail=self.latest_day_electricity_price_consumption_calculations,
        )

    async def calculate_avg_price(self, consumption=None, prices=None, granularity=None):
        """
        Calculates the average electricity price based on consumption and price data inputs
        over specified time granularity. Merges the consumption data with price data, computes
        various aggregated statistics including marginal and VAT-inclusive prices, and organizes
        the data by granularity. The calculations also include optimization efficiency metrics
        and potential savings.

        This function returns processed pricing calculations at a granular level and the grouped
        aggregations summarizing the analysis. The method supports daily granularity calculations
        by default when no specific granularity is provided and also updates internal class
        attributes storing the latest day's results.

        :param consumption: Consumption data consisting of electricity usage per time interval.
        :param prices: Price data containing electricity pricing details for corresponding
                       time intervals.
        :param granularity: Time granularity for aggregation (e.g., 'D' for daily).
        :return: A tuple containing detailed pricing calculations and grouped aggregated results.
        :rtype: tuple[DataFrame, DataFrame]
        """

        if (consumption is None or prices is None) and (granularity == 'D' or granularity is None):
            if self.latest_day_electricity_consumption is None or self.latest_day_electricity_prices is None:
                await asyncio.gather(self.get_consumption(),
                                     self.get_latest_day_prices())
            granularity = 'D'
            consumption = self.latest_day_electricity_consumption
            prices = self.latest_day_electricity_prices

        if consumption is not None and prices is None:
            logger.info(f"Prices not given, get prices from consumption dataframe")

            # take only 'price', 'timestamp_tz' columns from consumption df
            prices_ = consumption[['price', 'timestamp_tz']]

            # rename price to prices
            prices_.columns = ['prices', 'timestamp_tz']

            prices = add_vat_to_prices(prices_)

        if consumption is None:
            logger.info(f"Can't do electricity calculations no consumption data available ")
            return
        combine_df = pd.merge(consumption, prices, on=['timestamp_tz'], how='left', indicator=True)

        # filter df which has both
        combine_df = combine_df[combine_df['_merge'] == 'both']
        # combine_df['timestamp_tz_utc'] = combine_df['timestamp_tz']
        # combine_df['timestamp_tz'] = combine_df['datetime']

        price_calculations = combine_df
        price_calculations = apply_price_calculations(
            price_calculations, marginal_price=self.marginal_price
        )

        # insert day price calc to dd
        if granularity != 'YE':
            insert_to_db(price_calculations, 'price_calculations_all', 'timestamp_tz')

        grouped = self.group_calculations(price_calculations, granularity=granularity)

        if self.latest_day is not None and granularity == 'D' and len(grouped) > 0:
            self.latest_day_electricity_price_consumption_calculations = price_calculations
            self.latest_day_electricity_consumption_sum = float(grouped['consumption_sum'].iloc[0])
            self.latest_day_electricity_price_euro = float(grouped['price_marg_alv_euro_sum'].iloc[0])
            self.latest_day_optimization_savings_eur = float(grouped['day_optimization_savings_eur'].iloc[0])
            self.latest_day_optimization_efficiency = float(grouped['day_optimization_efficiency'].iloc[0])
            self.latest_day_avg_price_with_vat = float(grouped['day_avg_price_alv'].iloc[0])
            self.latest_day_avg_price_by_avg_spot = float(grouped['day_avg_price_with_avg_spot'].iloc[0])
            self.latest_day_avg_spot_price_with_vat = float(grouped['prices_cent_vat_avg'].iloc[0])
            self.latest_day_avg_khw_price_with_vat = float(grouped['day_avg_khw_price_with_alv'].iloc[0])
            if self.latest_day_electricity_prices is None:
                self.latest_day_electricity_prices = prices

        if self.latest_month is not None and granularity == 'ME' and len(grouped) > 0:
            self.latest_month_electricity_consumption = float(grouped['consumption_sum'].iloc[0])
            self.latest_month_electricity_price_euro = float(grouped['price_marg_alv_euro_sum'].iloc[0])
            self.latest_month_optimization_savings_eur = float(grouped['month_optimization_savings_eur'].iloc[0])
            self.latest_month_optimization_efficiency = float(grouped['month_optimization_efficiency'].iloc[0])
            self.latest_month_avg_price_with_vat = float(grouped['month_avg_price_alv'].iloc[0])
            self.latest_month_avg_price_by_avg_spot = float(grouped['month_avg_price_with_avg_spot'].iloc[0])
            self.latest_month_avg_spot_price_with_vat = float(grouped['prices_cent_vat_avg'].iloc[0])
            self.latest_month_avg_khw_price_with_vat = float(grouped['month_avg_khw_price_with_alv'].iloc[0])
            if self.month_prices is None:
                self.month_prices = prices

        if granularity == 'YE' and self.year_prices is None:
            self.year_prices = prices

        self.refresh_snapshot()
        return price_calculations, grouped

    async def get_latest_day_prices(self):
        self.latest_day_electricity_prices = await self.get_electricity_prices(apikey=self.apikey)

        return self.latest_day_electricity_prices

    async def get_specific_day_prices(self, date):

        if not isinstance(date, pd._libs.tslibs.timestamps.Timestamp):
            start = pd.Timestamp(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]), tz='EET')
            start = start.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)
        else:
            start = date.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)

            # delete always from year_prices because fetching these again is fairly fast
            if self.year_prices is not None:
                if not self.year_prices.empty:
                    self.year_prices = self.year_prices[self.year_prices['timestamp_tz'].dt.date.values != date]

        return await self.get_electricity_prices(self.apikey, start, end)

    @staticmethod
    async def get_electricity_prices(apikey=None, start=None, end=None, resolution=15):
        """
        Fetches electricity prices for a given date range using the Entso-E API. This method fetches
        day-ahead electricity prices for the specified country, processes the pricing data, and calculates the VAT-adjusted
        prices based on applicable VAT rules during the specified time periods. If no date range is specified, the default
        time period is set dynamically based on the current time.

        :param resolution:
        :param apikey: Entso-E API key required for authentication.
        :type apikey: str
        :param start: Start timestamp for the queried data in timezone-aware datetime format. If not provided,
            it defaults to a calculated range based on the current time.
        :type start: pd.Timestamp, optional
        :param end: End timestamp for the queried data in timezone-aware datetime format. Must be greater than
            or equal to the start time. Defaults to a calculated range if not provided.
        :type end: pd.Timestamp, optional
        :return: A Pandas DataFrame with processed electricity pricing data. Includes timestamps, raw prices
            (in cents), VAT-adjusted prices, and applicable VAT rates.
        :rtype: pd.DataFrame
        :raises ValueError: If the apikey argument is not provided.
        """

        if apikey is None:
            raise ValueError("Entso-E API key is need!")

        country_code = 'FI'

        def query_entsoe_client(apikey, start, end, resolution=None):
            client = EntsoePandasClient(api_key=apikey,
                                        # Optional parameters:
                                        retry_count=5,
                                        retry_delay=5,
                                        timeout=380)

            if start is None:
                days_later = days_later_for_latest()
                start = pd.Timestamp.now(tz='EET') - pd.Timedelta(days=days_later)
                start = start.normalize()
                end = start + pd.Timedelta(days=1)

            # check that start and end are in correct format, if not modify
            if not isinstance(start, pd._libs.tslibs.timestamps.Timestamp):
                start = pd.Timestamp(start, tz='EET')
            if not isinstance(end, pd._libs.tslibs.timestamps.Timestamp):
                end = pd.Timestamp(end, tz='EET') + pd.Timedelta(hours=23, minutes=59, seconds=59)

            logger.info(f" Get prices between {start} and {end}")

            if resolution is not None:
                resolution_ = f"{resolution}min"
            else:
                resolution_ = resolution

            try:
                entsoe_prices = client.query_day_ahead_prices(country_code, start=start, end=end, resolution=resolution_)
            except Exception as e:
                logger.warning(f"Error fetching prices: {e}")
                entsoe_prices = pd.DataFrame()
            return entsoe_prices

        loop = asyncio.get_running_loop()
        prices = await loop.run_in_executor(None, functools.partial(query_entsoe_client,
                                                                    apikey, start, end, resolution), )

        # prices = client.query_day_ahead_prices(country_code, start=start, end=end)
        if not prices.empty:

            return add_vat_to_prices(prices)
        else:
            logger.info(f"No prices found for {start} and {end}, return was {prices}")
            return None
