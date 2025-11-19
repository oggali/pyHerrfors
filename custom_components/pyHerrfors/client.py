#custom_components/pyHerrfors/client.py
import datetime
import pandas as pd
import aiohttp
import asyncio
from entsoe import EntsoePandasClient
import functools
import logging
import os
import json
import duckdb as dd
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

BASE_DIR = os.path.dirname(__name__)
TOKEN_FILE = os.path.join(BASE_DIR, "share", "herrfors_token.json")

DB_FILE = os.path.join(BASE_DIR, "share", "herrfors_data.db")


def _db_empty():
    if os.path.exists(DB_FILE):
        con = dd.connect(DB_FILE)
        db_tables = con.sql('SELECT * FROM duckdb_tables()')
        if len(db_tables['table_name'].fetchall()) == 0:
            con.close()
            return True
        else:
            con.close()
            return False

    if not os.path.exists(DB_FILE):
        return True

def _table_not_exists(table_name):
    if os.path.exists(DB_FILE):
        con = dd.connect(DB_FILE)
        db_tables = con.sql(f"SELECT * FROM information_schema.tables where table_name='{table_name}'")

        if len(db_tables) == 0:
            con.close()
            return True
        else:
            con.close()
            return False
    else:
        return True

def insert_to_db(df_to_insert, table_name, del_key_column=None):

    con = dd.connect(DB_FILE)

    if _table_not_exists(table_name):
        con.sql(f'''        
                CREATE TABLE {table_name} as
                SELECT * FROM df_to_insert
                ''')

    if del_key_column is not None:
        # first delete every row where date is same as in df_to_insert
        con.sql(f"""
        DELETE FROM {table_name}
        where {del_key_column} in ( select {del_key_column} from df_to_insert group by {del_key_column})
        """)

    # then insert
    con.sql(f'''
            INSERT INTO {table_name}
            SELECT * FROM df_to_insert
    ''')

    con.execute("CHECKPOINT")
    con.close()

def get_all_from_db_as_df(table_name=None):
    con = dd.connect(DB_FILE)
    if table_name is None:
        table_name = "price_calculations_all"

    return con.sql(f"SELECT * FROM {table_name}").to_df()

class Herrfors:

    def __init__(self, email, password, apikey=None, marginal_price=None):
        """

        """
        self.single_day_consumption = None
        # self.usage_place = usage_place
        # self.customer_number = customer_number
        self.email = email
        self.password = password
        self.session = None
        self.session_token = None
        self.login_time = None
        self.toke_exp = None
        self.headers = None
        self.time_step = None
        self.dataSetsYear = None
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
        self.latest_day_avg_price_with_vat= None
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

    async def __aexit__(self, *err):
        if self.session is not None:
            await self.session.close()
            self.session = None

    def get_session_token(self):

        if not os.path.exists(TOKEN_FILE):
            logger.info(f"⚠️ No token file found. {os.path.abspath(TOKEN_FILE)}")
            return False

        with open(TOKEN_FILE, "r") as f:
            token_data = json.load(f)

        exp = token_data.get("expires")
        logger.info(f"Token expires in {datetime.datetime.fromisoformat(exp.replace('Z', '+00:00'))}")
        if not exp:
            return False
        dt = datetime.datetime.fromisoformat(exp.replace("Z", "+00:00"))
        if not datetime.datetime.now(datetime.timezone.utc) < dt:
            return False
        else:
            self.login_time=token_data.get('token_timestamp')
            self.toke_exp= dt
            from decode_token import decrypt_wrapped_token # todo remove comment here when done testing

            created_time, self.session_token = decrypt_wrapped_token(token_data.get('token'),self.email, self.password)
            session = aiohttp.ClientSession()
            session.cookie_jar.update_cookies({"__Secure-next-auth.session-token": self.session_token})
            self.session = session

            self.headers = {
                "x-client-id": "6212c91e-f646-4a74-b3ce-38a4a3df2d9d",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "accept": "application/json, text/plain, */*",
                "referer": "https://portal.herrfors.fi/fi-FI/charts",
            }
            return True

    def fetch_expiration(self):
        try:
            url = "https://portal.herrfors.fi/api/auth/session"
            r = requests.get(url, cookies={"__Secure-next-auth.session-token": self.session_cookie}, timeout=10)
            r.raise_for_status()
            j = r.json()
            return j.get("expires")
        except Exception as e:
            print("Error fetching expiry:", e)
            return None

    async def logout(self):
        """
        Closes the session by logging out
        :return: the response object
        """
        #todo check if logout is needed to implement

        # r = None
        # if self.session is not None or self.session:
        #     r = await self.session.get("https://meter.katterno.fi/index.php?logout&amp;lang=FIN")
        await self.__aexit__()
        return

    def get_user_profile(self):
        """
        Returns the current user's profile information
        :return: the user details
        """


    def get_metering_points(self, customer):
        """
        Returns the metering points available for the specified customer
        :param customer: the customer ID
        :return: the metering points, including a lot of metadata about them
        """

        #https://meter.katterno.fi//consumption.php?date-year=2024&date-month=12
        #https://meter.katterno.fi//consumption.php?date-year=2024&date-month=12&date-day=19

    def _get_latest_day(self, update_self=True):
        """
        Determines the latest day to be considered based on the current hour and calculates related parameters.

        Calculates the date difference depending on whether the current hour is less than 8,
        determines two days prior; otherwise, determines one day prior. Constructs the date
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

        # if hour now is smaller than 6 then get two days later
        if datetime.datetime.now().hour < 6:
            self._days_later = 2
        else:
            self._days_later = 1


        # latest day check
        latest_day = datetime.date.today() - datetime.timedelta(days=self._days_later)
        previous_day = datetime.date.today() - datetime.timedelta(days=(self._days_later+1))
        if (self.latest_day is None or latest_day != self.latest_day) and update_self:

            self.latest_day = latest_day
            self.consumption_params = {'date-year':  self.latest_day.year,
                                      'date-month':  self.latest_day.month,
                                      'date-day':  self.latest_day.day,
                                       'date': self.latest_day.strftime('%Y-%m-%d'),
                                       'previous_date-year': previous_day.year,
                                       'previous_date-month': previous_day.month,
                                       'previous_date-day': previous_day.day,
                                       'previous_date': previous_day.strftime('%Y-%m-%d')
                                      }
            self.latest_day_electricity_consumption = None
            self.latest_day_electricity_prices = None
        return latest_day

    def _check_session(self):

        if self.session is None:
            return self.get_session_token()
        elif (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)) > self.toke_exp:
            return self.get_session_token()
        else:
            logger.info("Session is still valid, so we don't need to get a new one")
            return True


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
            calc_month = 10 # new client supports only from beginning of November
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

                    while fetch_day <=last_day:
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

                calc_month+=1
            await self.update_latest_month(True)
            await self.calculate_avg_price(consumption=self.year_consumption, prices=self.year_prices, granularity='YE')

        await self.logout()

    async def update_latest_month(self, poll_always=False):
        latest_day = self._get_latest_day(update_self=False)

        if self.year_consumption is None:
            poll_always=True

        if self.year_consumption is not None:
            if latest_day not in self.year_consumption['date'].values and datetime.datetime.now().hour > 9:
                logger.info(f"Latest day {latest_day} not found from memory, so let's try to fetch it")
                poll_always=True

        if self.year_prices is not None and self.year_consumption is not None and not poll_always:
            # check if dataframes are same size, if not then there is data missing, and we can try to poll missing ones
            if len(self.year_consumption)<len(self.year_prices):
                logger.info(f"Prices df size:{len(self.year_prices)} Consumption df size:{len(self.year_consumption)}")
                logger.info("Let's try to fill missing days")
                poll_always=True


        if poll_always or (7 < datetime.datetime.now().hour <= 8):
            logger.info(f"It's now {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} and we start polling data.")

            if self._check_session():
                start_day = datetime.date(year=latest_day.year,month=latest_day.month,day=1)

                last_day = latest_day
                self.latest_month =f"{latest_day.year}/{latest_day.month}"

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

                self.year_prices = pd.concat([self.year_prices, month_prices], axis=0)

                self.month_consumption=month_df
                self.month_prices=month_prices

                logger.info(f"Month {self.latest_month} prices df size is {len(month_prices)} and consumption df size is {len(month_df)}")

                await self.calculate_avg_price(consumption=month_df, prices=month_prices, granularity='ME')

                logger.info(
                    f"Month {self.latest_month} Electricity consumption is {self.latest_month_electricity_consumption} kWh"
                    f" Cost is {self.latest_month_electricity_price_euro} € with avg price {self.latest_month_avg_khw_price_with_vat} c/kWh")

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

    def update_latest_year(self):

        return

    async def get_latest_day_consumption(self):

        return await self.get_consumption(None)

    async def get_specific_month_consumption(self, start_day=None, last_day=None, month=None):

        if start_day is None:
            import calendar
            res = calendar.monthrange(int(month.split('/')[0]),month=int(month.split('/')[1]))
            start_day = datetime.date(year=int(month.split('/')[0]),month=int(month.split('/')[1]),day=1)
            last_day = datetime.date(year=int(month.split('/')[0]),month=int(month.split('/')[1]),day=res[1])

        # check if start_day is found from self.year_consumption
        month_df = pd.DataFrame(columns=['timestamp_tz', 'consumption'])
        month_df_missing = pd.DataFrame(columns=['timestamp_tz', 'consumption'])
        if self.year_consumption is not None:
            if start_day not in  self.year_consumption['timestamp_tz'].dt.date.values or self.year_consumption.empty:
                # loop dates between start_day to self._get_latest_day() store results to pd df
                await self.get_specific_day_consumption(start_day)
                if not self.single_day_consumption.empty:
                    month_df_missing = self.single_day_consumption
                    self.year_consumption=pd.concat([self.year_consumption, month_df_missing], axis=0)
                    month_df = month_df_missing
            else:
                month_df = self.year_consumption[self.year_consumption['timestamp_tz'].dt.date.values == start_day]
        else:
            await self.get_specific_day_consumption(start_day)
            if not self.single_day_consumption.empty:
                month_df_missing = self.single_day_consumption
                self.year_consumption = pd.concat([self.year_consumption, month_df_missing], axis=0)
                month_df = month_df_missing
        fetch_day = start_day + datetime.timedelta(days=1)

        fetch_tasks = []
        while fetch_day <= last_day:
            if self.year_consumption is not None:
                if fetch_day not in self.year_consumption['timestamp_tz'].dt.date.values or self.year_consumption.empty:
                    fetch_tasks.append(self.get_specific_day_consumption(fetch_day))
                    # month_df = pd.concat([month_df, self.single_day_consumption], axis=0)
                    # self.year_consumption = pd.concat([self.year_consumption, month_df], axis=0)
                else:
                    month_df = pd.concat(
                        [month_df, self.year_consumption[self.year_consumption['timestamp_tz'].dt.date.values == fetch_day]],
                        axis=0)
            else:
                fetch_tasks.append(self.get_specific_day_consumption(fetch_day))
                # month_df = pd.concat([month_df, self.single_day_consumption], axis=0)
                # self.year_consumption = pd.concat([self.year_consumption, month_df], axis=0)
            fetch_day = fetch_day + datetime.timedelta(days=1)
        if fetch_tasks:
            results = await asyncio.gather(*fetch_tasks)
            # await self.logout()
            if results is not None:
                # Filter out empty DataFrames
                filtered_results = [df for df in results if not df.empty]
                if filtered_results:
                    month_df_missing = pd.concat(filtered_results, ignore_index=True)
            if not month_df.empty and not month_df_missing.empty:
                month_df = pd.concat([month_df, month_df_missing], axis=0)
            else:
                if not month_df_missing.empty:
                    month_df = month_df_missing
            if not month_df_missing.empty:
                self.year_consumption = pd.concat([self.year_consumption, month_df_missing], axis=0)

        return month_df


    async def get_specific_day_consumption(self, date):

        if not isinstance(date, datetime.date):
            date = datetime.date(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]))

        self.consumption_params = {'date-year': date.year,
                  'date-month': date.month,
                  'date-day': date.day,
                  'date': date.strftime('%Y-%m-%d')
                  }

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
        granularity= ''
        if params is None:
            self._get_latest_day()

            params = self.consumption_params
            logger.info(f"No parameter given, get {self.latest_day} days consumption")
            params_given = False

        else:
            if params.get('date-day','false') != 'false':
                get_param = f"{params.get('date-year')}-{params.get('date-month')}-{params.get('date-day')} date"
                granularity = 'D'

            elif params.get('date-month','false') != 'false':
                get_param = f"{params.get('date-year')}-{params.get('date-month')} month"
                granularity = 'M'
            else:
                get_param = f"{params.get('date-year')} year"
                granularity = 'Y'
            logger.info(f"Getting consumption for {params.get('date')}")

        if int(params.get('date').replace('-',''))<20251001:
            time_step = 60
            self.resolution = 60
        else:
            time_step = 15
            self.resolution = 15
        logger.info(f"Time step used is {time_step}")

        if params.get('previous_date-year',None) is None and granularity == 'D':
            previous_day = datetime.date(int(params.get('date-year')), int(params.get('date-month')),
                                         int(params.get('date-day'))) - datetime.timedelta(days=1)
            # add previous params to params dict
            params['previous_date-year'] = previous_day.year
            params['previous_date-month'] = previous_day.month
            params['previous_date-day'] = previous_day.day
            params['previous_date'] = previous_day.strftime('%Y-%m-%d')

        requests_params = {
            "coId": "60754370",
            "from": f"{params.get('previous_date')}T22:00:00.000Z",
            "to": f"{params.get('date')}T22:00:00.000Z",
            "price": "true",
            "temp": "false",
            "timeStep": f"{time_step}",
        }

        url = "https://portal.herrfors.fi/api/charts/readings"

        logger.info(f"Parameters to request: {requests_params}")

        if self._check_session():

            r = await self.session.get(url, headers=self.headers, params=requests_params, timeout=30)

            r_txt = await r.text()
            # await self.logout()
            data = json.loads(r_txt)

            consumption_data = data['values']
            if r.status != 200:
                raise EnvironmentError (r)

            logger.debug(f"Get data: {consumption_data}")

            # sum elements inside list for check
            check_sum = len(consumption_data)

            consumption_data_df = pd.DataFrame(consumption_data)

            #transform date column from str to timestamp
            consumption_data_df['timestamp_tz'] = pd.to_datetime(consumption_data_df['date']).apply(lambda x: x.tz_convert('EET'))

            #convert timestamp timezone to EET and extract date only to own column
            consumption_data_df['date'] = consumption_data_df['timestamp_tz'].apply(lambda x: x.tz_convert('EET').date())

            def get_hours(day_, step=60):
                """
                Generate a list of timestamps for a given day, either split into 15-minute intervals or hourly.

                :param day_: A `datetime` object representing the day.
                :param step: The interval in minutes (default is 60). Supported values: 15, 60.
                :return: A list of datetime objects localized to the "Europe/Helsinki" timezone.
                """
                # Ensure `day_` begins at midnight
                day_ = datetime.datetime(year=day_.year, month=day_.month, day=day_.day, hour=0, minute=0, second=0)

                # Define EET timezone
                import pytz
                eet = pytz.timezone("Europe/Helsinki")

                # Validate the step value
                if step not in (15, 60):
                    raise ValueError("Step must be either 15 or 60 minutes.")

                # Generate timestamps with the given step
                timestamps = [
                    eet.localize(day_ + datetime.timedelta(minutes=minutes))
                    for minutes in range(0, 1440, step)  # 1440 minutes = 24 hours
                ]

                return timestamps

            if check_sum > 0:
                # todo fix
                # Validate consumption data matches 96 values
                if len(consumption_data_df) not in  [96, 24, 92, 100] and granularity == 'D':
                    raise ValueError(f"Invalid consumption data. Expected exactly 96 or 24 hourly consumption values for {consumption_data_df}")

                else:
                    consumption_df=consumption_data_df

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
                    # clear data from single_day_consumption df
                    self.single_day_consumption=pd.DataFrame(columns=self.single_day_consumption.columns)

                return self.single_day_consumption
        else:
            logger.info(f"No session available")

    async def get_specific_day_avg_price(self, date):

        await self.get_specific_day_consumption(date)
        return await self.calculate_avg_price(consumption=self.single_day_consumption,
                                        prices=self.get_specific_day_prices(date),
                                        granularity='D')

    def group_calculations(self, price_calculations, granularity):

        grouped = price_calculations.groupby(pd.Grouper(key='timestamp_tz', freq=granularity)).agg({
            'consumption': 'sum',
            'prices_cent': 'mean',
            'prices_cent_vat': 'mean',
            'price_marginal_alv': 'mean',
            'price': 'sum',
            'price_euro': 'sum',
            'price_vat': 'sum',
            'price_marg_alv': 'sum',
            'price_marg_alv_euro': 'sum'
        }).rename(columns={
            'consumption': 'consumption_sum',
            'prices_cent': 'prices_cent_avg',
            'prices_cent_vat': 'prices_cent_vat_avg',
            'price_marginal_alv': 'price_marginal_alv_avg',
            'price': 'price_cent_sum',
            'price_euro': 'price_euro_sum',
            'price_vat': 'price_alv_sum',
            'price_marg_alv': 'price_marg_alv_sum',
            'price_marg_alv_euro': 'price_marg_alv_euro_sum'
        })

        grouped['granularity'] = granularity

        if granularity == 'D':
            granularity_name = 'day'
        elif granularity == 'ME':
            granularity_name = 'month'
        else:
            granularity_name = 'year'

        if granularity == 'D' and len(price_calculations) not in  [24,96]:
            divisor = 7
        else:
            divisor = len(price_calculations)


        grouped[f'{granularity_name}_avg_price_alv'] = grouped['price_alv_sum'] / divisor
        grouped[f'{granularity_name}_avg_price_alv_marg'] = grouped['price_marg_alv_sum'] / divisor
        grouped[f'{granularity_name}_avg_khw_price_with_alv'] = grouped['price_alv_sum'] / grouped['consumption_sum']
        grouped[f'{granularity_name}_avg_price_with_avg_spot'] = (grouped['prices_cent_vat_avg'] * grouped[
            'consumption_sum']) / divisor
        grouped[f'{granularity_name}_optimization_efficiency'] = ((grouped['prices_cent_vat_avg'] - grouped[
            f'{granularity_name}_avg_khw_price_with_alv']) / grouped['prices_cent_vat_avg']) * 100
        grouped[f'{granularity_name}_optimization_savings_eur'] = ((grouped['prices_cent_vat_avg'] * grouped[
            'consumption_sum']) - grouped['price_alv_sum']) / 100

        # round all aggregated values to 3 decimal
        grouped = grouped.round(3)

        self.grouped_calculations = grouped
        if granularity == 'ME':
            grouped['month'] = f"{grouped.index[0].year}-{grouped.index[0].month}"
            # insert month price calc to dd
            insert_to_db(grouped, 'month_group_calculations')
            if self.month_group_calculations is not None:
                self.month_group_calculations = self.month_group_calculations[self.month_group_calculations['month'].values != grouped['month'].values]
                self.month_group_calculations=pd.concat([self.month_group_calculations,grouped], axis=0)
            else:
                self.month_group_calculations = grouped

        if granularity == 'D':
            grouped['date'] = f"{grouped.index[0].year}-{grouped.index[0].month}-{grouped.index[0].day}"
            # insert day price calc to dd
            insert_to_db(grouped, 'day_group_calculations')
            if self.day_group_calculations is not None:
                self.day_group_calculations = self.day_group_calculations[self.day_group_calculations['date'].values != grouped['date'].values]
                self.day_group_calculations=pd.concat([self.day_group_calculations,grouped], axis=0)
            else:
                self.day_group_calculations = grouped

        if granularity_name == 'year':
            grouped['year'] = f"{grouped.index[0].year}"
            # insert year price calc to dd
            insert_to_db(grouped, 'year_group_calculations','year')
            self.year_group_calculations = grouped


        return grouped


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

        if consumption is None or prices is None:
            if self.latest_day_electricity_consumption is None or self.latest_day_electricity_prices is None:
                await asyncio.gather(self.get_consumption(),
                                     self.get_latest_day_prices())
            granularity='D'
            consumption = self.latest_day_electricity_consumption
            prices = self.latest_day_electricity_prices

        if consumption is None:
            logger.info(f"Can't do electricity calculations no consumption data available ")
            return
        combine_df = pd.merge(consumption, prices, on=['timestamp_tz'], how='left', indicator=True)

        #filter df which has both
        combine_df=combine_df[combine_df['_merge']=='both']
        # combine_df['timestamp_tz_utc'] = combine_df['timestamp_tz']
        # combine_df['timestamp_tz'] = combine_df['datetime']

        combine_df['price_vat'] = combine_df.apply(lambda row_: row_['price'] * row_['vat'], axis=1)

        price_calculations = combine_df
        price_calculations['price'] = price_calculations.apply(lambda row_: row_['consumption'] * row_['prices_cent'],
                                                                       axis=1)
        price_calculations['price_euro'] = price_calculations.apply(lambda row_: row_['price'] / 100, axis=1)

        fixed_marginal_price = self.marginal_price
        if fixed_marginal_price is None:
            fixed_marginal_price = 0

        price_calculations['price_marginal_alv'] = price_calculations.apply(
            lambda row_: (row_['prices_cent_vat'] + fixed_marginal_price),
            axis=1)

        price_calculations['price_vat'] = price_calculations.apply(
            lambda row_: row_['consumption'] * row_['prices_cent_vat'], axis=1)

        price_calculations['price_marg_alv'] = price_calculations.apply(
            lambda row_: row_['consumption'] * (row_['prices_cent_vat'] + fixed_marginal_price), axis=1)
        price_calculations['price_marg_alv_euro'] = price_calculations.apply(
            lambda row_: row_['price_marg_alv'] / 100, axis=1)

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

        if self.latest_month is not None and granularity == 'ME' and len(grouped) > 0:
            self.latest_month_electricity_consumption = float(grouped['consumption_sum'].iloc[0])
            self.latest_month_electricity_price_euro = float(grouped['price_marg_alv_euro_sum'].iloc[0])
            self.latest_month_optimization_savings_eur = float(grouped['month_optimization_savings_eur'].iloc[0])
            self.latest_month_optimization_efficiency = float(grouped['month_optimization_efficiency'].iloc[0])
            self.latest_month_avg_price_with_vat = float(grouped['month_avg_price_alv'].iloc[0])
            self.latest_month_avg_price_by_avg_spot = float(grouped['month_avg_price_with_avg_spot'].iloc[0])
            self.latest_month_avg_spot_price_with_vat = float(grouped['prices_cent_vat_avg'].iloc[0])
            self.latest_month_avg_khw_price_with_vat = float(grouped['month_avg_khw_price_with_alv'].iloc[0])



        return price_calculations, grouped
        

    async def get_latest_day_prices(self):
        self.latest_day_electricity_prices = await self.get_electricity_prices(apikey=self.apikey)

        return self.latest_day_electricity_prices


    async def get_specific_day_prices(self, date):


        if not isinstance(date, pd._libs.tslibs.timestamps.Timestamp):
            start = pd.Timestamp(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]),tz='EET')
            start = start.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)
        else:
            start = date.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)

            # delete always from year_prices because fetching these again is fairly fast
            if self.year_prices is not None:
                if not self.year_prices.empty:
                    self.year_prices = self.year_prices[self.year_prices['timestamp_tz'].dt.date.values!=date]

        return await self.get_electricity_prices(self.apikey, start, end)


    @staticmethod
    async def get_electricity_prices(apikey=None,start=None, end=None, resolution=15):
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
            raise ValueError ("Entso-E API key is need!")


        country_code = 'FI'
        def query_entsoe_client(apikey, start, end, resolution):
            client = EntsoePandasClient(api_key=apikey,
                                        # Optional parameters:
                                        retry_count=5,
                                        retry_delay=5,
                                        timeout=380)

            if start is None:
                # if hour now is smaller than 8 then get two days later
                if datetime.datetime.now().hour < 8:
                    days_later = 2
                else:
                    days_later = 1
                start = pd.Timestamp.now(tz='EET') - pd.Timedelta(days=days_later)
                start = start.normalize()
                end = start + pd.Timedelta(days=1)

            # check that start and end are in correct format, if not modify
            if not isinstance(start, pd._libs.tslibs.timestamps.Timestamp):
                start = pd.Timestamp(start, tz='EET')
            if not isinstance(end, pd._libs.tslibs.timestamps.Timestamp):
                end = pd.Timestamp(end, tz='EET') + pd.Timedelta(hours=23, minutes=59, seconds=59)

            logger.info(f" Get prices between {start} and {end}")

            return client.query_day_ahead_prices(country_code, start=start, end=end, resolution=f"{resolution}min")

        loop = asyncio.get_running_loop()
        prices = await loop.run_in_executor(None, functools.partial(query_entsoe_client,
                                                                    apikey,start, end, resolution), )


        # prices = client.query_day_ahead_prices(country_code, start=start, end=end)
        if not prices.empty:
            prices_df = prices.to_frame(name='prices')
            prices_df = prices_df.reset_index().rename(columns={'index': 'datetime'})
            prices_df['prices_cent'] = prices_df['prices'] / 10

            normal_vat = 0.255
            earlier_normal_vat = 0.24
            discount_vat = 0.1
            discount_time_start = '2022-12-1'
            discount_time_end = '2023-4-30'
            new_normal_vat_start = '2024-09-1'

            prices_df['vat'] = prices_df['datetime'].apply(
                lambda x: earlier_normal_vat if x < pd.Timestamp(discount_time_start, tz='EET')
                                        or (pd.Timestamp(discount_time_end, tz='EET') < x < pd.Timestamp(new_normal_vat_start, tz='EET'))
                                        else discount_vat if x < pd.Timestamp(new_normal_vat_start, tz='EET') else normal_vat)

            prices_df['prices_cent_vat'] = prices_df['prices_cent'] * (1 + prices_df['vat'])

            prices_df['timestamp_tz'] = prices_df['datetime']

            prices_df['date'] = prices_df['timestamp_tz'].apply(lambda x: x.tz_convert('EET').date())

            return prices_df
        else:
            logger.info(f"No prices found for {start} and {end}, return was {prices}")

