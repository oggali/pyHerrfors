#custom_components/pyHerrfors/client.py
import datetime
import pandas as pd
import aiohttp
import asyncio
from entsoe import EntsoePandasClient
import functools
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)


class Herrfors:

    def __init__(self, usage_place, customer_number, apikey=None, marginal_price=None):
        """

        """
        self.single_day_consumption = None
        self.usage_place = usage_place
        self.customer_number = customer_number
        self.session = None
        self.login_time = None
        self.dataSetsYear = None
        self.apikey = apikey
        self.marginal_price = marginal_price

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

    async def __aexit__(self, *err):
        await self.session.close()
        self.session = None

    async def login(self,usage_place=None, customer_number=None):
        """
        Performs the login dance required to obtain cookies etc. for further API communication
        async def main():
            async with aiohttp.ClientSession() as session:
                async with session.get('http://httpbin.org/get') as resp:
                    logger.info(resp.status)
                    logger.info(await resp.text())

        asyncio.run(main())

        session = aiohttp.ClientSession()
        async with session.get('...'):
            # ...
        await session.close()
        """

        url = "https://meter.katterno.fi//index.php"

        # Set the login credentials

        if usage_place is not None:
            self.usage_place = usage_place
        if customer_number is not None:
            self.customer_number = customer_number

        # Set up the login data in a dictionary
        login_data = {"usageplace": self.usage_place, "customernumber": self.customer_number, "submit": "1"}
        logger.info(f"Logging into {url} with data: {login_data}")

        self.session = aiohttp.ClientSession()
        # Send a GET request to the data URL using the session object
        login_response = await self.session.post(url, data=login_data) # , verify=None)
        # logger.info(login_response.status)
        # logger.info(await login_response.text())
        # login_response_text = login_response.text
        login_response_text = str(await login_response.text())
        parse_data_sets_year = login_response_text[login_response_text.find('dataSetsYear = '):login_response_text.find(';\n\tdataSetsNettedYear = ')].replace('dataSetsYear = ', '')
        self.dataSetsYear = eval(parse_data_sets_year)


        # Check the response status code to see if the login was successful
        # if login_response.status_code == 200:
        if login_response.status == 200:
            import time
            logger.info("Login successful")
            self.login_time = time.time()
            # self.session = session
            return True
        else:
            logger.info("Login failed")
            return False

    async def logout(self):
        """
        Closes the session by logging out
        :return: the response object
        """
        r = None
        if self.session is not None or self.session:
            r = await self.session.get("https://meter.katterno.fi/index.php?logout&amp;lang=FIN")
        await self.__aexit__()
        return r

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
        if (self.latest_day is None or latest_day != self.latest_day) and update_self:

            self.latest_day = latest_day
            self.consumption_params = {'date-year':  self.latest_day.year,
                                      'date-month':  self.latest_day.month,
                                      'date-day':  self.latest_day.day
                                      }
            self.latest_day_electricity_consumption = None
            self.latest_day_electricity_prices = None
        return latest_day

    async def _check_session(self):

        import time

        if self.session is None:
            await self.login()
        elif (time.time() - self.login_time) / (60 * 60) > 1:
            await self.login()
        else:
            return


    async def update_latest_day(self):
        """
        Updates the latest day consumption and calculates the
        average price for that day. If the user session is not
        established, it logs in to create a session before proceeding.

        :raises RuntimeError: If authentication fails during login,
            this will lead to an inability to establish a session for the
            operation.
        """

        await self._check_session()
        self._get_latest_day()
        # delete always from year_prices because fetching these again is fairly fast
        if self.year_prices is not None:
            if not self.year_prices.empty:
                self.year_prices = self.year_prices[self.year_prices['timestamp_tz'].dt.date.values != self.latest_day]
        await self.calculate_avg_price(granularity='D')
        logger.info(f"Day {self.latest_day} Electricity consumption was {self.latest_day_electricity_consumption_sum} kWh"
              f"Cost was {self.latest_day_electricity_price_euro} € with avg price {self.latest_day_avg_price_with_vat} c/kWh")
        await self.logout()

    async def force_update_current_year(self, day_level=False):
        self.year_prices = None
        self.year_consumption = None
        self.day_group_calculations = None
        self.month_group_calculations = None
        logger.info("Force updating current year")

        self._get_latest_day()

        await self._check_session()

        calc_month = 1
        import calendar
        while calc_month < self.latest_day.month:

            res = calendar.monthrange(int(self.latest_day.year), month=int(calc_month))
            start_day = datetime.date(year=int(self.latest_day.year), month=int(calc_month), day=1)
            last_day = datetime.date(year=int(self.latest_day.year), month=int(calc_month), day=res[1])

            logger.info(f"Month {int(self.latest_day.year)}-{int(calc_month)} calculating dates between {start_day} and {last_day}")

            month_df, month_prices = await asyncio.gather(self.get_specific_month_consumption(start_day, last_day),
                                                          self.get_electricity_prices(self.apikey, start_day, last_day))

            if day_level:
                logger.info("Update day level calculations also")

                fetch_day = start_day

                while fetch_day <=last_day:
                    logger.debug(f"Calculating day level avg for {fetch_day}")

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



    async def update_latest_month(self, poll_always=False):
        latest_day = self._get_latest_day(update_self=False)

        if self.year_consumption is None:
            poll_always=True

        if self.year_consumption is not None:
            if latest_day not in self.year_consumption['timestamp_tz'].dt.date.values and datetime.datetime.now().hour > 9:
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

            await self._check_session()
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

            if latest_day not in month_df['timestamp_tz'].dt.date.values:
                logger.info(f"Latest day {latest_day} not found yet, so let's try again later")
                if self.latest_day is None:
                    self.latest_day = latest_day - datetime.timedelta(days=1)


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

            if results is not None:
                # todo add check if result list contains only empty dataframes
                month_df_missing = pd.concat(results, ignore_index=True)
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
                  'date-day': date.day
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
            logger.info(f"Getting consumption for {get_param}")

        await self._check_session()
        r = await self.session.get(
            "https://meter.katterno.fi//consumption.php",
            params=params)
        r_text = str(await r.text())
        parse_data_sets = r_text[r_text.find('dataSetsSingleConsumption = '):r_text.find(
            ';\n\tdataSetsSingleConsumptionNetted = ')].replace('dataSetsSingleConsumption = ', '')
        try:
            data_sets_single_consumption = eval(parse_data_sets)
        except Exception as e:
            logger.error(f"Error in eval(parse_data_sets). Exception was: {e}. parse_data_sets: {parse_data_sets}")
            data_sets_single_consumption = [[]]

        # sum elements inside list for check
        check_sum = sum(data_sets_single_consumption[0])

        def get_hours(day_):

            day_ = datetime.datetime(year=day_.year, month=day_.month, day=day_.day, hour=00, minute=0, second=0)
            import pytz
            # Define EET timezone
            eet = pytz.timezone("Europe/Helsinki")  # EET corresponds to Helsinki timezone
            # Generate timestamp list with EET timezone
            hours_ = [eet.localize(day_.replace(hour=h, minute=0, second=0)) for h in range(24)]

            return hours_

        if check_sum > 0:

            # Validate consumption data matches 24 values
            if len(data_sets_single_consumption[0]) != 24 and granularity == 'D':
                raise ValueError("Invalid consumption data. Expected exactly 24 hourly consumption values for {}")

            if not params_given:

                # Fetch data for the last 24 hours
                day = datetime.datetime.now() - datetime.timedelta(days=self._days_later)

                loop = asyncio.get_running_loop()
                hours = await loop.run_in_executor(None, functools.partial(get_hours,day), )
                # hours = get_hours(day)

                # Create consumption DataFrame with timezone-aware timestamps
                consumption_df = pd.DataFrame({'timestamp_tz': hours, 'consumption': data_sets_single_consumption[0]})
                self.latest_day_electricity_consumption = consumption_df
            else:
                
                if granularity == 'D':

                    day = datetime.date(params.get('date-year'),params.get('date-month'),params.get('date-day'))

                    loop = asyncio.get_running_loop()
                    hours = await loop.run_in_executor(None, functools.partial(get_hours, day), )
    
                    # hours = get_hours(day)
    
                    # Create consumption DataFrame with timezone-aware timestamps
                    consumption_df = pd.DataFrame({'timestamp_tz': hours, 'consumption': data_sets_single_consumption[0]})
    
                    self.single_day_consumption = consumption_df
                elif granularity == 'M':
                    # todo handle month days
                    consumption_df = pd.DataFrame({'days': [], 'consumption': data_sets_single_consumption[0]})

                else:
                    # todo handle year months
                    consumption_df = pd.DataFrame({'months': [], 'consumption': data_sets_single_consumption[0]})

            return consumption_df
        else:
            logger.info(f"No data available for given parameters: {params}")
            if granularity == 'D':
                # clear data from single_day_consumption df
                self.single_day_consumption=pd.DataFrame(columns=self.single_day_consumption.columns)

            return self.single_day_consumption

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

        if granularity == 'D' and len(price_calculations) != 24:
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
            if self.month_group_calculations is not None:
                self.month_group_calculations = self.month_group_calculations[self.month_group_calculations['month'].values != grouped['month'].values]
                self.month_group_calculations=pd.concat([self.month_group_calculations,grouped], axis=0)
            else:
                self.month_group_calculations = grouped

        if granularity == 'D':
            grouped['date'] = f"{grouped.index[0].year}-{grouped.index[0].month}-{grouped.index[0].day}"
            if self.day_group_calculations is not None:
                self.day_group_calculations = self.day_group_calculations[self.day_group_calculations['date'].values != grouped['date'].values]
                self.day_group_calculations=pd.concat([self.day_group_calculations,grouped], axis=0)
            else:
                self.day_group_calculations = grouped

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

        price_calculations = combine_df
        price_calculations['price'] = price_calculations.apply(lambda row_: row_['consumption'] * row_['prices_cent'],
                                                                       axis=1)
        price_calculations['price_euro'] = price_calculations.apply(lambda row_: row_['price'] / 100,
                                                                            axis=1)

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
    async def get_electricity_prices(apikey=None,start=None, end=None):
        """
        Fetches electricity prices for a given date range using the Entso-E API. This method fetches
        day-ahead electricity prices for the specified country, processes the pricing data, and calculates the VAT-adjusted
        prices based on applicable VAT rules during the specified time periods. If no date range is specified, the default
        time period is set dynamically based on the current time.

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
        def query_entsoe_client(apikey, start, end):
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
                end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)

            # check that start and end are in correct format, if not modify
            if not isinstance(start, pd._libs.tslibs.timestamps.Timestamp):
                start = pd.Timestamp(start, tz='EET')
            if not isinstance(end, pd._libs.tslibs.timestamps.Timestamp):
                end = pd.Timestamp(end, tz='EET') + pd.Timedelta(hours=23, minutes=59, seconds=59)

            logger.info(f" Get prices between {start} and {end}")

            return client.query_day_ahead_prices(country_code, start=start, end=end)

        loop = asyncio.get_running_loop()
        prices = await loop.run_in_executor(None, functools.partial(query_entsoe_client,
                                                                    apikey,start, end), )


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

            return prices_df
        else:
            logger.info(f"No prices found for {start} and {end}, return was {prices}")

