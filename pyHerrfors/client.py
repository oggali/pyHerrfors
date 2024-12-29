import datetime

import pandas as pd
import requests
from numpy.f2py.crackfortran import groupends


class Herrfors:

    def __init__(self, usage_place, customer_number, apikey=None, marginal_price=None):
        """
        :param username: your e-mail address
        :param password: your password
        """
        self.single_day_consumption = None
        self.usage_place = usage_place
        self.customer_number = customer_number
        self.session = None
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
        self.latest_day_avg_price_with_alv = None
        self.latest_day_avg_price_by_avg_spot = None
        self.latest_day_avg_spot_price_cent_with_vat = None
        self.latest_day_avg_khw_price_with_vat = None

        self.latest_month_electricity_consumption = None
        self.latest_month_electricity_price_euro = None
        self.latest_month_optimization_savings_eur = None
        self.latest_month_optimization_efficiency = None
        self.latest_month_avg_price_with_alv = None
        self.latest_month_avg_price_by_avg_spot = None
        self.latest_month_avg_spot_price_cent_with_vat = None
        self.latest_month_avg_khw_price_with_vat = None

        self.grouped_calculations = None

        self.month_group_calculations = None
        self.day_group_calculations = None

    def login(self):
        """
        Performs the login dance required to obtain cookies etc. for further API communication
        """
        session = requests.session()

        url = "https://meter.katterno.fi//index.php"

        # Set the login credentials

        # Set up the login data in a dictionary
        login_data = {"usageplace": self.usage_place, "customernumber": self.customer_number, "submit": "1"}

        # Send a GET request to the data URL using the session object
        login_response = session.post(url, data=login_data, verify=False)

        parse_data_sets_year = login_response.text[login_response.text.find('dataSetsYear = '):login_response.text.find(';\n\tdataSetsNettedYear = ')].replace('dataSetsYear = ', '')
        self.dataSetsYear = eval(parse_data_sets_year)

        # Check the response status code to see if the login was successful
        if login_response.status_code == 200:
            print("Login successful")
        else:
            print("Login failed")

        self.session = session

    def logout(self):
        """
        Closes the session by logging out
        :return: the response object
        """
        r = self.session.get("https://meter.katterno.fi/index.php?logout&amp;lang=FIN")
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

    def _get_latest_day(self):
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

        # if hour now is smaller than 8 then get two days later
        if datetime.datetime.now().hour < 8:
            self._days_later = 2
        else:
            self._days_later = 1


        # date_yesterday = datetime.date.today() - datetime.timedelta(days=self._days_later)

        self.latest_day = datetime.date.today() - datetime.timedelta(days=self._days_later)
        self.consumption_params = {'date-year':  self.latest_day.year,
                                  'date-month':  self.latest_day.month,
                                  'date-day':  self.latest_day.day
                                  }
        self.latest_day_electricity_consumption = None
        self.latest_day_electricity_prices = None

    def _check_session(self):

        import time
        if self.session is None:
            self.login()
        elif (time.time() - self.session.cookies._now) / (60 * 60) > 1:
            self.login()
        else:
            return


    def update_latest_day(self):
        """
        Updates the latest day consumption and calculates the
        average price for that day. If the user session is not
        established, it logs in to create a session before proceeding.

        :raises RuntimeError: If authentication fails during login,
            this will lead to an inability to establish a session for the
            operation.
        """

        self._check_session()
        self._get_latest_day()
        self.calculate_avg_price(granularity='D')

    def update_latest_month(self):
        if self.latest_day is None:
            self._get_latest_day()
        start_day = datetime.date(year=self.latest_day.year,month=self.latest_day.month,day=1)

        last_day = self.latest_day
        self.latest_month =f"{self.latest_day.year}/{self.latest_day.month}"

        # loop dates between start_day to self._get_latest_day() store results to pd df
        self.get_specific_day_consumption(start_day)
        month_df = self.single_day_consumption
        fetch_day = start_day + datetime.timedelta(days=1)

        while fetch_day <= last_day:

            self.get_specific_day_consumption(fetch_day)
            month_df = pd.concat([month_df, self.single_day_consumption], axis=0)
            fetch_day = fetch_day + datetime.timedelta(days=1)

        month_prices = self.get_electricity_prices(self.apikey,start_day, last_day)

        self.calculate_avg_price(consumption=month_df, prices=month_prices, granularity='ME')

        # filter self.latest_day prices and consumption from months dataframes
        self.latest_day_electricity_prices = month_prices[month_prices['timestamp_tz'].dt.date == self.latest_day]
        self.latest_day_electricity_consumption = month_df[month_df['timestamp_tz'].dt.date == self.latest_day]
        self.calculate_avg_price(consumption=self.latest_day_electricity_consumption,
                                 prices=self.latest_day_electricity_prices,
                                 granularity='D')


        return

    def update_latest_year(self):

        return

    def get_latest_day_consumption(self):

        return self.get_consumption(None)

    def get_specific_day_consumption(self, date):

        if not isinstance(date, datetime.date):
            date = datetime.date(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]))

        self.consumption_params = {'date-year': date.year,
                  'date-month': date.month,
                  'date-day': date.day
                  }

        if date < datetime.date.today():

            return self.get_consumption(self.consumption_params)
        else:
            raise ValueError

    def get_consumption(self, params=None):
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
            print(f"No parameter given, get {self.latest_day} days consumption")
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
            print(f"Getting consumption for {get_param}")

        self._check_session()
        r = self.session.get(
            "https://meter.katterno.fi//consumption.php",
            params=params)

        parse_data_sets = r.text[r.text.find('dataSetsSingleConsumption = '):r.text.find(
            ';\n\tdataSetsSingleConsumptionNetted = ')].replace('dataSetsSingleConsumption = ', '')
        data_sets_single_consumption = eval(parse_data_sets)

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

                hours = get_hours(day)

                # Create consumption DataFrame with timezone-aware timestamps
                consumption_df = pd.DataFrame({'timestamp_tz': hours, 'consumption': data_sets_single_consumption[0]})
                self.latest_day_electricity_consumption = consumption_df
            else:
                
                if granularity == 'D':

                    day = datetime.date(params.get('date-year'),params.get('date-month'),params.get('date-day'))
    
                    hours = get_hours(day)
    
                    # Create consumption DataFrame with timezone-aware timestamps
                    consumption_df = pd.DataFrame({'timestamp_tz': hours, 'consumption': data_sets_single_consumption[0]})
    
                    self.single_day_consumption = consumption_df
                elif granularity == 'M':
                    # todo handle month days
                    consumption_df = pd.DataFrame({'days': [], 'consumption': data_sets_single_consumption[0]})

                else:
                    # todo handle year months
                    consumption_df = pd.DataFrame({'months': [], 'consumption': data_sets_single_consumption[0]})

            return data_sets_single_consumption[0]
        else:
            print(f"No data available for given parameters: {params}")

    def get_specific_day_avg_price(self, date):

        self.get_specific_day_consumption(date)
        return self.calculate_avg_price(consumption=self.single_day_consumption,
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
            self.month_group_calculations = grouped

        if granularity == 'D':
            self.day_group_calculations = grouped

        return grouped


    def calculate_avg_price(self, consumption=None, prices=None, granularity=None):

        """
        Calculates the average electricity price based on consumption and price data inputs
        over specified time granularities. Merges the consumption data with price data, computes
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
                self.get_consumption()
                self.get_latest_day_prices()
            granularity='D'
            consumption = self.latest_day_electricity_consumption
            prices = self.latest_day_electricity_prices

        if consumption is None:
            print(f"Can't do electricity calculations no consumption data available ")
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
            self.latest_day_avg_price_with_alv = float(grouped['day_avg_price_alv'].iloc[0])
            self.latest_day_avg_price_by_avg_spot = float(grouped['day_avg_price_with_avg_spot'].iloc[0])
            self.latest_day_avg_spot_price_cent_with_vat = float(grouped['prices_cent_vat_avg'].iloc[0])
            self.latest_day_avg_khw_price_with_vat = float(grouped['day_avg_khw_price_with_alv'].iloc[0])

        if self.latest_month is not None and granularity == 'ME' and len(grouped) > 0:
            self.latest_month_electricity_consumption = float(grouped['consumption_sum'].iloc[0])
            self.latest_month_electricity_price_euro = float(grouped['price_marg_alv_euro_sum'].iloc[0])
            self.latest_month_optimization_savings_eur = float(grouped['month_optimization_savings_eur'].iloc[0])
            self.latest_month_optimization_efficiency = float(grouped['month_optimization_efficiency'].iloc[0])
            self.latest_month_avg_price_with_alv = float(grouped['month_avg_price_alv'].iloc[0])
            self.latest_month_avg_price_by_avg_spot = float(grouped['month_avg_price_with_avg_spot'].iloc[0])
            self.latest_month_avg_spot_price_cent_with_vat = float(grouped['prices_cent_vat_avg'].iloc[0])
            self.latest_month_avg_khw_price_with_vat = float(grouped['month_avg_khw_price_with_alv'].iloc[0])



        return price_calculations, grouped
        

    def get_latest_day_prices(self):
        self.latest_day_electricity_prices = self.get_electricity_prices(apikey=self.apikey)

        return self.latest_day_electricity_prices


    def get_specific_day_prices(self, date):


        if not isinstance(date, pd._libs.tslibs.timestamps.Timestamp):
            start = pd.Timestamp(int(date.split('-')[0]), int(date.split('-')[1]), int(date.split('-')[2]),tz='EET')
            start = start.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)
        else:
            start = date.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)


        return self.get_electricity_prices(self.apikey, start, end)


    @staticmethod
    def get_electricity_prices(apikey=None,start=None, end=None):
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

        from entsoe import EntsoePandasClient
        client = EntsoePandasClient(api_key=apikey,
                                    # Optional parameters:
                                    retry_count=3,
                                    retry_delay=5,
                                    timeout=60)

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
        if not isinstance(start, datetime.date):
            start = pd.Timestamp(start, tz='EET')
        if not isinstance(end, datetime.date):
            end = pd.Timestamp(end, tz='EET') + pd.Timedelta(hours=23, minutes=59, seconds=59)

        print(f" Get prices between {start} and {end}")

        country_code = 'FI'
        prices = client.query_day_ahead_prices(country_code, start=start, end=end)

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

