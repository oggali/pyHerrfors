import requests
from secrets import secrets


class Herrfors:

    def __init__(self, username, password):
        """
        :param username: your e-mail address
        :param password: your password
        """
        self.usage_place = secrets.Herrfors.usage_place
        self.customer_number = secrets.Herrfors.customer_number
        self.session = None
        self.dataSetsYear = None
        pass

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

    def get_consumption(self, params=None):
        """
        Returns consumption based on parameters provided, if no parameters are provided then newest daily ones are returned
        :param params: the parameters to pass to the request,
        give date-year to get monthly consumption for given year,
        give date-month to get daily consumptions for given month and year,
        date-day to get daily hourly consumption

        :return: the consumption data
        """
        if params is None:
            import datetime
            date_now = datetime.date.today()
            params = {'date-year': date_now.year,
                      'date-month': date_now.month,
                      'date-day': date_now.day-1
                      }

        r = self.session.get(
            "https://meter.katterno.fi//consumption.php",
            params=params)

        parse_data_sets = r.text[r.text.find('dataSetsSingleConsumption = '):r.text.find(
            ';\n\tdataSetsSingleConsumptionNetted = ')].replace('dataSetsSingleConsumption = ', '')
        data_sets_single_consumption = eval(parse_data_sets)

        # sum elements inside list for check
        check_sum = sum(data_sets_single_consumption[0])

        if check_sum > 0:
            return data_sets_single_consumption[0]
        else:
            print(f"No data available for given parameters: {params}")

    @staticmethod
    def get_electricity_prices(start=None, end=None):
        from entsoe import EntsoePandasClient
        client = EntsoePandasClient(api_key=secrets.Entsoe.api_key,
                                    # Optional parameters:
                                    retry_count=3,
                                    retry_delay=5,
                                    timeout=60)

        if start is None:
            import pandas as pd
            start = pd.Timestamp.now(tz='EET') - pd.Timedelta(days=1)
            start = start.normalize()
            end = start + pd.Timedelta(hours=23, minutes=59, seconds=59)

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

