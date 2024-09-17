from connectors.crypto.common.logger import log
import requests
import datetime

class Forex:
    def __init__(self, database=None):
        self.database = database
        #self.base_url = "https://theforexapi.com/api/"
        self.base_url = "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1"
        self.gold_url = 'https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD'
        self.session = requests.Session()

    """
    def get_rate(self, base_currency, dest_currency, date=None):
        if base_currency == dest_currency:
            return 1
        if not date:
            date = datetime.date.today().strftime('%Y-%m-%d')
        else:
            if isinstance(date, datetime.datetime):
                date = date.date().strftime('%Y-%m-%d')
            elif isinstance(date, datetime.date):
                date = date.strftime('%Y-%m-%d')
            elif isinstance(date, str):
                date = date
            else:
                date = "latest"
        payload = {
            "base": base_currency,
            "symbols": dest_currency,
            "rtype": "fpy",
        }
        response = self.__request("GET", date, payload)
        if response and dest_currency in response["rates"]:
            return response["rates"][dest_currency]
        return None

    def __request(self, method, path, payload=None):
        if not payload:
            payload = {}

        url = f"{self.base_url}{path}"

        response = self.session.request(method, url, params=payload)

        if response.status_code == 200:
            return response.json()
        return None
    """

    def get_rate(self, base_currency, dest_currency, date=None):
        rate = None
        if base_currency == dest_currency:
            rate = 1
        else:
            if not date:
                date = datetime.date.today().strftime('%Y-%m-%d')
            else:
                if isinstance(date, datetime.datetime):
                    date = date.date().strftime('%Y-%m-%d')
                elif isinstance(date, datetime.date):
                    date = date.strftime('%Y-%m-%d')
                elif isinstance(date, str):
                    date = date
                else:
                    date = "latest"

            path = f"/{date}/currencies/{base_currency.lower()}/{dest_currency.lower()}.json"
            response = self.__request("GET", path)
            if response and dest_currency.lower() in response:
                rate = response[dest_currency.lower()]

        if self.database:
            if rate:
                self.__store_in_database(base_currency, dest_currency, rate)
            else:
                log(f"[ERROR][FOREX] Got empty rate for {base_currency}/{dest_currency}")
                rate = self.__get_from_database(base_currency, dest_currency)

        return rate

    def get_gold_rate(self):
        url = f"{self.gold_url}"
        response = self.session.request("GET", url)
        if response.status_code == 200:
            data = response.json()
            return data[0]['spreadProfilePrices'][0]['ask']


    def __request(self, method, path):
        url = f"{self.base_url}{path}"
        response = self.session.request(method, url)
        if response.status_code == 200:
            return response.json()
        return None

    def __store_in_database(self, base_currency, dest_currency, rate):
        now = datetime.datetime.now()
        rates = self.database.getTable("forex_rates", arg=f"WHERE base_currency = '{base_currency}' AND dest_currency = '{dest_currency}'")
        if rates.empty:
            self.database.execute("""INSERT INTO forex_rates (base_currency, dest_currency, rate, created_at, updated_at)
                                        VALUES(%s, %s, %s, %s, %s)""",
                                    params=(base_currency, dest_currency, rate, now, now))
        else:
            self.database.execute("""UPDATE forex_rates SET rate = %s, updated_at = %s
                                        WHERE base_currency = %s AND dest_currency = %s""",
                                    params=(rate, now, base_currency, dest_currency))

    def __get_from_database(self, base_currency, dest_currency):
        rates = self.database.getTable("forex_rates", arg=f"WHERE base_currency = '{base_currency}' AND dest_currency = '{dest_currency}'")
        if rates.empty:
            return None
        else:
            return rates["rate"].values[0]