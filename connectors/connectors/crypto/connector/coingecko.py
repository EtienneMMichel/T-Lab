from connectors.crypto.singleton import Singleton
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
import threading
import requests
import datetime
import time

class Coingecko(metaclass=Singleton):

    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.__mutex = threading.Lock()

        self.__coins = {}
        self.__get_coins()

    def get_historical_price(self, symbol,  date):
        if symbol not in self.__coins:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        coin_id = self.__coins[symbol]["id"]

        if isinstance(date, datetime.datetime) or isinstance(date, datetime.date):
            date = date.strftime('%d-%m-%Y')

        response = self.__request("GET", f"coins/{coin_id}/history", payload={
            "date": date,
            "localization": False,
        })
        return response

    def get_historical_ohcl(self, symbol):
        if symbol not in self.__coins:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        coin_id = self.__coins[symbol]

        response = self.__request("GET", f"coins/{coin_id}/ohlc", payload={
            "vs_currency":'usd',
            "days": 7,
        })
        return response

    def __get_coins(self, include_platform=False):
        response = self.__request("GET", f"coins/list", payload={
            "include_platform": include_platform,
        })

        for coin in response:
            symbol = coin["symbol"].upper()
            if symbol not in self.__coins:
                self.__coins[symbol] = {"id": coin["id"], "symbol": symbol, "name": coin["name"]}

        return response

    def __request(self, method, path, payload=None, headers=None):
        self.__mutex.acquire()
        try:
            if not headers:
                headers = {}
            if not payload:
                payload = {}

            url = f"{self.base_url}/{path}"

            tries = 0
            while tries < 3:
                if method == "GET":
                    payload_str = self.__serialize(payload)
                    response = self.session.request(method, f"{url}?{payload_str}", timeout=(3, 27), headers=headers)
                else:
                    response = self.session.request(method, url, params=payload, timeout=(3, 27), headers=headers)

                try:
                    parsed_response = self.__parse_response(response)
                    break
                except TooManyRequestsError:
                    timeout = tries*60
                    log(f"[CoinGecko] Too Many Requests. Waiting {timeout} seconds...")
                    time.sleep(timeout)
                    tries += 1

            if tries >= 3:
                raise TooManyRequestsError

            return parsed_response
        finally:
            self.__mutex.release()

    def __parse_response(self, response):
        status_code = response.status_code
        if response.status_code != 200:
            if status_code == 429:
                raise TooManyRequestsError("[CoinGecko] Too Many Requests.")
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(response.status_code)
            if "error" in response_json:
                raise requests.exceptions.HTTPError("{} : {}".format(response.status_code, response_json["error"]))
            else:
                raise requests.exceptions.HTTPError(response.status_code)
        else:
            response = response.json()
            return response

    def __serialize(self, data):
        body_str = ""
        for key in sorted(data.keys()):
            value = data[key]
            if isinstance(value, bool):
                if data[key]:
                    value = "true"
                else:
                    value = "false"
            body_str += f"{key}={value}&"
        body_str = body_str[:-1]
        return body_str