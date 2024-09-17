from urllib.parse import urlencode
import requests
import json

class CoinMarketCap:

    def __init__(self, api_key, public_key):
        self.base_url = "https://pro-api.coinmarketcap.com"
        self.api_key = api_key
        self.public_key = public_key
        self.session = requests.Session()

    def get_token_price(self, symbols, convert="USD"):
        data = {}
        if isinstance(symbols, list):
            symbols = ','.join(symbols)
        response = self.__api_get_latest_price(symbols, convert)

        print(response)

        for symbol in response.keys():
            if len(response[symbol]) > 0:
                data[symbol] = response[symbol][0]["quote"][convert]["price"]
            else:
                data[symbol] = None
        print(data)
        return data

    def __api_get_latest_price(self, symbol, currency="USD"):
        response = self.__request("GET", "/v2/cryptocurrency/quotes/latest", {
            "symbol": symbol,
            "convert": currency,
        })
        return response

    def __request(self, method, path, payload=None, headers=None):
        if not headers:
            headers = {}
        if not payload:
            payload = {}

        headers["X-CMC_PRO_API_KEY"] = self.api_key

        url = f"{self.base_url}{path}"

        if method == "GET":
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", timeout=(3, 27), headers=headers)
        else:
            response = self.session.request(method, url, params=payload, timeout=(3, 27), headers=headers)
        return self.__parse_response(response)

    def __parse_response(self, response):
        if response.status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(response.status_code)

            if response_json["status"] and response_json["status"]["error_message"]:
                raise requests.exceptions.HTTPError("{} : {}".format(response.status_code, response_json["status"]["error_message"]))
            else:
                raise requests.exceptions.HTTPError(response.status_code)
        else:
            response = response.json()
            return response["data"]