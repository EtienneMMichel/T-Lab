from connectors.crypto.connector.exchanges.coinbase import CbForex
from urllib.parse import urlencode
import requests
import datetime
import json

class MevCapital:

    def __init__(self, api_key, database=None, base_currency="USD"):
        self.api_key = api_key
        self.database = database
        self.base_currency = base_currency
        self.base_url = "https://api.mevcapital.com"

        self.session = requests.Session()

    def get_hedging(self):
        response = self.__request("GET", "/fxhedge", auth=True)
        return response["data"]

    def get_hedging_sum(self):
        total_hedging = 0
        response = self.get_hedging()
        for hedging in response:
            total_hedging += hedging["mtm"]
        return total_hedging

    def get_balance(self, as_base_currency=False):
        response = self.__request(f"GET", f"/performance", auth=True)

        if as_base_currency:
            rate = CbForex(self.database).get_rate("USD", self.base_currency)
        else:
            rate = 1

        data = {}
        symbol = 'USDC'
        data[symbol] = {}
        data[symbol]["symbol"] = symbol
        data[symbol]["balance"] = float(response["SMA_positions_summary"][0]['totalAuM'])*rate
        data[symbol]["available_balance"] = 0
        data[symbol]["currency"] = self.base_currency
        print('data',data)
        return data

    def __request(self, method, path, payload=None, headers=None, auth=False):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        if auth:
            payload["UID"] = self.api_key

        if method == "GET":
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", headers=headers)
        else:
            response = self.session.request(method, url, data=json.dumps(payload), headers=headers)

        self.__request_log(response)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            raise requests.exceptions.HTTPError(f"{status_code} : {response.text}")
        else:
            return response.json()

    def __request_log(self, response):
        try:
            file_path = "mev_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}] [{self}] [TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}] [{self}] [FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except Exception as e:
            pass

    def __str__(self):
        return self.__class__.__name__