from connectors.crypto.connector.common.connector import CryptoConnector
import requests
import datetime
import hashlib
import urllib
import hmac
import json
import time

class ByBit(CryptoConnector):

    def __init__(self, api_key, api_secret, testing=False, passphrase=""):
        super().__init__(api_key, api_secret, testing)
        #TODO: Implement testing API
        self.base_url = "https://api.bybit.com"

    def get_products(self):
        response = self.__request("GET", "/v2/public/symbols")
        return self.__response(response)

    def get_product(self, symbol):
        raise Exception("Not implemented")

    def get_tickers(self):
        response = self.__request("GET", "/v2/public/tickers")
        return self.__response(response)

    def get_ticker(self, symbol):
        response = self.__request("GET", "/v2/public/tickers", payload={
            "symbol": symbol
        })
        return self.__response(response)[0]

    def get_orderbook(self, symbol):
        response = self.__request("GET", "/v2/public/orderBook/L2", payload={
            "symbol": symbol
        })
        return self.__response(response)

    def get_positions(self, product_id):
        #response = self.__request("GET", "/v2/positions", query={
        #                'product_id': product_id
        #           }, auth=True)
        #return self.__response(response)
        pass

    def get_wallet_balance(self, coin=""):
        if coin:
            payload = {"coin": coin}
        else:
            payload = {}

        response = self.__request("GET", "/v2/private/wallet/balance", payload=payload, auth=True)
        return self.__response(response)

    def get_order_history(self):
        response = self.__request("GET", "/v2/orders/history", auth=True)
        return self.__response(response)

    def place_order(self, symbol, size=1, side="Buy", order_type="Limit", time_in_force="GoodTillCancel"):
        response = self.__request("POST", "/v2/orders", {
                        "symbol": symbol,
                        "qty": size,
                        "side": side,
                        "order_type": order_type,
                        "time_in_force": time_in_force
                   }, auth=True)
        return self.__response(response)

    def __request(self, method, path, payload={}, auth=False, base_url=None, headers={}):
        if base_url is None:
            base_url = self.base_url

        url = '%s%s' % (base_url, path)

        headers['Content-Type'] = 'application/json'

        payload_str = self.__serialize(payload)

        if auth:
            if self.api_key is None or self.api_secret is None:
                raise Exception('Api_key or Api_secret missing')

            timestamp = self.__get_timestamp()
            payload['api_key'] = self.api_key
            payload['timestamp']  = timestamp
            payload["recv_window"] = 10000
            payload_str = self.__serialize(payload)
            signature = self.__generate_signature(payload_str)

            if method == "GET":
                payload_str += "&sign={}".format(signature)
            else:
                payload['sign'] = signature

        if method == "GET":
            response = self.session.request(method, f"{url}?{payload_str}", headers=headers)
        else:
            response = self.session.request(method, url, data=json.dumps(payload), headers=headers)
        return response

    def __response(self, response):
        if response.status_code != 200:
            raise requests.exceptions.HTTPError(response.status_code)
        else:
            response = response.json()
            if response['ret_code'] == 0:
                return response['result']
            else:
                raise requests.exceptions.HTTPError(response['ret_msg'])

    def __get_timestamp(self):
        return round(time.time()*1000)

    def __generate_signature(self, data=""):
        hash = hmac.new(self.api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

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
