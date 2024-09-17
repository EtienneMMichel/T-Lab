from connectors.crypto.connector.common.connector import CryptoConnector
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
import requests
from connectors.crypto.connector.core import is_stable_coin, is_fiat
import datetime
import hashlib
import hmac
import json
import time

class Bitmex(CryptoConnector):
#class Bitmex:
    PLATFORM_ID = 15
    PLATFORM_NAME = "Bitmex"

    def __init__(self, api_key, api_secret, testnet=False, strategy=None, passphrase=""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.session = requests.Session()

        if self.testing:
            self.base_url = "https://testnet.bitmex.com"
        else:
            self.base_url = "https://www.bitmex.com"

        self.__products = {}

        self.asset_multipliers = {}
        assets = self.__api_get_assets()
        for asset in assets:
            self.asset_multipliers[asset["currency"].upper()] = asset["scale"]

######################
### Public methods ###
######################

################
### REST API ###
################

    def is_ip_authorized(self):
        #TODO
        return True

    def has_read_permission(self):
        #TODO
        pass

    def has_write_permission(self):
        #TODO
        pass

    def has_withdraw_permission(self):
        #TODO
        pass

    def has_future_authorized(self):
        #TODO
        pass

    def get_assets(self):
        return self.__api_get_assets()

    def get_balance(self, as_usd=False, from_database=False):
        data = {}

        #response = self.__api_get_wallet()
        response = self.__api_get_margins()
        print(response)
        for asset_balance in response:
            if asset_balance["walletBalance"] > 0:
                symbol = asset_balance["currency"].upper()

                #TODO: Compute as USD amount here

                """
                conversion_factor = 1
                if symbol == 'USDT':
                    conversion_factor = 100000000
                if symbol == 'BTC':
                    conversion_factor = 1000000
                if symbol == 'XBT':
                    conversion_factor = 100000000
                """

                if symbol in self.asset_multipliers:
                    conversion_factor = 10**self.asset_multipliers[symbol]
                else:
                    conversion_factor = 1

                if as_usd and not is_stable_coin(symbol) and not is_fiat(symbol):
                    balance = ((asset_balance["walletBalance"] + asset_balance["unrealisedPnl"]) / conversion_factor)*self.get_mark_price(symbol+'_PERP_USD')["mark_price"]
                else:
                    balance = (asset_balance["walletBalance"] + asset_balance["unrealisedPnl"]) / conversion_factor

                data[symbol] = {
                    "symbol": symbol,
                    "balance": balance,
                    "available_balance": asset_balance["availableMargin"] / conversion_factor,
                    "currency": "USDT" if as_usd else symbol,
                }
        return data

    def get_profit(self, buy_crypto_history=None):
        #TODO
        pass

    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}

            products = self.__api_get_active_instrument()
            for product in products:

                if product["state"] != "Open":
                    continue

                generated_symbol = self.__compose_product_symbol(product)
                if generated_symbol == None:
                    continue
                if generated_symbol in data:
                    continue

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["symbol"],
                    "exchange_symbol": product["symbol"],
                    "contract_type": self.__compose_contract_type(product),
                    "strike_price": None,
                    "settlement_date": None,
                    "settlement_time": None,
                    "duration": None,
                    "tick_size": product["tickSize"],
                    #TODO: What is contract_size here ?
                    "contract_size": 1,
                    #TODO: What is min_notional here ?
                    "min_notional": product["lotSize"],
                    "base_asset_id": product["underlying"],
                    "base_asset_symbol": product["underlying"],
                    "quote_asset_id": product["quoteCurrency"],
                    "quote_asset_symbol": product["quoteCurrency"],
                }

                data[generated_symbol] = product_refactored
                self.__products[generated_symbol] = product_refactored

            return data
        else:
            return self.__products

    def get_product(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            return self.__products[symbol]

    def get_orderbook(self, symbol, pricing_client=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_orderbook(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_symbol,
            "exchange_symbol": exchange_symbol,
            "buy": [],
            "sell": [],
        }

        for order in response:
            order_refactored = {
                "price": order["price"],
                "size": order["size"],
            }
            if order["side"] == "Sell":
                data["buy"].append(order_refactored)
            else:
                data["sell"].append(order_refactored)

        return data

    def get_spot_price(self, symbol):
        #TODO:
        pass

    def get_mark_price(self, symbol, pricing_client=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_instrument(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "mark_price": response[0]["midPrice"]
        }

        return data

    def get_greeks(self, symbol):
        #TODO
        pass

    def get_open_interest(self, symbol):
        #TODO
        pass

    def get_positions(self):
        if not self.__products:
            self.get_products()

        data = {}
        response = self.__api_get_position()
        for position in response:

            product = [product for product in self.__products.values() if product["exchange_symbol"] == position["symbol"]][0]
            symbol = product["symbol"]

            data[symbol] = {
                "symbol": symbol,
                "exchange_id": product["exchange_id"],
                "exchange_symbol": product["exchange_symbol"],
                "size": position["currentQty"],
                "entry_price": position["avgEntryPrice"],
                "maintenance_margin": position["maintMargin"],
                "contract_type": product["contract_type"],
                "base_asset_id": product["base_asset_id"],
                "base_asset_symbol": product["base_asset_symbol"],
                "quote_asset_id": product["quote_asset_id"],
                "quote_asset_symbol": product["quote_asset_symbol"],
            }
        #TODO: Add spot positions using balance here ??
        return data

    def get_position(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_position(exchange_symbol)

        if len(response) == 0:
            data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "size": 0,
            "entry_price": 0,
            "liquidation_price": 0,
            "size_underlying": 0,
            "size_quote_ccy": 0,
        }
        else:
            if response[0]["liquidationPrice"] is None:
                liquidation_price = 0
            else:
                liquidation_price = response[0]["liquidationPrice"]
            data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "size": response[0]["currentQty"],
            "entry_price": response[0]["avgEntryPrice"],
            "liquidation_price": liquidation_price,
            "size_underlying": response[0]["homeNotional"],
            "size_quote_ccy": response[0]["foreignNotional"],
        }

        return data

    def get_position_margin(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_position(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "auto_topup": "????",
            "margin": response[0]["maintMargin"],
            "entry_price": response["avgEntryPrice"],
            "liquidation_price": response[0]["liquidationPrice"],

        }
        return data

    def set_position_margin(self, symbol, delta_margin):
        #TODO
        pass

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_symbol, size, side, order_type="market_order", time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": response["orderID"],
            "type": "market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "side": response["side"].lower(),
            "size": response["orderQty"],
            "filled_size": response["cumQty"],
            "unfilled_size": response["leavesQty"],
            "average_price": response["avgPx"],
            "limit_price": None,
            "stop_price": None,
            "time_in_force": self.__compose_time_in_force(response),
            "commission": 0,
        }
        return data

    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0, reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        started_at = time.time()

        data = {
            "id": response["orderID"],
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = response["orderID"]
            order_placed = False
            elapsed_time = time.time() - started_at
            while elapsed_time < timeout:
                active_orders = self.get_active_orders(symbol)

                # Order placed
                if not any(order["id"] == order_id for order in active_orders):
                    order_placed = True
                    break

                elapsed_time = time.time() - started_at
                time.sleep(0.1)

            if not order_placed:
                data = self.cancel_order(symbol, order_id)
            else:
                data = self.get_order_state(data["id"])

        # Doesn't wait for order to be placed
        else:
            data["status"] = self.__compose_order_status(response)
            data["symbol"] = symbol
            data["exchange_id"] = exchange_id
            data["exchange_symbol"] = exchange_symbol
            data["side"] = response["side"].lower()
            data["size"] = response["orderQty"]
            data["filled_size"] = response["cumQty"]
            data["unfilled_size"] = response["leavesQty"]
            data["average_price"] = response["avgPx"]
            data["limit_price"] = response["price"]
            data["stop_price"] = response["stopPx"]
            data["time_in_force"] = self.__compose_time_in_force(response)
            data["commission"] = 0

        return data

    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_symbol, size, side, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": response["orderID"],
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "side": response["side"].lower(),
            "size": response["orderQty"],
            "filled_size": response["cumQty"],
            "unfilled_size": response["leavesQty"],
            "average_price": response["avgPx"],
            "limit_price": response["price"],
            "stop_price": response["stopPx"],
            "time_in_force": self.__compose_time_in_force(response),
            "commission": 0,
        }
        return data

    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_symbol, size, side, order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": response["orderID"],
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "side": response["side"].lower(),
            "size": response["orderQty"],
            "filled_size": response["cumQty"],
            "unfilled_size": response["leavesQty"],
            "average_price": response["avgPx"],
            "limit_price": response["price"],
            "stop_price": response["stopPx"],
            "time_in_force": self.__compose_time_in_force(response),
            "commission": 0,
        }
        return data

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.0075):
        #TODO
        pass

    def get_active_orders(self, symbol=None):
        if not self.__products:
            self.get_products()

        if symbol:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_id = self.__products[symbol]["exchange_id"]
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_orders(active=True)

        data = []
        for order in response:
            if symbol and order["symbol"] != exchange_symbol:
                continue
            active_order = {
                "id": order["orderID"],
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol,
                "exchange_id": order["symbol"],
                "exchange_symbol": order["symbol"],
                "side": order["side"].lower(),
                "size": order["orderQty"],
                "filled_size": order["cumQty"],
                "unfilled_size": order["leavesQty"],
                "average_price": order["avgPx"],
                "limit_price": order["price"],
                "stop_price": order["stopPx"],
                "time_in_force": self.__compose_time_in_force(order),
            }
            data.append(active_order)
        return data

    def get_order_state(self, symbol, order_id):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_orders(symbol=exchange_symbol, order_id=order_id)

        if len(response) == 0:
            raise OrderNotFoundError(order_id)
        order = response[0]

        data = {
            "id": order["orderID"],
            "type": self.__compose_order_type(order),
            "status": self.__compose_order_status(order),
            "symbol": symbol,
            "exchange_id": order["symbol"],
            "exchange_symbol": order["symbol"],
            "side": order["side"].lower(),
            "size": order["orderQty"],
            "filled_size": order["cumQty"],
            "unfilled_size": order["leavesQty"],
            "average_price": order["avgPx"],
            "limit_price": order["price"],
            "stop_price": order["stopPx"],
            "time_in_force": self.__compose_time_in_force(order),
        }
        return data

    def cancel_order(self, symbol, order_id):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_cancel_order(order_id)

        if len(response) == 0:
            raise OrderNotFoundError(order_id)
        order = response[0]

        if "error" in order:
            if order["error"] == "Unable to cancel order":
                raise UnableToCancelOrder(order_id)
            else:
                raise requests.exceptions.HTTPError(f"200 : {order['error']}")

        data = {
            "id": order["orderID"],
            "type": self.__compose_order_type(order),
            "status": self.__compose_order_status(order),
            "symbol": symbol,
            "exchange_id": order["symbol"],
            "exchange_symbol": order["symbol"],
            "side": order["side"].lower(),
            "size": order["orderQty"],
            "filled_size": order["cumQty"],
            "unfilled_size": order["leavesQty"],
            "average_price": order["avgPx"],
            "limit_price": order["price"],
            "stop_price": order["stopPx"],
            "time_in_force": self.__compose_time_in_force(order),
        }
        return data

    def edit_order(self, order_id, symbol, limit_price, size):
        #TODO
        pass

    def get_order_history(self, symbol=None):
        #TODO
        pass

    def get_leverage(self, symbol):
        #TODO
        pass

    def set_leverage(self, symbol, leverage):
        #TODO
        pass

    def get_wallet_transactions(self):
        #TODO
        pass

    def change_order_leverage(self, product_id):
        #TODO
        pass

    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        #TODO
        pass

    def get_candle(self, symbol, start_date, end_date):
        #TODO
        pass

    def get_candle_history(self, symbol, start_date, end_date, resolution):
        #TODO
        pass

    def get_margins(self):
        response = self.__api_get_margins()
        data = {}
        for margin in response:
            symbol = margin["currency"]
            data[symbol] = {
                "amount": margin["amount"],
                "initial_margin": margin["initMargin"],
                "maintenance_margin": margin["maintMargin"],
                "margin_balance": margin["marginBalance"],
                "leverage": margin["marginLeverage"],
                # Ici ajoute tous les champs dont tu as besoin en plus
            }
        return data

#################
### Websocket ##
#################

    def is_connection_lost(self):
        #TODO
        pass

    def is_connection_aborted(self):
        #TODO
        pass

    def set_reconnect_timeout(self, timeout):
        #TODO
        pass

    def set_abortion_datetime(self, datetime):
        #TODO
        pass

    def show_websocket_logs(self, show):
        #TODO
        pass

    def cleanup(self, force=False):
        #TODO
        pass

#######################
### Public channels ###
#######################

    def subscribe_orderbook(self, symbols, callback=None):
        #TODO
        pass

    def unsubscribe_orderbook(self, symbols, callback=None):
        #TODO
        pass

    def subscribe_mark_price(self, symbols, callback=None):
        #TODO
        pass

    def unsubscribe_mark_price(self, symbols, callback=None):
        #TODO
        pass

    def subscribe_funding_rate(self, symbols, callback=None):
        #TODO
        pass

    def unsubscribe_funding_rate(self, symbols, callback=None):
        #TODO
        pass

    def subscribe_volume(self, symbols, callback=None):
        #TODO
        pass

    def unsubscribe_volume(self, symbols, callback=None):
        #TODO
        pass

#########################
### Privates channels ###
#########################

    def subscribe_orders(self, symbols, callback=None):
        #TODO
        pass

    def unsubscribe_orders(self, symbols, callback=None):
        #TODO
        pass

    def subscribe_balances(self, callback=None):
        #TODO
        pass

    def unsubscribe_balances(self, callback=None):
        #TODO
        pass

    def subscribe_positions(self, callback=None):
        #TODO
        pass

    def unsubscribe_positions(self, callback=None):
        #TODO
        pass

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_assets(self):
        response = self.__request("GET", "/api/v1/wallet/assets")
        return response

    def __api_get_wallet(self):
        response = self.__request("GET", "/api/v1/user/wallet", payload={
            "currency": "all",
        }, auth=True)
        return response

    def __api_get_instrument(self, symbol=None):
        if symbol:
            response = self.__request("GET", "/api/v1/instrument", payload={
                "symbol": symbol,
            })
        else:
            response = self.__request("GET", "/api/v1/instrument")
        return response

    def __api_get_active_instrument(self):
        response = self.__request("GET", "/api/v1/instrument/active")
        return response

    def __api_get_orderbook(self, symbol):
        response = self.__request("GET", "/api/v1/orderbook/L2", payload={
            "symbol": symbol,
            "depth": 0,
        })
        return response

    def __api_get_position(self, symbol=None):
        if symbol:
            response = self.__request("GET", "/api/v1/position", payload={
                "filter": f'{{"symbol":"{symbol}"}}',
            }, auth=True)
        else:
            response = self.__request("GET", "/api/v1/position", auth=True)
        return response

    def __api_get_orders(self, symbol=None, order_id=None, active=False):
        data = {}

        if symbol:
            data["symbol"] = symbol
        if order_id:
            data["orderID"] = order_id
        if active:
            data["open"] = True

        if not data:
            payload = None
        else:
            payload = {"filter": json.dumps(data)}
        response = self.__request("GET", "/api/v1/order", payload=payload, auth=True)
        return response

    def __api_place_order(self, symbol, size, side, order_type, limit_price=None, stop_price=None, time_in_force="gtc", reduce_only=False):

        payload = {
            "symbol": symbol,
            "side": "Buy" if side == "buy" else "Sell",
            "orderQty": size,
        }

        if order_type == "market_order":
            payload["ordType"] = "Market"
        elif order_type == "limit_order":
            payload["order_type"] = "Limit"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            payload["order_type"] = "Stop"
            payload["stopPx"] = stop_price
        elif order_type == "stop_limit_order":
            payload["order_type"] = "StopLimit"
            payload["price"] = str(limit_price)
            payload["stopPx"] = str(stop_price)

        if time_in_force == "gtc":
            payload["timeInForce"] = "GoodTillCancel"
        elif time_in_force == "ioc":
            payload["timeInForce"] = "ImmediateOrCancel"
        elif time_in_force == "fok":
            payload["timeInForce"] = "FillOrKill"

        if reduce_only:
            payload["execInst"] = "ReduceOnly"

        response = self.__request("POST", "/api/v1/order", payload=payload, auth=True)
        return response

    def __api_cancel_order(self, order_id):
        response = self.__request("DELETE", "/api/v1/order", {
            "orderID": order_id,
        }, auth=True)
        return response

    def __api_get_margins(self):
        response = self.__request("GET", "/api/v1/user/margin", {
            "currency": "all",
        }, auth=True)
        return response

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        #headers["Accept"] = "application/json"

        if auth:
            request_path = f"{path}?{urlencode(payload)}" if payload else path
            data = ""

            expires = str(self.__get_timestamp() + 5)
            signature_data = method + request_path + expires + data
            signature = self.__generate_signature(signature_data)
            #print("-----------------------------------------")
            #print(f"verb={method}")
            #print(f"url={request_path}")
            #print(f"expires={expires}")
            #print(f"data={data}")
            #print(f"message={signature_data}")
            #print(f"signature={signature}")
            #print("-----------------------------------------")
            headers["api-expires"] = expires
            headers["api-key"] = self.api_key
            headers["api-signature"] = signature

        if method == "GET":
            response = self.session.request(method, f"{url}?{urlencode(payload)}", headers=headers)
        else:
            response = self.session.request(method, url, params=payload, headers=headers)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)
            raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["error"]["message"]))
        else:
            response_json = response.json()
            return response_json

    def __get_timestamp(self):
        d = datetime.datetime.utcnow()
        epoch = datetime.datetime(1970,1,1)
        return int((d - epoch).total_seconds())

    def __generate_signature(self, data):
        hash = hmac.new(self.api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    def __compose_order_type(self, response):
        order_type = "unknown"
        if response["ordType"] == "Market":
            order_type = "market"
        elif response["ordType"] == "Limit":
            order_type = "limit"
        elif response["ordType"] == "Stop":
            order_type = "stop_market"
        elif response["ordType"] == "StopLimit":
            order_type = "stop_limit"
        return order_type

    def __compose_order_status(self, response):
        order_status = "unknown"
        if response["ordStatus"] == "New":
            order_status = "open"
        elif response["ordStatus"] == "Filled":
            order_status = "filled"
        #TODO: handle partially filled orders
        elif response["ordStatus"] == "Canceled":
            order_status = "cancelled"
        return order_status

    def __compose_contract_type(self, product):
        contract_type = None
        # Perpetual future
        if product["typ"] == "FFWCSX":
            contract_type = "perpetual_future"
        #TODO: What is this ?
        elif product["typ"] == "FFCCSX":
            contract_type = None
        #TODO: Temporary skip futures
        # Future
        elif product["typ"] == "IFXXXP":
            contract_type = "future"
            contract_type = None
        # Spot
        elif product["typ"] == "FFWCSX":
            contract_type = "spot"
        else:
            contract_type = None
        return contract_type

    def __compose_product_symbol(self, product):
        base_asset_symbol = product["underlying"]
        quote_asset_symbol = product["quoteCurrency"]

        # Perpetual future
        if product["typ"] == "FFWCSX":
            generated_symbol = f"{base_asset_symbol}_PERP_{quote_asset_symbol}"
        #TODO: What is this ?
        elif product["typ"] == "FFWCSF":
            generated_symbol = None
        #TODO: Temporary skip futures
        # Future
        elif product["typ"] == "FFCCSX":
            generated_symbol = f"{base_asset_symbol}_FUT_{quote_asset_symbol}"
            generated_symbol = None
        # Spot
        elif product["typ"] == "IFXXXP":
            generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"
        else:
            generated_symbol = None

        return generated_symbol

    def __compose_time_in_force(self, order):
        time_in_force = "unknown"
        if order["timeInForce"] == "GoodTillCancel":
            time_in_force = "gtc"
        elif order["timeInForce"] == "ImmediateOrCancel":
            time_in_force = "ioc"
        elif order["timeInForce"] == "FillOrKill":
            time_in_force = "fok"
        return time_in_force
