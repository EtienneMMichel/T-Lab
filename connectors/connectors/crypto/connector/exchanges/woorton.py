from connectors.threading.Threads import StoppableThread, format_traceback
import connectors.crypto.connector.common.websocket_codes as op_codes
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.threading.Threads import format_traceback
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
from decimal import Decimal
import websocket
import threading
import requests
import datetime
import hashlib
import base64
import select
import pandas as pd
import hmac
import time
import json
import uuid
import copy


class Woorton:
    PLATFORM_ID = 11
    PLATFORM_NAME = "Woorton"

    def __init__(self, api_key, api_secret="", testnet=False, strategy=None, passphrase="", base_currency="USD"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.session = requests.Session()

        if self.testing:
            self.base_url = "https://api.uat.woorton.net"
            self.ws_url = "wss://socket.uat.woorton.net/quotes"
        else:
            self.base_url = "https://api.woorton.net"
            self.ws_url = "wss://socket.woorton.net/quotes"

        self.__products = {}
        self.__assets = {}

        self.__websocket_mutex = threading.Lock()
        self.__websockets = []
        self.__max_websocket = 3
        self.__websocket_number = 0
        self.__websocket_handlers_mutex = threading.Lock()
        self.__websocket_handlers = {}
        self.__websocket_channels_mutex = threading.Lock()
        self.__websocket_channels = {}

        self.__subscribe_mutex = threading.Lock()

        self.__receive_thread = None


        self.__show_websocket_logs = False

        self.subscribe_levels = {
            "BTC_USD": 1,
            "ETH_USD": 5,
            "SOL_USD": 50,
            "AVAX_USD": 50,
            "ADA_USD": 50,
            "DOGE_USD": 50,
            "XRP_USD": 50,
        }

######################
### Public methods ###
######################

################
### REST API ###
################

    def has_read_permission(self):
        pass

    def has_write_permission(self):
        pass

    def has_withdraw_permission(self):
        pass

    def has_future_authorized(self):
        pass

    def get_symbol_usd_price(self, symbol, date=None):
        return self.__get_symbol_usd_price(symbol, date)

    def get_balance(self, as_base_currency=True, as_eur=False):

        if not self.__products:
            self.get_products()

        if as_base_currency:
            rate = CbForex(self.database).get_rate('USD', self.base_currency)
        else:
            rate = 1
        data = {}
        balances = self.__api_get_balance()
        print('balances',balances)

        for asset, asset_balance in balances.items():
            if float(asset_balance) == 0:
                continue

            if as_base_currency:
                usd_price =  CbForex(self.database).get_rate(asset, 'USD')
                #usd_price = self.__get_symbol_usd_price(asset)
                balance = float(asset_balance)*usd_price*rate
                available_balance = float(asset_balance)*usd_price*rate
            else:
                balance = float(asset_balance)
                available_balance = float(asset_balance)
            data[asset] = {
                "symbol": asset,
                "balance": balance,
                "available_balance": available_balance,
                "currency": self.base_currency if as_base_currency else asset,
            }

        return data

    #TODO: To test
    def get_profit(self, buy_crypto_history=None):
        if not self.__products:
            self.get_products()

        total_balance = 0
        total_deposit = 0
        total_withdraw = 0

        # Compute balances
        balances = self.get_balance(as_base_currency=True)
        for balance in balances.values():
            total_balance += balance["balance"]

        feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
        feeder_cashflows_transaction_ids = feeder_cashflows["last_tx_id"].values
        internal_deposit_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_deposit'")
        internal_deposit_transaction_ids = internal_deposit_cashflows["last_tx_id"].values
        internal_withdraw_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_withdraw'")
        internal_withdraw_transaction_ids = internal_withdraw_cashflows["last_tx_id"].values

        # Compute deposits and withdrawals
        transaction_history = self.get_deposit_withdraw_history()
        for transaction in transaction_history:
            if transaction['operation_id'] in feeder_cashflows_transaction_ids and not self.strategy.is_feeder_investable():
                #print(f"### Skip transaction by feeder id")
                continue
            if transaction['operation_id'] in internal_deposit_transaction_ids:
                #print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['operation_id'] in internal_withdraw_transaction_ids:
                #print(f"### Skip transaction by internal withdraw id")
                continue

            if transaction["type"] == "deposit":
                total_deposit += transaction["base_currency_amount"]
            else:
                total_withdraw += transaction["base_currency_amount"]

        #total_balance = total_balance + total_unrealized + total_premium
        total_profit = total_balance - total_deposit + total_withdraw

        data = {
            "total_balance": total_balance,
            "total_deposit": total_deposit,
            "total_withdraw": total_withdraw,
            "total_profit": total_profit,
        }
        return data

    def get_products(self, reload=False):
        if not self.__products or reload:
            self.__products = {}
            products = self.__api_get_instruments()
            for product in products:
                base_asset_symbol = product["underlier"].split("_")[0]
                quote_asset_symbol = product["underlier"].split("_")[1]
                generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["underlier"],
                    "exchange_symbol": product["name"],
                    "contract_type": "spot",
                    "contract_size": 1,
                    "strike_price": None,
                    "settlement_date": None,
                    "settlement_time": None,
                    "duration": None,
                    "precision": {
                        "amount": product["quantity_precision"],
                        "price": 10**-product["price_significant_digits"],
                        "notional": None,
                    },
                    "limits": {
                        "amount": {
                            "min": product["min_quantity_per_trade"],
                            "max": product["max_quantity_per_trade"]},
                        "price": {
                            "min": 10**-product["price_significant_digits"],
                            "max": None},
                        "notional": {
                            "min": None,
                            "max": None},
                    },
                    "base_asset_id": base_asset_symbol,
                    "base_asset_symbol": base_asset_symbol,
                    "quote_asset_id": quote_asset_symbol,
                    "quote_asset_symbol": quote_asset_symbol,
                }
                self.__products[generated_symbol] = product_refactored
        return self.__products

    def get_product(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            return self.__products[symbol]

    def get_orderbook(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_orderbook(exchange_id)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "buy": [],
            "sell": [],
        }

        for order in response[exchange_symbol]["bids"]:
                order_refactored = {
                    "price": float(order[0]),
                    "size": float(order[1]),
                }
                data["buy"].append(order_refactored)
        for order in response[exchange_symbol]["asks"]:
            order_refactored = {
                "price": float(order[0]),
                "size": float(order[1]),
            }
            data["sell"].append(order_refactored)

        return data

    def get_mark_price(self, symbol, pricing_client=None):
        if not self.__products:
            self.get_products()

        if symbol not in self.__products and symbol not in self.__assets:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        asset = symbol.split("_")[0]
        price = CbForex(self.database).get_rate(asset, 'USD')
        data = {
            "symbol": symbol,
            "exchange_id": self.__products[symbol]["exchange_id"],
            "exchange_symbol": self.__products[symbol]["symbol"],
            "mark_price": price,
        }

        return data

    def get_positions(self):
        rate = CbForex(self.database).get_rate('USD', self.base_currency)
        data = {}
        balances = self.__api_get_balance()
        print('balances',balances)

        for asset, asset_balance in balances.items():

            if float(asset_balance) == 0:
                continue

            usd_price =  CbForex(self.database).get_rate(asset, 'USD')

            data[asset] = {
                "symbol": asset,
                "exchange_id": asset,
                "exchange_symbol": asset,
                "size": float(asset_balance),
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": asset,
                "base_asset_symbol": asset,
                "quote_asset_id": self.base_currency,
                "quote_asset_symbol": self.base_currency,
                "base_currency_amount": float(asset_balance)*usd_price*rate,
                "base_currency_amount": float(asset_balance)*usd_price*rate,
                "base_currency_symbol": self.base_currency,
            }

        return data

    def get_rfq(self, symbol, quantity, side):
        data = {}

        response = self.__api_request_for_quote(symbol, quantity, side)
        print(response)
        sys.exit(1)

        for asset, asset_balance in balances:
            usd_price = CbForex(self.database).get_rate(asset, 'USD')
            data[asset] = {
                "symbol": asset,
                "exchange_id": asset,
                "exchange_symbol": asset,
                "size": asset_balance,
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": asset,
                "base_asset_symbol": asset,
                "quote_asset_id": self.base_currency,
                "quote_asset_symbol": self.base_currency,
                "base_currency_amount": asset_balance*usd_price*rate,
                "base_currency_symbol": self.base_currency,
            }

        return data

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)

        response = self.__api_place_order(exchange_symbol, side, size, order_type="market_order")

        order_id = response['trades'][0]["order"]

        time.sleep(1)

        data = self.get_order_state(symbol, order_id)

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], data["average_price"])

        return data

    def place_fill_or_killed_order(self, symbol, size, side, price, slippage=1, time_in_force="gtc", timeout=0, reduce_only=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        price = self.__round_price(symbol, price)

        response = self.__api_place_order(exchange_symbol, side, size, order_type="fill_or_killed", slippage=slippage, limit_price = price)
        print(response)
        order_id = response['order_id']

        time.sleep(2)

        data = self.get_order_state(symbol, order_id)

        print('LAAAAA')

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], price)

        return data

    def get_active_orders(self, symbol=None):
        if not self.__products:
            self.get_products()
        if symbol:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_id = self.__products[symbol]["exchange_id"]
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = []
        response = self.__api_get_open_orders()
        for order_id in response["open"]:
            order = response["open"][order_id]
            pair = order["descr"]["pair"]
            if symbol and exchange_symbol != pair:
                continue
            else:
                active_order = {
                    "id": str(order_id),
                    "type": self.__compose_order_type(order),
                    "status": self.__compose_order_status(order),
                    "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_symbol"] == pair), None),
                    "exchange_id": pair,
                    "exchange_symbol": pair,
                    "side": order["descr"]["type"],
                    "size": float(order["vol"]),
                    "filled_size": float(order["vol_exec"]),
                    "unfilled_size": float(order["vol"]) - float(order["vol_exec"]),
                    "average_price": float(order["price"]),
                    "limit_price": float(order["limitprice"]) if "limitprice" in order else None,
                    "stop_price": float(order["stopprice"]) if "stopprice" in order else None,
                    "time_in_force": "???",
                    "commission": float(order["fee"]) if "fee" in order else None,
                    "created_at": datetime.datetime.fromtimestamp(int(order["opentm"])),
                    "updated_at": datetime.datetime.fromtimestamp(int(order["closetm"])) if "closetm" in order and order["closetm"] != 0 else datetime.datetime.fromtimestamp(int(order["opentm"])),
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

        try:
            response = self.__api_get_order_state(order_id)
        except:
            response = self.__api_trade_state(order_id)
        print('RESPONSE',response)
        print('ICUI')
        data = {
            "id": str(order_id),
            "type": 'market' if response[0]['order_type'] =='MKT' else 'fill_or_kill',
            "status": self.__compose_order_status(response[0]),
            "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_symbol"] == response[0]['instrument']), None),
            "exchange_id": response[0]['instrument'],
            "exchange_symbol": response[0]['instrument'],
            "side": response[0]['side'],
            "size": float(response[0]['quantity']),
            "filled_size": float(response[0]['quantity']),
            "unfilled_size": 0,
            "average_price": float(response[0]['executed_price']) if response[0]['executed_price'] else None,
            "limit_price": None,
            "stop_price": None,
            "time_in_force": "???",
            "commission": None,
            "created_at": datetime.datetime.strptime(response[0]["created"][:19], "%Y-%m-%dT%H:%M:%S"),
            "updated_at": datetime.datetime.strptime(response[0]["created"][:19], "%Y-%m-%dT%H:%M:%S"),
        }
        print('data',data)

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

        time.sleep(0.1)

        data = self.get_order_state(symbol, order_id)
        return data

    def get_order_history(self, symbol=None):
        if not self.__products:
            self.get_products()

        data = self.get_active_orders(symbol)

        closed_orders = {}
        response = self.__api_get_closed_orders()
        closed_orders.update(response["closed"])

        offset = 0
        while len(response["closed"]) >= 50:
            offset += 50
            response = self.__api_get_closed_orders(offset=offset)
            closed_orders.update(response["closed"])

        for order_id, order in closed_orders.items():
            pair = order["descr"]["pair"]
            data.append({
                "id": str(order_id),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_symbol"] == pair), None),
                "exchange_id": pair,
                "exchange_symbol": pair,
                "side": order["descr"]["type"],
                "size": float(order["vol"]),
                "filled_size": float(order["vol_exec"]),
                "unfilled_size": float(order["vol"]) - float(order["vol_exec"]),
                "average_price": float(order["price"]),
                "limit_price": float(order["limitprice"]) if "limitprice" in order else None,
                "stop_price": float(order["stopprice"]) if "stopprice" in order else None,
                "time_in_force": "???",
                "commission": float(order["fee"]) if "fee" in order else None,
                "created_at": datetime.datetime.fromtimestamp(int(order["opentm"])),
                "updated_at": datetime.datetime.fromtimestamp(int(order["closetm"])) if "closetm" in order and order["closetm"] != 0 else datetime.datetime.fromtimestamp(int(order["opentm"])),
            })

        return data

    def get_trade_history(self, symbol, start_date, end_date):
        data = []

        start_timestamp = int(start_date.timestamp())
        stop_timestamp = int(end_date.timestamp())

        trades = {}
        response = self.__api_get_trade_history(start_timestamp, stop_timestamp)
        trades.update(response["trades"])

        offset = 0
        while len(response["trades"]) >= 50:
            offset += 50
            response = self.__api_get_trade_history(start_timestamp, stop_timestamp, offset=offset)
            trades.update(response["trades"])

        for order_id, trade in trades.items():
            data.append({
                "id": order_id,
                "symbol": symbol,
                "exchange_id": trade["pair"],
                "exchange_symbol": trade["pair"],
                "side" : trade["type"],
                "size": trade["vol"],
                "price": trade["price"],
                "datetime": datetime.datetime.fromtimestamp(int(trade['time']))
            })

        return data

    def get_transaction_history(self):
        data = []

        operations = {}
        response = self.__api_get_ledgers_info()
        operations.update(response["ledger"])

        offset = 0
        while len(response["ledger"]) >= 50:
            offset += 50
            response = self.__api_get_ledgers_info(offset=offset)
            operations.update(response["ledger"])

        for operation in operations.values():
            date = datetime.datetime.fromtimestamp(operation["time"])
            rate = CbForex(self.database).get_rate('USD', self.base_currency, date)
            usd_amount = CbForex(self.database).get_rate(self.base_currency, 'USD', date)
            base_currency_amount = float(operation["amount"])*rate*usd_amount

            data.append({
                "date": date,
                "type": operation["type"],
                "source_symbol": operation["asset"],
                "source_amount": float(operation["amount"]),
                "destination_symbol": operation["asset"],
                "destination_amount": float(operation["amount"]),
                "fees": float(operation["fee"]),
                "operation_id": operation["refid"],
                "wallet_address": None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
            })

        return data

    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        data = []

        response = self.__api_get_ledgers_info()

        for operation in response:

            date = datetime.datetime.strptime(operation['created'][:19], "%Y-%m-%dT%H:%M:%S")
            rate = CbForex(self.database).get_rate('USD', self.base_currency, date)
            usd_amount = CbForex(self.database).get_rate(self.base_currency, 'USD', date)
            base_currency_amount = float(operation["amount"])*rate*usd_amount
            data.append({
                "date": date,
                "type": "deposit",
                "source_symbol": operation["currency"],
                "source_amount": abs(float(operation["amount"])),
                "destination_symbol": operation["currency"],
                "destination_amount": abs(float(operation["amount"])),
                "fees": 0,
                "operation_id": operation["transaction_id"],
                "wallet_address": None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
            })

        return data

#################
### Websocket ###
#################

    def show_websocket_logs(self, show):
        self.__show_websocket_logs = show

    def cleanup(self, force=False):
        symbols = []
        with self.__websocket_mutex:
            with self.__websocket_channels_mutex:
                for ws in self.__websocket_channels.keys():
                    symbols.extend(self.__websocket_channels[ws]["symbols"])

        self.__unsubscribe_dispatcher(symbols, None)

#######################
### Public channels ###
#######################

    def subscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            #else:
            #    exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__subscribe_dispatcher(symbols, callback=callback)

    def unsubscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            #else:
            #    exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__unsubscribe_dispatcher(symbols, callback=callback)

        # Cleanup associated data
        #for symbol in symbols:
        #    if symbol in self.__orderbooks:
        #        del self.__orderbooks[symbol]

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_balance(self):
        response = self.__request("GET", f"/balance", auth=True)
        return response

    def __api_get_instruments(self):
        response = self.__request("GET", f"/instruments", auth=True)
        return response

    def __api_get_orderbook(self, symbol, count=None):
        payload = {
            "pair": symbol,
        }
        if count:
            payload["count"] = count
        response = self.__request("GET", f"/0/public/Depth", payload=payload)
        return response

    def __api_get_ticker_information(self, symbol):
        response = self.__request("GET", f"/0/public/Ticker", payload={"pair": symbol})
        return response

    def __api_get_recent_trades(self, symbol, timestamp=None, count=None):
        payload = {
            "pair": symbol,
        }
        if timestamp:
            payload["since"] = timestamp
        if count:
            payload["count"] = count
        response = self.__request("GET", "/0/public/Trades", payload=payload)
        return response

    def __api_request_for_quote(self, symbol, quantity=None, side=None):
        client_rfq_id = str(uuid.uuid4())
        payload = {
            "instrument": symbol,
            "side": side,
            "quantity": quantity,
            "client_rfq_id": client_rfq_id,
        }
        print('payload',payload)
        response = self.__request("POST", "/request_for_quote/", payload=payload, auth=True)
        return response

    def __api_place_order(self, symbol, side, size, order_type, slippage=1, limit_price=None):
        client_order_id = str(uuid.uuid4())
        valid_until = datetime.datetime.strftime(datetime.datetime.utcnow() + datetime.timedelta(seconds=10), '%Y-%m-%dT%H:%M:%S.%fZ')

        payload = {
            "instrument": symbol,
            "client_order_id": client_order_id,
            "side": side,
            "quantity": size,
            "valid_until": valid_until,
        }

        if order_type == "market_order":
            payload["order_type"] = "MKT"
        elif order_type == "fill_or_killed":
            payload["order_type"] = "FOK"
            payload["price"] = str(limit_price)
            payload["acceptable_slippage_in_basis_points"] = str(slippage)
        elif order_type == "stop_market_order":

            return
        elif order_type == "stop_limit_order":

            return

        # if time_in_force:
        #     payload["time_in_force"] = time_in_force.upper()

        response = self.__request("POST", "/v2/order/", payload=payload, auth=True)

        return response

    def __api_get_open_orders(self):
        response = self.__request("POST", "/0/private/OpenOrders", auth=True)
        return response

    def __api_get_closed_orders(self, start_timestamp=None, stop_timestamp=None, offset=0):
        payload = {}
        if start_timestamp:
            payload["start"] = start_timestamp
        if stop_timestamp:
            payload["end"] = stop_timestamp
        if offset:
            payload["ofs"] = offset
        response = self.__request("POST", "/0/private/ClosedOrders", payload=payload, auth=True)
        return response

    def __api_get_order_state(self, order_id):
        print('order_id',order_id)
        response = self.__request("GET", f"/order/{order_id}", auth=True)
        return response

    def __api_trade_state(self, trade_id):
        print('trade_id',trade_id)
        response = self.__request("GET", f"/trade/{trade_id}", auth=True)
        return response

    def __api_cancel_order(self, order_id):
        response = self.__request("POST", "/0/private/CancelOrder", {
           "txid": str(order_id),
        }, auth=True)
        return response

    def __api_get_trade_history(self, start_timestamp=None, stop_timestamp=None, offset=0):
        payload = {}
        if start_timestamp:
            payload["start"] = start_timestamp
        if stop_timestamp:
            payload["end"] = stop_timestamp
        if offset:
            payload["ofs"] = offset
        response = self.__request("POST", "/0/private/TradesHistory", payload=payload, auth=True)
        return response

    def __api_get_ledgers_info(self):

        response = self.__request("GET", "/ledger/", auth=True)
        return response

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        if auth:
            headers["Authorization"] = f"Token {self.api_key}"

        print(method)
        print(url)

        if method == "GET":
            print("AAAAAAAAAAAAAAAAAAAAaa")
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", headers=headers)
        else:
            print("BBBBBBBBBBBBBBBBBBBBBbb")
            #response = self.session.request(method, f"{url}", data=json.dumps(payload), headers=headers, allow_redirects=False)
            response = self.session.request(method, f"{url}", json=payload, headers=headers, allow_redirects=False)

        self.__request_log(response)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200 and status_code != 201:
            print("-----------------------------------------------")
            print(response.request.method)
            print("-----------------------------------------------")
            print(response.request.url)
            print("-----------------------------------------------")
            print("-----------------------------------------------")
            print(response.request.headers)
            print("-----------------------------------------------")
            print(response.request.body)
            print("-----------------------------------------------")

            #print(response.history)
            #print("-----------------------------------------------")
            #print(response.history[0].request.method)

            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)

            raise requests.exceptions.HTTPError(f"{status_code} : {response_json['errors'][0]['message']}")
        else:
            #print("-----------------------------------------------")
            #print(response.request.url)
            #print("-----------------------------------------------")
            #print(response.request.headers)
            #print("-----------------------------------------------")
            #print(response.request.body)
            #print("-----------------------------------------------")
            response_json = response.json()
            #print(response_json)
            #print("-----------------------------------------------")

            if 'error' in response_json and len(response_json["error"]) > 0:
                raise requests.exceptions.HTTPError(f"{status_code} : {response_json['error'][0]}")
            else:
                return response_json

    def __round_size(self, product_symbol, size):
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["amount"]["min"]:
            rounded_size = float(Decimal(str(size)) - Decimal(str(size)) % Decimal(str(self.__products[product_symbol]["limits"]["amount"]["min"])))
            return rounded_size
        else:
            return size

    def __round_price(self, product_symbol, price):
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["price"]["min"]:
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(self.__products[product_symbol]["limits"]["price"]["min"])))
            return rounded_price
        else:
            return price

    def __compose_order_status(self, order):
        order_status = "unknown"
        if not order["executed_price"]:
            order_status = "open"
        else:
            order_status = "filled"
        return order_status

    def __get_timestamp(self):
        return str(int(time.time()*1000))

    def __generate_signature(self, path, data):
        encoded = (str(data["nonce"]) + urlencode(data)).encode()
        message = path.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    def __log_order(self, order_id, symbol, size, side, order_type, status, execution_price):
        try:
            self.strategy.log_trade(order_id, symbol, size, side, order_type, status, execution_price)
        except Exception as e:
            log(f"[{self}][ERROR] Unable to log order : {type(e).__name__} - {e}\n{format_traceback(e.__traceback__)}")

    def __request_log(self, response):
        try:
            file_path = "woorton_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}] [{self.strategy}] [TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}] [{self.strategy}] [FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except:
            pass

    def __str__(self):
        if self.strategy:
            return f"{self.PLATFORM_NAME} {self.strategy}"
        else:
            return f"{self.PLATFORM_NAME}"

#################
### Websocket ###
#################

    def __create_websocket(self):
        self.__ws_log(f"Creating new websocket.", "CREATE")
        ws = None
        with self.__websocket_mutex:
            if len(self.__websockets) == self.__max_websocket:
                self.__ws_log(f"[ERROR] Can't create new websocket, max connection reached ({self.__max_websocket})", "CREATE")
            else:
                ws = websocket.WebSocket()
                self.__websockets.append(ws)
                with self.__websocket_channels_mutex:
                    self.__websocket_channels[ws] = {
                        "callbacks": [],
                        "symbols": [],
                    }

                # Connect websocket
                ws.connect(self.ws_url, header={"Authorization": f"Token {self.api_key}"})

                # Launch reception thread
                if not self.__receive_thread:
                    self.__receive_thread = StoppableThread(name="Thread-Woorton-websocket_callback", target=self.__websocket_callback)
                    self.__receive_thread.start()

                # Tell server to enable heartbeat
                #self.__heartbeat(ws)

                # Setup websocket handler
                with self.__websocket_handlers_mutex:
                    self.__websocket_handlers[ws] = {
                        "is_connected": True,
                        "is_auth": False,
                        "connection_lost": False,
                        "connection_aborted": False,
                        #"reconnect_timer": threading.Timer(self.__heartbeat_timeout, self.__reconnect_timer, [ws])
                        "reconnect_timer": None,
                    }
                    #self.__websocket_handlers[ws]["reconnect_timer"].name = f"Thread-Woorton-reconnect-{hex(id(ws))}"
                    #self.__websocket_handlers[ws]["reconnect_timer"].start()

                self.__websocket_number += 1
                self.__ws_log(f"New websocket created : {ws}.", "CREATE")

        return ws

    def __delete_websocket(self, websocket):
        self.__ws_log(f"Removing websocket '{websocket}'.", "DELETE")

        # Remove websocket handler
        with self.__websocket_handlers_mutex:
            if self.__websocket_handlers[websocket]["reconnect_timer"]:
                self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
            del self.__websocket_handlers[websocket]

        # Close and remove websocket
        with self.__websocket_mutex:
            # Remove websocket
            self.__websockets.remove(websocket)
            self.__websocket_number -= 1

            # Stop reception thread if no websocket anymore
            if self.__websocket_number == 0:
                self.__receive_thread.stop()
                self.__ws_log("Joining reception thread", "DELETE")
                self.__receive_thread.join()
                self.__ws_log("Reception thread joined", "DELETE")
                self.__receive_thread = None

            # Close websocket connection
            websocket.close()

        self.__ws_log(f"Websocket removed '{websocket}'.", "DELETE")

    def __subscribe_dispatcher(self, symbols, callback=None):
        self.__ws_log(f"Subscribing with symbols '{symbols}'", "SUBSCRIBE")

        # Remove already registered symbols
        with self.__websocket_mutex:
            for ws in self.__websockets:
                with self.__websocket_channels_mutex:
                    for symbol in symbols:
                        if symbol in self.__websocket_channels[ws]["symbols"]:
                            self.__ws_log(f"Skip already registered symbol '{symbol}'.", "SUBSCRIBE")
                            symbols.remove(symbol)

        # Exit if no symbol to register
        if len(symbols) == 0:
            return

        with self.__subscribe_mutex:

            # Create a websocket if not existing yet
            if self.__websocket_number == 0:
                ws = self.__create_websocket()

            with self.__websocket_mutex:
                ws = self.__websockets[0]
                # Subscribe channels on websocket
                self.__ws_log(f"Register symbols '{symbols}' on websocket '{ws}'.", "SUBSCRIBE")

                for symbol in symbols:
                    self.__subscribe(ws, symbol)

                with self.__websocket_channels_mutex:
                    self.__websocket_channels[ws]["symbols"].extend(symbols)
                    if callback and callback not in self.__websocket_channels[ws]["callbacks"]:
                        self.__websocket_channels[ws]["callbacks"].append(callback)

    def __unsubscribe_dispatcher(self, symbols, callback=None):
        self.__ws_log(f"Unsubscribing with symbols '{symbols}'", "UNSUBSCRIBE")

        websockets_to_remove = []
        with self.__subscribe_mutex:
            with self.__websocket_channels_mutex:
                ws = self.__websockets[0]

                # Unubscribe channels on websocket
                self.__ws_log(f"Unregister symbols '{symbols}' on websocket '{ws}'.", "UNSUBSCRIBE")

                # Send unsubscribe message only if websocket is connected
                with self.__websocket_handlers_mutex:
                    if self.__websocket_handlers[ws]["is_connected"]:
                        for symbol in symbols:
                            if symbol in self.__websocket_channels[ws]["symbols"]:
                                self.__unsubscribe(ws, symbol)
                                self.__websocket_channels[ws]["symbols"].remove(symbol)

                # Remove callback if no symbol registered anymore
                if (len(self.__websocket_channels[ws]["symbols"]) == 0 and
                    callback in self.__websocket_channels[ws]["callbacks"]):
                        self.__websocket_channels[ws]["callbacks"].remove(callback)

                # Remove all callbacks if none given
                if callback is None:
                    self.__websocket_channels[ws]["callbacks"] = []

                # Delete websocket if no symbol subscribed anymore
                if len(self.__websocket_channels[ws]["symbols"]) == 0:
                    websockets_to_remove.append(ws)

            # Remove empty websockets from subscribed
            for ws in websockets_to_remove:
                self.__delete_websocket(ws)
                del self.__websocket_channels[ws]

        self.__ws_log(f"Unsubscribed from symbols '{symbols}'", "UNSUBSCRIBE")

    def __subscribe(self, websocket, symbol):
        self.__ws_log(f"Subscribing to '{symbol}' on {websocket}", "SUBSCRIBE")

        product = self.__products[symbol]

        #mark_price = self.get_mark_price(symbol)["mark_price"]
        #max_level = 60000 / mark_price if (symbol in ["BTC_USD", "ETH_USD"]) else 15000 / mark_price
        #max_level = 1

        max_level = self.subscribe_levels[symbol] if symbol in self.subscribe_levels else 50

        data = {
            "event": "subscribe",
            "instrument": product["exchange_symbol"],
            "levels": [product["limits"]["amount"]["min"], max_level]
        }

        # Send subscribe message on websocket
        self.__ws_send(websocket, data)

        self.__ws_log(f"Subscribed to '{symbol}' on {websocket}", "SUBSCRIBE")

    def __unsubscribe(self, websocket, symbol):
        self.__ws_log(f"Unsubscribing from '{symbol}' on {websocket}", "UNSUBSCRIBE")

        product = self.__products[symbol]

        data = {
            "event": "unsubscribe",
            "instrument": product["exchange_symbol"],
        }

        # Send subscribe message on websocket
        self.__ws_send(websocket, data)

        self.__ws_log(f"Unsubscribed from '{symbol}' on {websocket}", "UNSUBSCRIBE")

    def __websocket_callback(self):
        while not self.__receive_thread.is_stopped():
            with self.__websocket_handlers_mutex:
                connected_websockets = [ws for ws in self.__websocket_handlers.keys() if self.__websocket_handlers[ws]["is_connected"]]
            websockets,_,_ = select.select(connected_websockets, [], [], 0.1)
            for ws in websockets:

                try:
                    op_code, frame = ws.recv_data_frame()
                except websocket._exceptions.WebSocketConnectionClosedException:
                    log(f"[{self}] [RECEIVE - {ws}] Connection closed by remote host")
                    self.__connection_lost = True
                    self.__on_close(ws)
                    continue
                except Exception as e:
                    log(f"Exception occured in thread __websocket_callback on {ws}: {e}")
                    log(format_traceback(e.__traceback__))
                    continue

                if op_code == op_codes.OPCODE_CLOSE:
                    self.__ws_log(f"Close frame", f"RECEIVE - {ws}")
                    self.__on_close(ws)
                elif op_code == op_codes.OPCODE_PING:
                    self.__ws_log(f"Ping frame : Not supported yet", f"RECEIVE - {ws}")
                elif op_code == op_codes.OPCODE_PONG:
                    self.__ws_log(f"Pong frame : Not supported yet", f"RECEIVE - {ws}")
                elif op_code == op_codes.OPCODE_CONT:
                    self.__ws_log(f"Continuation frame : Not supported yet", f"RECEIVE - {ws}")
                elif op_code == op_codes.OPCODE_BINARY:
                    self.__ws_log(f"Binary frame : Not supported yet", f"RECEIVE - {ws}")
                else:
                    self.__on_message(ws, frame.data.decode("utf-8"))

        # Reset thread object if exiting
        log(f"[{self}] Exit reception thread")
        #self.__receive_thread = None

    def __on_message(self, websocket, message):
        self.__ws_log(message, f"RECEIVE - {websocket}")

        data = json.loads(message)

        if "event" in data:
            event = data["event"]

            if event == "price":
                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["instrument"]][0]

                data_refactored = {
                    "symbol": symbol,
                    "exchange_id": data["instrument"],
                    "exchange_symbol": data["instrument"],
                    "buy": [],
                    "sell": [],
                }

                for order in data["levels"]["buy"]:
                    order_refactored = {
                        "price": float(order["price"]),
                        "size": float(order["quantity"]),
                    }
                    data_refactored["buy"].append(order_refactored)
                for order in data["levels"]["sell"]:
                    order_refactored = {
                        "price": float(order["price"]),
                        "size": float(order["quantity"]),
                    }
                    data_refactored["sell"].append(order_refactored)
            else:
                return

            with self.__websocket_channels_mutex:
                if websocket in self.__websocket_channels:
                    # Call channel callbacks
                    for callback in self.__websocket_channels[websocket]["callbacks"]:
                        try:
                            callback(self, copy.deepcopy(data_refactored))
                        except Exception as e:
                            log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __on_close(self, websocket):

        # Do nothing if websocket deleted by connector
        with self.__websocket_mutex:
            if websocket not in self.__websockets:
                return

        self.__ws_log(f"Websocket {websocket} closed", "ON_CLOSE")

        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = False
            self.__websocket_handlers[websocket]["is_auth"] = False
            self.__websocket_handlers[websocket]["connection_lost"] = True
            if self.__websocket_handlers[websocket]["reconnect_timer"]:
                self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
                self.__websocket_handlers[websocket]["reconnect_timer"] = None

        # Start background reconnection thread
        #threading.Thread(target=self.__reconnection_loop, args=[websocket]).start()

        #try:
        #    self.__reconnect(websocket)
        #except Exception as e:
        #    log(f"[{self}][RECONNECT] Reconnection failed : {e}")
        #    log(format_traceback(e.__traceback__))
        #    self.__delete_websocket(websocket)
        #    with self.__websocket_channels_mutex:
        #        del self.__websocket_channels[websocket]

    def __ws_send(self, websocket, data):
        self.__ws_log(data, "SEND")
        websocket.send(json.dumps(data))

    def __ws_log(self, message, header=""):
        if self.__show_websocket_logs:
            if header:
                message = f"[{header}] {message}"
            log(f"[{self}] {message}")
