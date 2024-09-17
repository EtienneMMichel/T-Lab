from connectors.threading.Threads import StoppableThread, format_traceback
from connectors.crypto.connector.common.connector import CryptoConnector
import connectors.crypto.connector.common.websocket_codes as op_codes
from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.threading.Threads import format_traceback
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
from decimal import Decimal
from enum import IntEnum
import websocket
import threading
import requests
import datetime
import select
import queue
import time
import json
import copy
import re

class DeribitErrorCodes(IntEnum):
    FORBIDDEN = 13021
    UNAUTHORIZED = 13009
    NOT_ENOUGH_FUNDS = 10009
    ORDER_NOT_FOUND = 10004

class Deribit(CryptoConnector):
    PLATFORM_ID = 8
    PLATFORM_NAME = "Deribit"

    SCOPE_REGEX = {
        "account": "^.*(account:(read_write|read)).*$",
        "trade": "^.*(trade:(read_write|read)).*$",
        "wallet": "^.*(wallet:(read_write|read)).*$",
    }

    def __init__(self, api_key, api_secret, testnet=False, strategy=None, passphrase="", base_currency="USD"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.session = requests.Session()

        if self.testing:
            self.base_url = "http://test.deribit.com/api/v2"
            self.ws_url = "wss://test.deribit.com/ws/api/v2"
        else:
            self.base_url = "https://www.deribit.com/api/v2"
            self.ws_url = "wss://www.deribit.com/ws/api/v2"
            #self.ws_url = "ws://localhost:12345"

        # TODO: Set fees
        self.option_fees = 0.0005
        self.future_fees = 0.001

        self.currencies = ['BTC','ETH','USDC','SOL', 'USDT']
        self.__products = {}
        self.__assets = {}

        self.__CHANNELS_LIMITS = {
            "orderbook": 500,
            "mark_price": 500,
            "funding_rate": 500,
            "volume": 500,
        }

        self.__tick_size_steps = {}

        self.__websocket_mutex = threading.Lock()
        self.__websockets = []
        self.__max_websocket = 100
        self.__websocket_number = 0
        self.__websocket_handlers_mutex = threading.Lock()
        self.__websocket_handlers = {}
        self.__websocket_channels_mutex = threading.Lock()
        self.__websocket_channels = {}

        self.__subscribe_mutex = threading.Lock()

        self.__heartbeat_timeout = 35
        self.__receive_thread = None
        self.__connection_lost = False
        self.__connection_aborted = False
        self.__reconnect_timeout = 30
        self.__abortion_datetime = None
        self.__show_websocket_logs = False

        self.__auth_token = None
        self.__auth_refresh_token = None
        self.__auth_timeout = None

        self.__last_mark_price = {}
        self.__last_funding_rate = {}
        self.__last_volume = {}

        self.__orderbooks = {}

######################
### Public methods ###
######################

################
### REST API ###
################

    def is_ip_authorized(self):
        try:
            self.__refresh_token()
        except UnauthorizedError:
            return False
        return True

    def has_read_permission(self):
        response = self.__refresh_token()
        scope = response["scope"]

        account_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["account"], scope)) else None
        trade_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["trade"], scope)) else None

        if not account_scope or not trade_scope:
            permission = False
        elif ((account_scope == "read" and trade_scope == "read") or
            (account_scope == "read" and trade_scope == "read_write") or
            (account_scope == "read_write" and trade_scope == "read") or
            (account_scope == "read_write" and trade_scope == "read_write")):
            permission = True
        else:
            permission = False

        return permission

    def has_write_permission(self):
        response = self.__refresh_token()
        scope = response["scope"]

        account_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["account"], scope)) else None
        trade_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["trade"], scope)) else None

        if not account_scope or not trade_scope:
            permission = False
        elif ((account_scope == "read" and trade_scope == "read_write") or
            (account_scope == "read_write" and trade_scope == "read_write")):
            permission = True
        else:
            permission = False

        return permission

    def has_withdraw_permission(self):
        response = self.__refresh_token()
        scope = response["scope"]

        account_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["account"], scope)) else None
        wallet_scope = result.group(2) if (result := re.search(self.SCOPE_REGEX["wallet"], scope)) else None

        if not account_scope or not wallet_scope:
            permission = False
        elif ((account_scope == "read" and wallet_scope == "read_write") or
            (account_scope == "read_write" and wallet_scope == "read_write")):
            permission = True
        else:
            permission = False

        return permission

    def has_future_authorized(self):
        return True

    def get_fees(self):
        return {
            "taker": 0,
            "maker": 0,
        }

    def get_symbol_usd_price(self, symbol, date=None):
        return self.__get_symbol_usd_price(symbol, date)

    def get_balance(self, as_base_currency=False, as_eur=False):
        if not self.__products:
            self.get_products()

        if as_base_currency:
            rate = CbForex(self.database).get_rate("USD", self.base_currency)
        else:
            rate = 1

        data = {}
        for currency in self.currencies:
            response = self.__api_get_account_summary(currency)
            if response["equity"] == 0:
                continue
            symbol = response["currency"]

            if as_base_currency:
                usd_price = self.__get_symbol_usd_price(symbol)
                conversion_factor = usd_price*rate
            else:
                conversion_factor = 1

            data[symbol] = {
                "symbol" : symbol,
                "balance" : float(response["equity"])*conversion_factor,
                "available_balance" : float(response["available_funds"])*conversion_factor,
                "currency" : self.base_currency if as_base_currency else symbol,
            }

        return data

    def get_profit(self, buy_crypto_history=None):
        data = {}
        total_deposit = 0
        total_withdraw = 0
        total_profit = 0
        total_balance = 0

        balances = self.get_balance(as_base_currency=True)
        for balance in balances.values():
            total_balance += balance["balance"]

        feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
        feeder_cashflows_transaction_ids = feeder_cashflows["last_tx_id"].values
        internal_deposit_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_deposit'")
        internal_deposit_transaction_ids = internal_deposit_cashflows["last_tx_id"].values
        internal_fees_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'fees'")
        internal_fees_transaction_id = internal_fees_cashflows["last_tx_id"].values
        internal_withdraw_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_withdraw'")
        internal_withdraw_transaction_ids = internal_withdraw_cashflows["last_tx_id"].values

        #transactions = self.get_deposit_withdraw_history()
        buy_crypto_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {self.strategy.id}")
        for index, transaction in buy_crypto_history.iterrows():

            #print("##########")
            #print(transaction)
            #print("##########")

            if transaction['order_number'] in feeder_cashflows_transaction_ids and not self.strategy.is_feeder_investable():
                #print(f"### Skip transaction by feeder id")
                continue
            if transaction['order_number'] in internal_deposit_transaction_ids:
                #print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['order_number'] in internal_fees_transaction_id:
                #print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['order_number'] in internal_withdraw_transaction_ids:
                #print(f"### Skip transaction by internal withdraw id")
                continue
            if transaction["transaction_type"] == "deposit":
                #print("### ADD deposit")
                total_deposit += transaction["base_currency_amount"]
            else:
                #print("### ADD withdraw")
                total_withdraw += transaction["base_currency_amount"]


        total_profit = total_balance - total_deposit + total_withdraw

        data = {
            "total_balance": total_balance,
            "total_deposit": total_deposit,
            "total_withdraw": total_withdraw,
            "total_profit": total_profit,
        }

        return data

    def get_assets(self, reload=False):
        if not self.__assets or reload:
            self.__assets = {}
            products = self.get_products(reload=reload)
            for product in products.values():
                if product["base_asset_symbol"] not in self.__assets:
                    self.__assets[product["base_asset_symbol"]] = {
                        "id": product["base_asset_id"],
                        "symbol": product["base_asset_symbol"],
                    }
                if product["quote_asset_symbol"] not in self.__assets:
                    self.__assets[product["quote_asset_symbol"]] = {
                        "id": product["quote_asset_id"],
                        "symbol": product["quote_asset_symbol"],
                    }
        return self.__assets

    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}
            for currency in self.currencies:
                response = self.__api_get_products(currency)
                product_refactored = {}
                for product in response:

                    generated_symbol = self.__compose_product_symbol(product)

                    product_refactored = {
                        "symbol": generated_symbol,
                        "exchange_id": product["instrument_name"],
                        "exchange_symbol": product["instrument_name"],
                        "contract_type": None,
                        # Contract size is always 1 for spot
                        "contract_size": None,
                        "strike_price": None,
                        "settlement_date": None,
                        "settlement_time": None,
                        "duration": None,
                        "precision": {
                            "amount": None,
                            "price": None,
                            "notional": None,
                        },
                        "limits": {
                            "amount": {
                                "min": product["min_trade_amount"],
                                "max": None},
                            "price": {
                                "min": product["tick_size"],
                                "max": None},
                            "notional": {
                                "min": None,
                                "max": None},
                        },
                    }

                    if not (contract_type := self.__compose_contract_type(product)):
                        # Do not handle this product types
                        continue
                    product_refactored["contract_type"] = contract_type
                    product_refactored["contract_size"] = product["contract_size"]

                    product_refactored["strike_price"] = float(product["strike"]) if "strike" in product else None

                    settlement_datetime = datetime.datetime.fromtimestamp(product["expiration_timestamp"]/1000,
                                            tz=datetime.timezone.utc)
                    settlement_time = settlement_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
                    settlement_date = settlement_time[:10] if settlement_time else None
                    duration = (settlement_datetime.date() - datetime.date.today()).days
                    product_refactored["settlement_date"] = settlement_date
                    product_refactored["settlement_time"] = settlement_time
                    product_refactored["duration"] = duration
                    product_refactored["base_asset_id"] = product["base_currency"]
                    product_refactored["base_asset_symbol"] = product["base_currency"]
                    product_refactored["quote_asset_id"] = product["quote_currency"]
                    product_refactored["quote_asset_symbol"] = product["quote_currency"]

                    # Compose tick_sizes
                    tick_sizes = []
                    for tick_size in product["tick_size_steps"]:
                        tick_sizes.append(tick_size)
                    self.__tick_size_steps[generated_symbol] = sorted(tick_sizes, key=lambda d: d['above_price'])

                    # Temporary overwrite contract_size for BTC options
                    if ((contract_type == "call_option" or contract_type == "put_option") and
                        product["base_currency"] == "BTC"):
                        product_refactored["contract_size"] = 0.1

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
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = None
        if pricing_client:
            if response := pricing_client.get_orderbook(self.PLATFORM_ID, symbol):
                data = {
                    "symbol": symbol,
                    "exchange_id": exchange_id,
                    "exchange_symbol": exchange_symbol,
                    "buy": response["buy"] if "buy" in response else [],
                    "sell": response["sell"] if "sell" in response else [],
                }
        if response == None:
            response = self.__api_get_orderbook_by_symbol(exchange_symbol)

            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "buy": [],
                "sell": [],
            }

            index_price = response["index_price"]

            for order in response["bids"]:
                order_refactored = {}
                if (self.__products[symbol]["contract_type"] == "call_option" or
                    self.__products[symbol]["contract_type"] == "put_option"):
                    order_refactored["price"] = order[0]*index_price
                else:
                    order_refactored["price"] = order[0]
                order_refactored["size"] = order[1]
                data["buy"].append(order_refactored)
            for order in response["asks"]:
                order_refactored = {}
                if (self.__products[symbol]["contract_type"] == "call_option" or
                    self.__products[symbol]["contract_type"] == "put_option"):
                    order_refactored["price"] = order[0]*index_price
                else:
                    order_refactored["price"] = order[0]
                order_refactored["size"] = order[1]
                data["sell"].append(order_refactored)

        return data

    def get_spot_price(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
            base_asset_symbol = self.__products[symbol]['base_asset_symbol']
            quote_asset_symbol = self.__products[symbol]['quote_asset_symbol']

        index_symbol = f"{base_asset_symbol}_USDC".lower()

        response = self.__api_get_index_price(index_symbol)
        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "spot_price": response["index_price"],
        }
        return data

    def get_mark_price(self, symbol, pricing_client=None):
        if not self.__products:
            self.get_products()
        if not self.__assets:
            self.get_assets()

        if symbol not in self.__products and symbol not in self.__assets:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif symbol in self.__products:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

            response = None
            if pricing_client:
                if response := pricing_client.get_mark_price(self.PLATFORM_ID, symbol):
                    data = {
                        "symbol": symbol,
                        "exchange_id": exchange_id,
                        "exchange_symbol": exchange_symbol,
                        "mark_price": float(response["mark_price"]),
                    }
            if response == None:
                response = self.__api_get_ticker(exchange_symbol)
                data = {
                    "symbol": symbol,
                    "exchange_id": exchange_id,
                    "exchange_symbol": exchange_symbol,
                    "mark_price": response["mark_price"],
                }
        else:
            price = self.__get_symbol_usd_price(symbol)
            data = {
                "symbol": symbol,
                "exchange_id": self.__assets[symbol]["id"],
                "exchange_symbol": self.__assets[symbol]["symbol"],
                "mark_price": price,
            }

        return data

    def get_greeks(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_ticker(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "delta": float(response["greeks"]['delta']),
            "gamma": float(response["greeks"]['gamma']),
            "theta": float(response["greeks"]['theta']),
            "rho": float(response["greeks"]['rho']),
            "vega": float(response["greeks"]['vega']),
        }
        return data

    def get_positions(self):
        if not self.__products:
            self.get_products()

        rate = CbForex(self.database).get_rate("USD", self.base_currency)

        data = {}
        for currency in self.currencies:
            response = self.__api_get_positions(currency)
            for position in response:
                if position["size"] == 0:
                    continue

                # Find corresponding product
                product = [product for symbol, product in self.__products.items() if product["exchange_symbol"] == position["instrument_name"]][0]

                symbol = product["symbol"]
                usd_price = self.__get_symbol_usd_price(symbol)

                if product["contract_type"] == "perpetual_future":
                    base_currency_amount = position["size"]*rate
                else:
                    base_currency_amount = position["size"]*usd_price*rate

                data[symbol] = {
                    "symbol": symbol,
                    "exchange_id": position["instrument_name"],
                    "exchange_symbol": position["instrument_name"],
                    "size": position["size"],
                    "entry_price": position["average_price"],
                    "maintenance_margin": 0,
                    "contract_type": product["contract_type"],
                    "base_asset_id": product["base_asset_id"],
                    "base_asset_symbol": product["base_asset_symbol"],
                    "quote_asset_id": product["quote_asset_id"],
                    "quote_asset_symbol": product["quote_asset_symbol"],
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                }

            response = self.__api_get_account_summary(currency)

            if response["equity"] == 0:
                continue
            symbol = response["currency"]
            usd_price = self.__get_symbol_usd_price(symbol)
            data[symbol] = {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "size": response["balance"],
                "entry_price": 0,
                "maintenance_margin": response["maintenance_margin"],
                "contract_type": 'spot',
                "base_asset_id": symbol,
                "base_asset_symbol": symbol,
                "quote_asset_id": symbol,
                "quote_asset_symbol": symbol,
                "base_currency_amount": response["balance"]*usd_price*rate,
                "base_currency_symbol": self.base_currency,
            }

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

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "size": response["size"],
            "entry_price": response["average_price"],
            "total_pl": response["total_profit_loss"],
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
            "auto_topup": None,
            "margin": response["maintenance_margin"],
            "entry_price": response["average_price"],
        }
        return data

    #TODO: Find which endpoint to use to set margin
    def set_position_margin(self, symbol):
        #response = self.__api_set_position(product_symbol)
        pass

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        """
        # Size is expressed as local currency but deribit trades in USD for futures
        if self.__products[symbol]["contract_type"] == "perpetual_future":
            mark_price = self.get_mark_price(symbol)["mark_price"]
            print(f"Mark_price is : {mark_price}")
            size = size*mark_price
        """

        size = self.__round_size(symbol, size)

        response = self.__api_place_order(exchange_symbol, size, side, order_type="market_order", time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["order"]["order_id"]),
            "type": "market",
            "status": self.__compose_order_status(response["order"]),
            "symbol": symbol,
            "exchange_id": response["order"]["instrument_name"],
            "exchange_symbol": response["order"]["instrument_name"],
            "side": response["order"]["direction"],
            "size": response["order"]["amount"],
            "filled_size": response["order"]["filled_amount"],
            "unfilled_size": response["order"]["amount"] - response["order"]["filled_amount"],
            "average_price": response["order"]["average_price"],
            "limit_price": None,
            "stop_price": None,
            "time_in_force": self.__compose_time_in_force(response["order"]),
            "commission": 0,
            "created_at": datetime.datetime.fromtimestamp(response["order"]["creation_timestamp"]/1000),
            "updated_at": datetime.datetime.fromtimestamp(response["order"]["last_update_timestamp"]/1000),
        }

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "market_order", data["status"], response["order"]["average_price"])

        # Average_price is not as USD for options
        if (self.__products[symbol]["contract_type"] == "call_option" or
            self.__products[symbol]["contract_type"] == "put_option"):
            data["average_price"] = self.__compute_option_average_price(response)
        """
        # Adapt size for futures
        if self.__products[symbol]["contract_type"] == "perpetual_future":
            data["size"] = data["size"] / data["average_price"]
            data["filled_size"] = data["filled_size"] / data["average_price"]
            data["unfilled_size"] = data["unfilled_size"] / data["average_price"]
        """

        return data

    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0, reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        limit_price = self.__round_price(symbol, limit_price)

        response = self.__api_place_order(exchange_symbol, size, side, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        started_at = time.time()

        data = {
            "id": str(response["order"]["order_id"]),
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = str(response["order"]["order_id"])
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
                order = self.__api_get_order_state(order_id)

                data["status"] = self.__compose_order_status(order)
                data["symbol"] = symbol
                data["exchange_id"] = order["instrument_name"]
                data["exchange_symbol"] = order["instrument_name"]
                data["side"] = order["direction"]
                data["size"] = order["amount"]
                data["filled_size"] = order["filled_amount"]
                data["unfilled_size"] = order["amount"] - order["filled_amount"]
                data["average_price"] = order["average_price"] if "average_price" in order else order["price"]
                data["limit_price"] = order["price"]
                data["stop_price"] = None
                data["time_in_force"] = self.__compose_time_in_force(order)
                data["commission"] = 0
                data["created_at"] = datetime.datetime.fromtimestamp(order["creation_timestamp"]/1000),
                data["updated_at"] = datetime.datetime.fromtimestamp(order["last_update_timestamp"]/1000),


                # Average_price is not as USD for options
                if (self.__products[symbol]["contract_type"] == "call_option" or
                    self.__products[symbol]["contract_type"] == "put_option"):
                    data["average_price"] = self.__compute_option_average_price(response)

        # Doesn't wait for order to be placed
        else:
            data["status"] = self.__compose_order_status(response["order"])
            data["symbol"] = symbol
            data["exchange_id"] = response["order"]["instrument_name"]
            data["exchange_symbol"] = response["order"]["instrument_name"]
            data["side"] = response["order"]["direction"]
            data["size"] = response["order"]["amount"]
            data["filled_size"] = response["order"]["filled_amount"]
            data["unfilled_size"] = response["order"]["amount"] - response["order"]["filled_amount"]
            data["average_price"] = response["order"]["average_price"]
            data["limit_price"] = response["order"]["price"]
            data["stop_price"] = None
            data["time_in_force"] = self.__compose_time_in_force(response["order"])
            data["commission"] = 0
            data["created_at"] = datetime.datetime.fromtimestamp(response["order"]["creation_timestamp"]/1000),
            data["updated_at"] = datetime.datetime.fromtimestamp(response["order"]["last_update_timestamp"]/1000),

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], response["order"]["average_price"])

        return data

    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        stop_price = self.__round_price(symbol, stop_price)

        response = self.__api_place_order(exchange_symbol, size, side, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["order"]["order_id"]),
            "type": "stop_market",
            "status": self.__compose_order_status(response["order"]),
            "symbol": symbol,
            "exchange_id": response["order"]["instrument_name"],
            "exchange_symbol": response["order"]["instrument_name"],
            "side": response["order"]["direction"],
            "size": response["order"]["amount"],
            "filled_size": 0,
            "unfilled_size": response["order"]["amount"],
            "average_price": 0,
            "limit_price": None,
            "stop_price": response["order"]["trigger_price"],
            "time_in_force": self.__compose_time_in_force(response["order"]),
            "commission": 0,
            "created_at": datetime.datetime.fromtimestamp(response["order"]["creation_timestamp"]/1000),
            "updated_at": datetime.datetime.fromtimestamp(response["order"]["last_update_timestamp"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_market_order", data["status"], response["order"]["trigger_price"])

        return data

    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        stop_price = self.__round_price(symbol, stop_price)
        limit_price = self.__round_price(symbol, limit_price)

        response = self.__api_place_order(exchange_symbol, size, side, order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["order"]["order_id"]),
            "type": "stop_limit",
            "status": self.__compose_order_status(response["order"]),
            "product_id": response["order"]["instrument_name"],
            "symbol": response["order"]["instrument_name"],
            "side": response["order"]["direction"],
            "size": response["order"]["amount"],
            "filled_size": 0,
            "unfilled_size": response["order"]["amount"],
            "average_price": 0,
            "limit_price": response["order"]["price"],
            "stop_price": response["order"]["trigger_price"],
            "time_in_force": self.__compose_time_in_force(response["order"]),
            "commission": 0,
            "created_at": datetime.datetime.fromtimestamp(response["order"]["creation_timestamp"]/1000),
            "updated_at": datetime.datetime.fromtimestamp(response["order"]["last_update_timestamp"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_limit_order", data["status"], response["order"]["price"])

        return data

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.00075):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        unfilled_size = size

        # Overwrite limit price coef depending on product's base asset
        base_asset_symbol = self.__products[symbol]["base_asset_symbol"]
        if base_asset_symbol == "BTC":
            limit_price_coef = 0.5
        elif base_asset_symbol == "ETH":
            limit_price_coef = 0.05

        order_queue = queue.Queue()
        limit_order_placed = False
        stop_order_placed = False
        partial_order = False

        # Define orders callback as inner function
        def orders_callback(connector, data):
            order_queue.put(data)

        self.subscribe_orders(symbol, orders_callback)

        # Get orderbook for symbol
        orderbook = self.get_orderbook(symbol)

        #log(f"[{self}] Orderbook : {orderbook}")

        # Compute prices from orderbook with coefficients
        if side == "sell":
            log(f"[{self}] Orderbook best price : {orderbook['buy'][0]['price']}")
            limit_price = orderbook["buy"][0]["price"] + limit_price_coef
            stop_price = orderbook["buy"][0]["price"]*(1-stop_price_coef)
        else:
            log(f"[{self}] Orderbook best price : {orderbook['sell'][0]['price']}")
            limit_price = orderbook["sell"][0]["price"] - limit_price_coef
            stop_price = orderbook["sell"][0]["price"]*(1+stop_price_coef)

        # Place limit order
        try:
            log(f"[{self}] Place {side} limit order of {size} on {symbol} at {limit_price}")
            response = self.place_limit_order(symbol, side=side, size=size, limit_price=limit_price)
            log(f"[{self}] {response}")
            limit_order_id = response["id"]
        except Exception as e:
            log(f"[{self}][ERROR] Can't place limit order : {type(e).__name__} - {e}. Place market order instead")
            self.unsubscribe_orders(symbol)
            response = self.place_market_order(symbol, side=side, size=size)
            log(f"{response}")
            return response

        # Get orderbook for symbol
        #orderbook = self.get_orderbook(symbol)
        #log(f"[{self}] Orderbook : {orderbook}")

        # Place stop market order
        try:
            log(f"[{self}] Place {side} stop market order of {size} on {symbol} at {stop_price}")
            response = self.place_stop_market_order(symbol, side=side, size=size, stop_price=stop_price)
            log(f"[{self}] {response}")
            stop_market_order_id = response["id"]
        except ImmediateExecutionStopOrderError:
            log(f"[{self}][ERROR] Immediate execution of stop order. Cancelling limit order and placing market order instead.")
            self.cancel_order(symbol, limit_order_id)
            self.unsubscribe_orders(symbol)
            response = self.place_market_order(symbol, side=side, size=size)
            log(f"{response}")
            return response

        time.sleep(1)

        # Check for immediate order filling
        active_orders = self.get_active_orders(symbol)
        log(f"[{self}] #########################################")
        log(f"[{self}] Active orders : {active_orders}")
        log(f"[{self}] #########################################")
        if not any(order["id"] == limit_order_id for order in active_orders):
            log(f"[{self}] Limit order immediatelly placed.")
            limit_order_placed = True

        if not any(order["id"] == stop_market_order_id for order in active_orders):
            log(f"[{self}] Stop order immediatelly placed.")
            stop_order_placed = True

        # Start loop for timeout seconds if no order placed yet
        if not limit_order_placed and not stop_order_placed:
            log(f"[{self}] Wait {timeout} seconds for orders to be placed")
            started_at = datetime.datetime.now()
            while started_at + datetime.timedelta(seconds=timeout) >= datetime.datetime.now():

                # Wait for order update
                if not order_queue.empty():
                    order = order_queue.get()

                    log(f"[{self}] Got an order event : {order}")

                    # Limit order triggered
                    if order["id"] == limit_order_id and float(order["filled_size"]) == size:
                        limit_order_placed = True

                        log(f"[{self}] Cancel stop market order '{stop_market_order_id}'")
                        # Cancel stop market order
                        try:
                            self.cancel_order(symbol, stop_market_order_id)
                            log(f"[{self}] Stop market order '{stop_market_order_id}' cancelled")
                        except Exception as e:
                            log(f"[{self}][ERROR] Failed to cancel stop market order '{stop_market_order_id}' : {type(e).__name__} - {e}")
                        break

                    # Stop market order triggered
                    elif order["id"] == stop_market_order_id and float(order["filled_size"]) == size:
                        stop_order_placed = True

                        log(f"[{self}] Cancel limit order '{limit_order_id}'")
                        # Cancel limit order
                        try:
                            self.cancel_order(symbol, limit_order_id)
                            log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                        except Exception as e:
                            log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")
                        break

                    if order["id"] == limit_order_id:
                        unfilled_size = float(order["unfilled_size"])
                        #TODO: edit order size of stop market order

        # An order has been immediatelly placed
        else:
            if limit_order_placed:
                order = self.get_order_state(symbol, limit_order_id)
                try:
                    log(f"[{self}] Cancel stop market order '{stop_market_order_id}'")
                    self.cancel_order(symbol, stop_market_order_id)
                    log(f"[{self}] Stop market order '{stop_market_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel stop market order '{stop_market_order_id}' : {type(e).__name__} - {e}")
            elif stop_order_placed:
                order = self.get_order_state(symbol, stop_market_order_id)
                try:
                    log(f"[{self}] Cancel limit order '{limit_order_id}'")
                    self.cancel_order(symbol, limit_order_id)
                    log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")

        # Unsubscribe orders
        self.unsubscribe_orders(symbol, orders_callback)

        # Timeout
        if not limit_order_placed and not stop_order_placed:

            limit_order_active = False
            stop_market_order_active = False
            active_orders = self.get_active_orders(symbol)
            for order in active_orders:
                if limit_order_id == order["id"]:
                    limit_order_active = True
                if stop_market_order_id == order["id"]:
                    stop_market_order_active = True

            if limit_order_active:
                try:
                    log(f"[{self}] Cancel limit order '{limit_order_id}'")
                    self.cancel_order(symbol, limit_order_id)
                    log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")
            if stop_market_order_active:
                try:
                    log(f"[{self}] Cancel stop market order '{stop_market_order_id}'")
                    self.cancel_order(symbol, stop_market_order_id)
                    log(f"[{self}] Stop market order '{stop_market_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel stop market order '{stop_market_order_id}' : {type(e).__name__} - {e}")

            if limit_order_active and stop_market_order_active:
                log(f"[{self}] No order has been placed")
                log(f"[{self}] Place {side} market order of {unfilled_size} on {symbol}")
                order = self.place_market_order(symbol, side=side, size=unfilled_size)
                log(f"[{self}] {order}")
            elif not limit_order_active:
                log(f"[{self}] Limit order placed")
                order = self.get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order['average_price'])
            elif not stop_market_order_active:
                log(f"[{self}] Stop market order placed")
                order = self.get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order['average_price'])
        else:
            log(f"[{self}] An order has been executed : {order}")

            # Complete order if partially filled
            if order["filled_size"] < size and float(size-order['filled_size']) > 10:
                log(f"[{self}] Complete partially filled order with {round(size-order['filled_size'], 0)} {symbol}")
                response = self.place_market_order(symbol, side=side, size=round(size-order['filled_size'], 0))
                log(f"{response}")
                filled_size = order["filled_size"]+response["filled_size"]
                average_price = (order["average_price"]*order["filled_size"] + response["average_price"]*response["filled_size"]) / filled_size
            else:
                filled_size = order["filled_size"]
                average_price = order["average_price"]

            order["filled_size"] = filled_size
            order["unfilled_size"] = 0
            order["average_price"] = average_price

            if order["status"] == "filled":
                self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], average_price)

        # Get orderbook for symbol
        #orderbook = self.get_orderbook(symbol)
        #log(f"[{self}] Orderbook : {orderbook}")

        return order

    def get_active_orders(self, symbol=None):
        if not self.__products:
            self.get_products()

        data = []

        if symbol:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_id = self.__products[symbol]["exchange_id"]
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            response = self.__api_get_active_orders_by_instrument(exchange_symbol)
        else:
            response = []
            for currency in self.currencies:
                response.extend(self.__api_get_active_orders_by_currency(currency))

        for order in response:
            filled_amount = order["filled_amount"] if "filled_amount" in order else 0
            active_order = {
                "id": str(order["order_id"]),
                "type": order["order_type"],
                "status": self.__compose_order_status(order),
                "symbol": symbol,
                "exchange_id": order["instrument_name"],
                "exchange_symbol": order["instrument_name"],
                "side": order["direction"],
                "size": order["amount"],
                "filled_size": filled_amount,
                "unfilled_size": order["amount"] - filled_amount,
                "average_price": order["average_price"] if "average_price" in order else None,
                "limit_price": order["price"] if "price" in order else None,
                "stop_price": order["trigger_price"] if "trigger_price" in order else None,
                "time_in_force": self.__compose_time_in_force(order),
                "created_at": datetime.datetime.fromtimestamp(order["creation_timestamp"]/1000),
                "updated_at": datetime.datetime.fromtimestamp(order["last_update_timestamp"]/1000),
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

        response = self.__api_get_order_state(order_id)
        filled_amount = response["filled_amount"] if "filled_amount" in response else 0
        data = {
            "id": str(response["order_id"]),
            "type": response["order_type"],
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["instrument_name"],
            "exchange_symbol": response["instrument_name"],
            "side": response["direction"],
            "size": response["amount"],
            "filled_size": filled_amount,
            "unfilled_size": response["amount"] - filled_amount,
            "average_price": response["average_price"] if "average_price" in response else None,
            "limit_price": response["price"] if "price" in response else None,
            "stop_price": response["trigger_price"] if "trigger_price" in response else None,
            "time_in_force": self.__compose_time_in_force(response),
            "created_at": datetime.datetime.fromtimestamp(response["creation_timestamp"]/1000),
            "updated_at": datetime.datetime.fromtimestamp(response["last_update_timestamp"]/1000),
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

        filled_amount = response["filled_amount"] if "filled_amount" in response else 0

        data = {
            "id": str(response["order_id"]),
            "type": response["order_type"],
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["instrument_name"],
            "exchange_symbol": response["instrument_name"],
            "side": response["direction"],
            "size": response["amount"],
            "filled_size": filled_amount,
            "unfilled_size": response["amount"] - filled_amount,
            "average_price": response["average_price"] if "average_price" in response else None,
            "limit_price": response["price"] if "price" in response else None,
            "stop_price": response["trigger_price"] if "trigger_price" in response else None,
            "time_in_force": self.__compose_time_in_force(response),
            "created_at": datetime.datetime.fromtimestamp(response["creation_timestamp"]/1000),
            "updated_at": datetime.datetime.fromtimestamp(response["last_update_timestamp"]/1000),
        }

        return data

    def get_order_history(self, symbol=None):
        if not self.__products:
            self.get_products()

        data = []
        for currency in self.currencies:
            response =  self.__api_get_trade_history(currency)

            for order in response:

                uniformized_symbol = next((product["symbol"] for product in self.__products.values() if product["exchange_symbol"] == order["instrument_name"]), None)

                if symbol != None and uniformized_symbol != symbol:
                    continue

                order_refactored = {
                    "created_at": datetime.datetime.fromtimestamp(order["timestamp"]/1000),
                    "updated_at": datetime.datetime.fromtimestamp(order["timestamp"]/1000),
                    "id": str(order["order_id"]),
                    "type": order["order_type"],
                    "status": order["state"],
                    "symbol": uniformized_symbol,
                    "exchange_id": order["instrument_name"],
                    "exchange_symbol": order["instrument_name"],
                    "side": order["direction"],
                    "size": order["amount"],
                    "order_type": order["order_type"],
                    "size": order['amount'],
                    "filled_size": order["amount"],
                    "unfilled_size": 0,
                    "average_price": order["price"],
                    "limit_price": order["price"],
                    "stop_price": order["price"],
                    "fees": order["fee"],
                    "fees_currency": order["fee_currency"],
                    "time_in_force": "gtc",
                    "source": "???",
                    "is_liquidation": True if order['order_type'] =='liquidation' else False,
                    "cancellation_reason": None,
                }

                data.append(order_refactored)

        data.sort(key=lambda order:order["created_at"], reverse=True)
        return data

    def get_leverage(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = {}
        response = self.__api_get_position(exchange_symbol)

        data = {
            "symbol": symbol,
            "leverage": response["leverage"],
        }

        return data

    def set_leverage(self, symbol, leverage):
        # Can't set leverage on deribit as it's automatically handled with available margin
        return self.get_leverage(symbol)

    def create_wallet_address(self, symbol):
        return self.__api_create_deposit_address(symbol)

    def get_wallet_address(self, symbol):
        return self.__api_get_deposit_address(symbol)

    def send_rfq(self, symbol, side):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
        return self.__api_send_rfq(exchange_symbol, side)

    def withdraw(self, symbol, amount, address, network_name=None):
        return self.__api_withdraw(symbol, amount, address)

    def get_trade_history(self, symbol, start_date, end_date):
        data = []
        for currency in self.currencies:
            trades =  self.__api_get_trade_history(currency)
            for trade in trades:
                if trade["instrument_name"] != symbol:
                    continue
                date = datetime.datetime.fromtimestamp(trade['time']/1000)
                if date < start_date or date > end_date:
                    continue
                data.append({
                    "id": trade["order_id"],
                    "symbol": symbol,
                    "exchange_id": trade["symbol"],
                    "exchange_symbol": trade["symbol"],
                    "side" : trade["direction"],
                    "size": trade["amount"],
                    "price": trade["price"],
                    "datetime": datetime.datetime.fromtimestamp(trade['time']/1000)
                })
        return data

    def get_transaction_history(self):
        start_date = datetime.date(datetime.date.today().year, 1, 1)
        end_date = datetime.date.today()

        min_time = datetime.datetime.min.time()
        max_time = datetime.datetime.max.time()
        start_date = datetime.datetime.combine(start_date, min_time)
        end_date = datetime.datetime.combine(end_date, max_time)

        start_timestamp = int(datetime.datetime.timestamp(start_date))*1000
        stop_timestamp = int(datetime.datetime.timestamp(end_date))*1000

        data = []
        for currency in self.currencies:
            transactions = self.__api_get_transaction_log(currency, start_timestamp, stop_timestamp)
            if transactions["logs"]:
                for transaction in transactions["logs"]:
                    data.append({
                        "transaction_id": transaction["id"],
                        "transaction_type": transaction["type"],
                        "symbol": transaction["currency"],
                        "amount": transaction["change"],
                        "date": datetime.datetime.fromtimestamp(transaction["timestamp"]/1000),
                    })
        return data

    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        data = []

        if not start_date:
            start_date = datetime.date(2022, 1, 1)
        if not end_date:
            end_date = datetime.date.today()

        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        for currency in self.currencies:

            transfers = self.__api_get_transfers(currency=currency)

            for transfer in transfers["data"]:
                date = datetime.datetime.fromtimestamp(transfer['created_timestamp']/1000)
                if date < start_date or date > end_date:
                    continue

                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(transfer["currency"], date)
                base_currency_amount = transfer["amount"]*rate*usd_amount

                data.append({
                    "date": date,
                    "type": "withdraw" if transfer["direction"] == "payment" else "deposit",
                    "source_symbol": transfer["currency"],
                    "source_amount": transfer["amount"],
                    "destination_symbol": transfer["currency"],
                    "destination_amount": transfer["amount"],
                    "fees": 0,
                    "operation_id": str(transfer["id"]),
                    "wallet_address": transfer["other_side"],
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": None,
                })

            withdrawals = self.__api_get_withdrawals(currency=currency)
            for withdrawal in withdrawals["data"]:
                date = datetime.datetime.fromtimestamp(withdrawal['created_timestamp']/1000)
                if date < start_date or date > end_date:
                    continue
                transaction_id = withdrawal["transaction_id"]
                # Filter-out fees and feeder cashflows
                if self.strategy and self.database:
                    transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                    if not transaction_cashflow.empty:
                        cashflow = transaction_cashflow.iloc[0]
                        if cashflow.type == 'fees' or cashflow.type == 'feeder_cashflow':
                            continue

                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(withdrawal["currency"], date)
                base_currency_amount = withdrawal["amount"]*rate*usd_amount

                data.append({
                    "date": date,
                    "type": "withdraw",
                    "source_symbol": withdrawal["currency"],
                    "source_amount": withdrawal["amount"],
                    "destination_symbol": withdrawal["currency"],
                    "destination_amount": withdrawal["amount"],
                    "fees": withdrawal["fee"] if "fee" in withdrawal else 0,
                    "operation_id": transaction_id,
                    "wallet_address": withdrawal["address"],
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": transaction_id,
                })

            deposits = self.__api_get_deposits(currency=currency)
            for deposit in deposits["data"]:
                date = datetime.datetime.fromtimestamp(deposit['received_timestamp']/1000)
                if date < start_date or date > end_date:
                    continue
                transaction_id = deposit["transaction_id"]
                # Filter-out fees and feeder cashflows
                if self.strategy and self.database:
                    transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                    if not transaction_cashflow.empty:
                        cashflow = transaction_cashflow.iloc[0]
                        if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                            continue

                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(deposit["currency"], date)
                base_currency_amount = deposit["amount"]*rate*usd_amount

                data.append({
                    "date": date,
                    "type": "deposit",
                    "source_symbol": deposit["currency"],
                    "source_amount": deposit["amount"],
                    "destination_symbol": deposit["currency"],
                    "destination_amount": deposit["amount"],
                    "fees": deposit["fee"] if "fee" in deposit else 0,
                    "operation_id": transaction_id,
                    "wallet_address": deposit["address"],
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": transaction_id,
                })

        if len(data) > 0:
            data.sort(key=lambda operation:operation["date"])

        return data

    def get_candle(self, symbol, start_date, end_date):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if start_date >= end_date:
            start_date, end_date = end_date, start_date

        timespan = end_date - start_date

        if timespan <= datetime.timedelta(hours=1):
            resolution = "1"
        elif timespan <= datetime.timedelta(days=1):
            resolution = "60"
        elif timespan <= datetime.timedelta(weeks=1):
            resolution = "1D"
        else:
            resolution = "1D"

        start_timestamp = round(datetime.datetime.timestamp(start_date))*1000
        stop_timestamp = round(datetime.datetime.timestamp(end_date))*1000

        response = self.__api_get_candle_history(exchange_symbol, start_timestamp, stop_timestamp, resolution)

        if response["status"] == "no_data":
            data = None
        else:
            candle_number = len(response["ticks"])
            if candle_number == 0:
                open = 0
                close = 0
                low = 0
                high = 0
                volume = 0
            else:
                open = response["open"][0]
                close = response["close"][candle_number-1]
                low = response["low"][0]
                high = response["high"][0]
                volume = 0

                for index in range(0, candle_number):
                    low = response["low"][index] if response["low"][index] < low else low
                    high = response["high"][index] if response["high"][index] > high else high
                    volume += response["volume"][index]

            data = {
                "symbol": symbol,
                "open": open,
                "close": close,
                "low": low,
                "high": high,
                "volume": volume,
                "timespan": timespan,
            }

        return data

    def get_candle_history(self, symbol, start_date, end_date, resolution):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if start_date >= end_date:
            start_date, end_date = end_date, start_date

        start_timestamp = round(datetime.datetime.timestamp(start_date))
        stop_timestamp = round(datetime.datetime.timestamp(end_date))

        response = self.__api_get_candle_history(exchange_symbol, start_timestamp, stop_timestamp, resolution)

        data = []
        candle_number = len(response["ticks"])
        for index in range(0, candle_number):
            data.append({
                "symbol": symbol,
                "open": response["open"][index],
                "close": response["close"][index],
                "low": response["low"][index],
                "high": response["high"][index],
                "volume": response["volume"][index],
            })

        return data

    def get_portfolio_margins(self, currency, add_positions=True, simulated_positions={}):
        if not self.__products:
            self.get_products()

        refactored_positions = {}
        print('simulated_positions',simulated_positions)
        for symbol in simulated_positions.keys():
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
            refactored_positions[exchange_symbol] = simulated_positions[symbol]
        print(refactored_positions)
        simulated_positions = refactored_positions
        #data = {"currency": currency}
        response = self.__api_get_portfolio_margin(currency, add_positions, simulated_positions)
        #data["margin"] = response["projected_margin"]
        balance = self.get_balance()
        if balance[currency]['balance'] > 0:
            response['projected_margin']  =  response['projected_margin']/balance[currency]['balance']
        else:
            response['projected_margin'] = 0
        data = response

        return data

    def get_margins(self, symbol, amount, price):
        if not self.__products:
            self.get_products()

        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        #data = {"currency": currency}
        response = self.__api_get_margin(exchange_symbol, amount, price)

        #data["margin"] = response["projected_margin"]
        data = response

        return data

#################
### Websocket ###
#################
    def is_connection_lost(self):
        return self.__connection_lost

    def is_connection_aborted(self):
        return self.__connection_aborted

    def set_reconnect_timeout(self, timeout):
        self.__reconnect_timeout = timeout

    def set_abortion_datetime(self, datetime):
        self.__abortion_datetime = datetime

    def show_websocket_logs(self, show):
        self.__show_websocket_logs = show

    def cleanup(self, force=False):
        if not force:
            disconnect_all = True
            with self.__websocket_channels_mutex:
                for websocket in self.__websocket_channels.keys():
                    if len(self.__websocket_channels[websocket]) > 0:
                        disconnect_all = False
        else:
            disconnect_all = True

        if disconnect_all:

            self.__receive_thread.stop()
            self.__receive_thread.join()

            with self.__websocket_mutex:
                for websocket in self.__websockets:
                    websocket.close()
                self.__websockets = []

            with self.__websocket_handlers_mutex:
                for websocket in self.__websocket_handlers.keys():
                    self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
                self.__websocket_handlers_mutex = {}

            with self.__websocket_channels_mutex:
                self.__websocket_channels = {}

#######################
### Public channels ###
#######################

    def subscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            interval = "100ms"
            group = "none"
            depth = 20
            #channels.append(f"book.{exchange_symbol}.{group}.{depth}.{interval}")
            channels.append(f"book.{exchange_symbol}.{interval}")

        self.__subscribe_dispatcher("orderbook", channels, symbols, auth=False, callback=callback)

    def unsubscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            interval = "100ms"
            group = "none"
            depth = 20
            #channels.append(f"book.{exchange_symbol}.{group}.{depth}.{interval}")
            channels.append(f"book.{exchange_symbol}.{interval}")

        self.__unsubscribe_dispatcher("orderbook", channels, symbols, callback)

    def subscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if not self.__products:
                self.get_products()
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__subscribe_dispatcher("mark_price", channels, symbols, auth=False, callback=callback)

    def unsubscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__unsubscribe_dispatcher("mark_price", channels, symbols, callback)

    def subscribe_funding_rate(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__subscribe_dispatcher("funding_rate", channels, symbols, auth=False, callback=callback)

    def unsubscribe_funding_rate(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__unsubscribe_dispatcher("funding_rate", channels, symbols, callback)

    def subscribe_volume(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__subscribe_dispatcher("volume", channels, symbols, auth=False, callback=callback)

    def unsubscribe_volume(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"incremental_ticker.{exchange_symbol}")

        self.__unsubscribe_dispatcher("volume", channels, symbols, callback)

#########################
### Privates channels ###
#########################

    def subscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"user.orders.{exchange_symbol}.raw")

        self.__subscribe_dispatcher("orders", channels, symbols, auth=True, callback=callback)

    def unsubscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            channels.append(f"user.orders.{exchange_symbol}.raw")

        self.__unsubscribe_dispatcher("orders", channels, symbols, callback)

    def subscribe_balances(self, callback=None):
        if not self.__products:
            self.get_products()

        channels = []
        for currency in self.currencies:
            channels.append(f"user.portfolio.{currency}")

        self.__subscribe_dispatcher("portfolio", channels, self.currencies, auth=True, callback=callback)

    def unsubscribe_balances(self, callback=None):
        if not self.__products:
            self.get_products()

        channels = []
        for currency in self.currencies:
            channels.append(f"user.portfolio.{currency}")

        self.__unsubscribe_dispatcher("portfolio", channels, self.currencies, callback=callback)

    def subscribe_positions(self, callback=None):
        if not self.__products:
            self.get_products()

        channels = ["user.changes.any.any.raw"]
        self.__subscribe_dispatcher("positions", channels, ["any"], auth=True, callback=callback)

    def unsubscribe_positions(self, callback=None):
        if not self.__products:
            self.get_products()

        channels = ["user.changes.any.any.raw"]
        self.__unsubscribe_dispatcher("positions", channels, ["any"], callback=callback)

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_account_summary(self, currency="USDC"):
        response = self.__request("GET", "/private/get_account_summary", payload={
            "currency": currency,
        }, auth=True)
        return response

    def __api_get_products(self, currency):
        response = self.__request("GET", "/public/get_instruments", payload={
            "currency": currency,
        })
        return response

    def __api_get_orderbook_by_symbol(self, product_symbol):
        response = self.__request("GET", "/public/get_order_book", payload={
            "instrument_name": str(product_symbol),
            "depth": 1000,
        })
        return response

    def __api_get_index_price(self, product_symbol):
        response = self.__request("GET", "/public/get_index_price", payload={
            "index_name": str(product_symbol)
        })
        return response

    def __api_get_ticker(self, product_symbol):
        response = self.__request("GET", "/public/ticker", payload={
            "instrument_name": str(product_symbol)
        })
        return response

    def __api_get_index_price(self, product_symbol):
        response = self.__request("GET", "/public/get_index_price", payload={
            "index_name": str(product_symbol)
        })
        return response

    def get_mark_price_history(self, symbol, start_date, stop_date):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        start_timestamp = round(datetime.datetime.timestamp(start_date))
        stop_timestamp = round(datetime.datetime.timestamp(stop_date))

        response = self.__api_get_mark_price_history(exchange_symbol, start_timestamp, stop_timestamp)

    def __api_get_mark_price_history(self, product_symbol, start_timestamp, stop_timestamp):
        response = self.__request("GET", "/public/get_mark_price_history", payload={
            "instrument_name": str(product_symbol),
            "start_timestamp": start_timestamp,
            "stop_timestamp": stop_timestamp,
        })
        return response

    def __api_get_positions(self, currency="USDC"):
        response = self.__request("GET", "/private/get_positions", {
           "currency": str(currency),
        }, auth=True)
        return response

    def __api_get_position(self, product_symbol):
        response = self.__request("GET", "/private/get_position", {
           "instrument_name": str(product_symbol),
        }, auth=True)
        return response

    def __api_get_trade_history(self, currency):
        response = self.__request("GET", "/private/get_user_trades_by_currency", {
            "currency": str(currency),
            "count": 1000,
            "include_old": "true",
        }, auth=True)
        return response['trades']

    def __api_get_transaction_log(self, currency, start, stop):
        response = self.__request("GET", "/private/get_transaction_log", {
            "currency": str(currency),
            "start_timestamp": start,
            "end_timestamp": stop,
            "count": 1000,
        }, auth=True)
        return response

    def __api_place_order(self,
        product_symbol,
        size,
        side,
        order_type,
        stop_price=None,
        limit_price=None,
        time_in_force="gtc",
        trigger="mark_price",
        reduce_only=False):

        payload = {
            "instrument_name": product_symbol,
            "amount": float(size),
        }

        if order_type == "market_order":
            payload["type"] = "market"
        elif order_type == "limit_order":
            payload["type"] = "limit"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            payload["type"] = "stop_market"
            payload["trigger_price"] = stop_price
            payload["trigger"] = trigger
        elif order_type == "stop_limit_order":
            payload["type"] = "stop_limit"
            payload["trigger_price"] = stop_price
            payload["price"] = limit_price
            payload["trigger"] = trigger

        if time_in_force == "gtc":
            payload["time_in_force"] = "good_til_cancelled"
        elif time_in_force == "gtd":
            payload["time_in_force"] = "good_til_day"
        elif time_in_force == "fok":
            payload["time_in_force"] = "fill_or_kill"
        elif time_in_force == "ioc":
            payload["time_in_force"] = "immediate_or_cancel"

        if reduce_only:
            payload["reduce_only"] = "true"

        if str(side) == 'buy':
            response = self.__request("GET", "/private/buy", payload, auth=True)
        else:
            response = self.__request("GET", "/private/sell", payload, auth=True)

        return response

    def __api_get_active_orders_by_currency(self, currency):
        response = self.__request("GET", "/private/get_open_orders_by_currency", {
           "currency": str(currency),
        }, auth=True)
        return response

    def __api_get_active_orders_by_instrument(self, product_symbol):
        response = self.__request("GET", "/private/get_open_orders_by_instrument", {
           "instrument_name": str(product_symbol),
        }, auth=True)
        return response

    def __api_send_rfq(self, product_symbol, side):
        response = self.__request("GET", "/private/send_rfq", {
           "instrument_name": str(product_symbol),
           "side": str(side),
        }, auth=True)
        return response

    def __api_get_order_state(self, order_id):
        response = self.__request("GET", "/private/get_order_state", {
           "order_id": str(order_id),
        }, auth=True)
        return response

    def __api_cancel_order(self, order_id):
        response = self.__request("GET", "/private/cancel", {
           "order_id": str(order_id),
        }, auth=True)
        return response

    def __api_get_order_history(self, currency):
        response = self.__request("GET", "/private/get_order_history_by_currency", {
           "currency": str(currency),
           "include_old": True,
           "include_unfilled": True,
           "count": 1000,
        }, auth=True)
        return response

    def __api_get_settlement_history_by_currency(self, currency):
        response = self.__request("GET", "/private/get_settlement_history_by_currency", {
           "currency": str(currency),
           "include_old": True,
           "include_unfilled": True,
           "count": 1000,
        }, auth=True)
        return response

    def __api_create_deposit_address(self, currency):
        response = self.__request("GET", "/private/create_deposit_address", {
           "currency": currency,
        }, auth=True)
        return response

    def __api_get_deposit_address(self, currency):
        response = self.__request("GET", "/private/get_current_deposit_address", {
           "currency": currency,
        }, auth=True)
        return response

    def __api_withdraw(self, symbol, amount, address):
        response = self.__request("POST", "/private/withdraw", {
           "currency": symbol,
           "address": address,
           "amount": amount,
        }, auth=True)
        return response

    def __api_get_deposits(self, currency="USDC"):
        response = self.__request("GET", "/private/get_deposits", payload={
            "currency": currency,
            "count": 1000,
        }, auth=True)
        return response

    def __api_get_withdrawals(self, currency="USDC"):
        response = self.__request("GET", "/private/get_withdrawals", payload={
            "currency": currency,
            "count": 1000,
        }, auth=True)
        return response

    def __api_get_transfers(self, currency="USDC"):
        response = self.__request("GET", "/private/get_transfers", payload={
            "currency": currency,
            "count": 1000,
        }, auth=True)
        return response

    def __api_get_candle_history(self, symbol, start_date, stop_date, resolution):
        response = self.__request("GET", "/public/get_tradingview_chart_data", payload={
           "instrument_name": symbol,
           "start_timestamp": start_date,
           "end_timestamp": stop_date,
           "resolution": resolution,
        })
        return response

    def __api_get_portfolio_margin(self, currency, add_positions=True, simulated_positions={}):
        params = {
            "currency": currency,
            "add_positions": "true" if add_positions else "false",
        }
        if len(simulated_positions) >0:
            params["simulated_positions"]= json.dumps(simulated_positions)

        response = self.__request("GET", "/private/get_portfolio_margins", payload=params, auth=True)
        return response

    def __api_get_margin(self, symbol, amount, price):
        params = {
            "instrument_name": symbol,
            "amount": amount,
            "price": price,
        }
        response = self.__request("GET", "/private/get_margins", payload=params, auth=True)
        return response

    def __get_symbol_usd_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        price = None
        convs = ["USDC", "USD", "USDT", "BUSD"]

        if is_fiat(symbol):
            price = CbForex(self.database).get_rate(symbol, "USD", date)
        elif symbol in convs:
            price = 1
        elif not date:
            rate = None
            if symbol in self.currencies:
                rate = self.__api_get_index_price(f"{symbol.lower()}_usd")
                if rate:
                    price = rate["index_price"]
            else:
                rate = self.get_mark_price(symbol)
                if rate:
                    price = rate["mark_price"]
        else:
            candle = None
            if symbol in self.currencies:
                for conv in convs:
                    if f"{symbol}_PERP_{conv}" in self.__products:
                        candle = self.get_candle(f"{symbol}_PERP_{conv}", date, date-datetime.timedelta(minutes=10))
                        if candle:
                            price = candle["close"]
                            break
            else:
                candle = self.get_candle(symbol, date, date-datetime.timedelta(minutes=1))
                if candle:
                    price = candle["close"]

        return price

    def __get_timestamp(self):
        d = datetime.datetime.utcnow()
        epoch = datetime.datetime(1970,1,1)
        return str(int((d - epoch).total_seconds()))

    def __request(self, method, path, payload=None, auth=False, base_url=None, headers=None):
        if base_url is None:
            base_url = self.base_url
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = '%s%s' % (base_url, path)

        headers["Accept"] = "application/json"

        if auth:
            if not self.__is_authenticated():
                self.__authenticate()
            headers["Authorization"] = "Bearer " + self.__auth_token

        if method == "GET":
            payload_str = urlencode(payload)
            response = self.session.request(method, f"{url}?{payload_str}", headers=headers)
        else:
            response = self.session.request(method, url, params=payload, headers=headers)
        self.__request_log(response)

        try:
            parsed_response = self.__parse_response(response)

        except OrderNotFoundError as e:
            # Set exception's order id
            e.order_id = payload["order_id"] if "order_id" in payload else None
            raise e

        return parsed_response

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:

            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)

            if response_json["error"]:
                if response_json["error"]["code"] == DeribitErrorCodes.ORDER_NOT_FOUND:
                    raise OrderNotFoundError
                if response_json["error"]["code"] == DeribitErrorCodes.FORBIDDEN:
                    raise ForbiddenError(f"Forbidden action : {response_json['error']['data']}")
                if response_json["error"]["code"] == DeribitErrorCodes.UNAUTHORIZED:
                    raise UnauthorizedError
                if response_json["error"]["code"] == DeribitErrorCodes.NOT_ENOUGH_FUNDS:
                    raise NotEnoughFundsError
                raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["error"]))
            else:
                raise requests.exceptions.HTTPError(status_code)
        else:
            response = response.json()
            return response["result"]

    def __authenticate(self):
        response = self.__request("GET", "/public/auth", payload={
            "grant_type": "client_credentials",
            "client_id": self.api_key,
            "client_secret": self.api_secret
        })
        self.__auth_token = response["access_token"]
        self.__auth_refresh_token = response["refresh_token"]
        self.__auth_timeout = time.time() + response["expires_in"]

    def __is_authenticated(self):
        if not self.__auth_timeout:
            return False
        if self.__auth_timeout < time.time():
            return False
        return True

    def __refresh_token(self):
        if not self.__is_authenticated():
            self.__authenticate()

        response = self.__request("GET", "/public/auth", payload={
            "grant_type": "refresh_token",
            "refresh_token": self.__auth_refresh_token,
        })
        self.__auth_token = response["access_token"]
        self.__auth_refresh_token = response["refresh_token"]
        self.__auth_timeout = time.time() + response["expires_in"]
        return response

    def __compose_product_symbol(self, response):
        base_asset_symbol = response["base_currency"]
        quote_asset_symbol = response["quote_currency"]
        strike_price = round(float(response["strike"])) if "strike" in response else None
        settlement_time = datetime.datetime.fromtimestamp(response["expiration_timestamp"]/1000,
                            tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        settlement_date = settlement_time[:10] if settlement_time else None

        if response["kind"] == "future":
            if response["settlement_period"] == "perpetual":
                generated_symbol = f"{base_asset_symbol}_PERP_{quote_asset_symbol}"
            else:
                generated_symbol = f"{base_asset_symbol}_FUT_{settlement_date}_{quote_asset_symbol}"
        elif response["kind"] == "option":
            if response["option_type"] == "put":
                generated_symbol = f"{base_asset_symbol}_PUT_{strike_price}_{settlement_date}_{quote_asset_symbol}"
            else:
                generated_symbol = f"{base_asset_symbol}_CALL_{strike_price}_{settlement_date}_{quote_asset_symbol}"
        else:
            generated_symbol = None
        return generated_symbol

    def __compose_contract_type(self, response):
        if response["kind"] == "future":
            if response["settlement_period"] == "perpetual":
                contract_type = "perpetual_future"
            else:
                contract_type = "future"
        elif response["kind"] == "option":
            if response["option_type"] == "put":
                contract_type = "put_option"
            else:
                contract_type = "call_option"
        else:
            contract_type = response["kind"]
        return contract_type

    def __compose_order_status(self, response):
        order_status = "unknown"
        if (response["order_state"] == "open" or
            response["order_state"] == "untriggered" or
            (response["order_state"] == "triggered" and response["filled_amount"] == 0)):
            order_status = "open"
        elif (response["order_state"] == "filled" and
            response["filled_amount"] == response["amount"]):
            order_status= "filled"
        elif (response["order_state"] == "filled" and
            response["filled_amount"] != response["amount"]):
            order_status = "partially_filled"
        elif response["order_state"] == "cancelled":
            order_status = "cancelled"
        return order_status

    def __compose_time_in_force(self, response):
        if response["time_in_force"] == "good_til_cancelled":
            time_in_force = "gtc"
        elif response["time_in_force"] == "fill_or_kill":
            time_in_force = "fok"
        elif response["time_in_force"] == "immediate_or_cancel":
            time_in_force = "ioc"
        else:
            time_in_force = "gtc"
        return time_in_force

    def __round_size(self, product_symbol, size):
        # Get products if not done yet
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["amount"]["min"]:
            rounded_size = float(Decimal(str(size)) - Decimal(str(size)) % Decimal(str(self.__products[product_symbol]["limits"]["amount"]["min"])))
            return rounded_size
        return size

    def __round_price(self, product_symbol, price):
        # Get products if not done yet
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["price"]["min"]:
            min_price = self.__products[product_symbol]["limits"]["price"]["min"]

            # Min price may depend on price value
            if product_symbol in self.__tick_size_steps:
                for tick_size_step in self.__tick_size_steps[product_symbol]:
                    if price > tick_size_step["above_price"]:
                        min_price = tick_size_step["tick_size"]

            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(min_price)))
            return rounded_price
        return price

    def __compute_option_average_price(self, order):
        average_price = None
        trades = order["trades"]
        if len(trades) > 0:
            average_price = 0
            sum_amount = 0
            for trade in trades:
                average_price += trade["price"]*trade["underlying_price"]*trade["amount"]
                sum_amount += trade["amount"]
            average_price = average_price / sum_amount
        else:
            average_price = 0

        if average_price == None:
            average_price = order["price"] if "price" in order else 0

        return average_price

    def __log_order(self, order_id, symbol, size, side, order_type, status, execution_price):
        try:
            self.strategy.log_trade(order_id, symbol, size, side, order_type, status, execution_price)
        except Exception as e:
            log(f"[{self}][ERROR] Unable to log order : {type(e).__name__} - {e}\n{format_traceback(e.__traceback__)}")

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
                    self.__websocket_channels[ws] = {}

                # Connect websocket
                ws.connect(self.ws_url)

                # Launch reception thread
                if not self.__receive_thread:
                    self.__receive_thread = StoppableThread(name="Thread-Deribit-websocket_callback", target=self.__websocket_callback)
                    self.__receive_thread.start()

                # Tell server to enable heartbeat
                self.__heartbeat(ws)

                # Setup websocket handler
                with self.__websocket_handlers_mutex:
                    self.__websocket_handlers[ws] = {
                        "is_connected": True,
                        "is_auth": False,
                        "connection_lost": False,
                        "connection_aborted": False,
                        "reconnect_timer": threading.Timer(self.__heartbeat_timeout, self.__reconnect_timer, [ws])
                    }
                    self.__websocket_handlers[ws]["reconnect_timer"].name = f"Thread-Deribit-reconnect-{hex(id(ws))}"
                    self.__websocket_handlers[ws]["reconnect_timer"].start()

                self.__websocket_number += 1
                self.__ws_log(f"New websocket created : {ws}.", "CREATE")

        return ws

    def __delete_websocket(self, websocket):
        #self.__ws_log(f"Removing websocket '{websocket}'.", "DELETE")
        log(f"[{self}][DELETE] Removing websocket '{websocket}'")

        # Remove websocket handler
        with self.__websocket_handlers_mutex:
            if self.__websocket_handlers[websocket]["reconnect_timer"]:
                self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
            del self.__websocket_handlers[websocket]

        # Close and remove websocket
        with self.__websocket_mutex:

            # Remove websocket
            self.__websockets.remove(websocket)

            # Close websocket connection
            websocket.close()

            self.__websocket_number -= 1

            # Stop reception thread if no websocket anymore
            if self.__websocket_number == 0:
                self.__receive_thread.stop()
                log(f"[{self}][DELETE] Joining reception thread")
                #self.__receive_thread.join()
                log(f"[{self}][DELETE] Reception thread joined")
                self.__receive_thread = None

        #self.__ws_log(f"Websocket removed '{websocket}'.", "DELETE")
        log(f"[{self}][DELETE] Websocket removed '{websocket}'")

    def __subscribe_dispatcher(self, event, channels, symbols, auth=False, callback=None):
        self.__ws_log(f"Subscribing to '{event}' with symbols '{symbols}'", "SUBSCRIBE")

        # Remove already registered channels and symbols
        with self.__websocket_mutex:
            for ws in self.__websockets:
                with self.__websocket_channels_mutex:
                    if event in self.__websocket_channels[ws]:
                        for channel in list(channels):
                            if channel in self.__websocket_channels[ws][event]["channels"]:
                                if callback and callback not in self.__websocket_channels[ws][event]["callbacks"]:
                                    self.__ws_log(f"Add new callback to already registered channel '{channel}'.", "SUBSCRIBE")
                                    self.__websocket_channels[ws][event]["callbacks"].append(callback)
                                else:
                                    self.__ws_log(f"Skip already registered channel '{channel}'.", "SUBSCRIBE")
                                channels.remove(channel)
                        for symbol in list(symbols):
                            if symbol in self.__websocket_channels[ws][event]["symbols"]:
                                self.__ws_log(f"Skip already registered symbol '{symbol}'.", "SUBSCRIBE")
                                symbols.remove(symbol)

        # Exit if no channel to register
        if len(channels) == 0:
            return

        with self.__subscribe_mutex:

            # No limit on channel
            if event not in self.__CHANNELS_LIMITS:
                self.__ws_log(f"No limits on event '{event}'.", "SUBSCRIBE")

                # Create a websocket if not existing yet
                if self.__websocket_number == 0:
                    ws = self.__create_websocket()

                with self.__websocket_mutex:

                    # Subscribe channels on websocket
                    self.__ws_log(f"Register channels '{channels}' on websocket '{ws}'.", "SUBSCRIBE")
                    ws = self.__websockets[0]
                    self.__subscribe(ws, channels, auth)

                    # Add event and symbols to websocket channels
                    with self.__websocket_channels_mutex:

                        if ws not in self.__websocket_channels:
                            self.__websocket_channels[ws] = {
                                event: {
                                    "auth": auth,
                                    "callbacks": [],
                                    "channels": [],
                                    "symbols": [],
                                },
                            }
                        elif event not in self.__websocket_channels[ws]:
                            self.__websocket_channels[ws][event] = {
                                "auth": auth,
                                "callbacks": [],
                                "channels": [],
                                "symbols": [],
                            }

                            # Add channels and symbols to websocket
                            self.__websocket_channels[ws][event]["channels"].extend(channels)
                            self.__websocket_channels[ws][event]["symbols"].extend(symbols)

                            # Add callback to websocket
                            if callback and callback not in self.__websocket_channels[ws][event]["callbacks"]:
                                self.__websocket_channels[ws][event]["callbacks"].append(callback)

            # Limits on channel
            else:
                channel_limit = self.__CHANNELS_LIMITS[event]
                self.__ws_log(f"Limits on event '{event}' ({channel_limit}).", "SUBSCRIBE")

                # Register channels on existing websockets
                with self.__websocket_mutex:
                    channel_index = 0
                    for ws in self.__websockets:

                        # Skip websocket if not connected
                        with self.__websocket_handlers_mutex:
                            if not self.__websocket_handlers[ws]["is_connected"]:
                                continue

                        # Get number of channels registered on websocket
                        with self.__websocket_channels_mutex:

                            if event not in self.__websocket_channels[ws]:
                                self.__ws_log(f"Event '{event}' not registered on {ws}", "SUBSCRIBE")
                                self.__websocket_channels[ws][event] = {
                                    "auth": auth,
                                    "callbacks": [],
                                    "channels": [],
                                    "symbols": [],
                                }
                            channel_number_on_event = len(self.__websocket_channels[ws][event]["channels"])

                            # Add callback to websocket channels
                            if callback and callback not in self.__websocket_channels[ws][event]["callbacks"]:
                                self.__websocket_channels[ws][event]["callbacks"].append(callback)

                        # Get channels to register on event on websocket
                        if channel_number_on_event == channel_limit:
                            self.__ws_log(f"Websocket '{ws}' already full on channel '{channel}'", "SUBSCRIBE")
                            if self.__websocket_number == self.__max_websocket:
                                self.__ws_log(f"[ERROR] Maximum number of websocket already reached. Cancelling next subscribtions on '{channel}''", "SUBSCRIBE")
                                return
                            else:
                                continue
                        elif channel_number_on_event + len(channels) <= channel_limit:
                            channel_number_to_register = len(channels)
                            channels_to_register = channels
                            symbols_to_register = symbols
                        else:
                            channel_number_to_register = channel_limit - channel_number_on_event
                            channels_to_register = channels[channel_index:channel_index+channel_number_to_register]
                            symbols_to_register = symbols[channel_index:channel_index+channel_number_to_register]

                        # Subscribe channels on event on websocket
                        self.__ws_log(f"Register channels '{channels_to_register}' on websocket '{ws}'.", "SUBSCRIBE")
                        self.__subscribe(ws, channels_to_register, auth)
                        channel_index += channel_number_to_register

                        # Add channels and symbols to websocket channels
                        with self.__websocket_channels_mutex:
                            self.__websocket_channels[ws][event]["channels"].extend(channels_to_register)
                            self.__websocket_channels[ws][event]["symbols"].extend(symbols_to_register)

                        # Break loop if all symbols are registered
                        if channel_index >= len(channels):
                            break

                # Register symbols on new websockets
                while channel_index < len(channels):

                    # Create new websocket
                    if not (ws := self.__create_websocket()):
                        return

                    # Get number of channels registered on websocket
                    with self.__websocket_channels_mutex:

                        if event not in self.__websocket_channels[ws]:
                            self.__ws_log(f"Event '{event}' not registered on {ws}", "SUBSCRIBE")
                            self.__websocket_channels[ws][event] = {
                                "auth": auth,
                                "callbacks": [],
                                "channels": [],
                                "symbols": [],
                            }
                        channel_number_on_event = len(self.__websocket_channels[ws][event]["channels"])

                        # Add callback to websocket channels
                        if callback and callback not in self.__websocket_channels[ws][event]["callbacks"]:
                            self.__websocket_channels[ws][event]["callbacks"].append(callback)

                    # Get channels to register on event on websocket
                    if channel_number_on_event + len(channels[channel_index:]) <= channel_limit:
                        channel_number_to_register = len(channels[channel_index:])
                        channels_to_register = channels[channel_index:]
                        symbols_to_register = symbols[channel_index:]
                    else:
                        channel_number_to_register = channel_limit - channel_number_on_event
                        channels_to_register = channels[channel_index:channel_index+channel_number_to_register]
                        symbols_to_register = symbols[channel_index:channel_index+channel_number_to_register]

                    # Subscribe channels on event on websocket
                    self.__ws_log(f"Register channels '{channels_to_register}' on websocket '{ws}'.", "SUBSCRIBE")
                    self.__subscribe(ws, channels_to_register, auth)
                    channel_index += channel_number_to_register

                    # Add channels and symbols to websocket channels
                    with self.__websocket_channels_mutex:
                        self.__websocket_channels[ws][event]["channels"].extend(channels_to_register)
                        self.__websocket_channels[ws][event]["symbols"].extend(symbols_to_register)

    def __unsubscribe_dispatcher(self, event, channels, symbols, callback=None):
        self.__ws_log(f"Unsubscribing from '{event}' with symbols '{symbols}'", "UNSUBSCRIBE")

        websockets_to_remove = []
        with self.__subscribe_mutex:
            with self.__websocket_channels_mutex:
                for websocket in self.__websocket_channels.keys():
                    channels_to_unsubscribe = []

                    for channel in channels:
                        if event in self.__websocket_channels[websocket] and channel in self.__websocket_channels[websocket][event]["channels"]:
                            channels_to_unsubscribe.append(channel)

                    # Unsubscribe channels on websocket
                    if len(channels_to_unsubscribe) > 0:

                        # Send unsubscribe message only if websocket is connected
                        with self.__websocket_handlers_mutex:
                            if self.__websocket_handlers[websocket]["is_connected"]:
                                self.__unsubscribe(websocket, channels_to_unsubscribe, self.__websocket_channels[websocket][event]["auth"])

                        for channel in channels_to_unsubscribe:
                            # Remove channel from event registered ones
                            self.__websocket_channels[websocket][event]["channels"].remove(channel)

                        for symbol in symbols:
                            if event in self.__websocket_channels[websocket] and symbol in self.__websocket_channels[websocket][event]["symbols"]:
                                # Remove symbol from event registered ones
                                self.__websocket_channels[websocket][event]["symbols"].remove(symbol)

                    # Remove callback if no channel registered anymore
                    if (len(self.__websocket_channels[websocket][event]["channels"]) == 0 and
                        callback in self.__websocket_channels[websocket][event]["callbacks"]):
                            self.__websocket_channels[websocket][event]["callbacks"].remove(callback)

                    # Remove all callbacks if none given
                    if callback is None:
                        self.__websocket_channels[websocket][event]["callbacks"] = []

                    # Remove event if no channel registered anymore
                    if event in self.__websocket_channels[websocket] and len(self.__websocket_channels[websocket][event]["channels"]) == 0:
                        del self.__websocket_channels[websocket][event]

                    # Delete websocket if no event subscribed anymore
                    if len(self.__websocket_channels[websocket]) == 0:
                        self.__delete_websocket(websocket)
                        websockets_to_remove.append(websocket)

                # Remove empty websockets from subscribed channels
                for websocket in websockets_to_remove:
                    del self.__websocket_channels[websocket]

            self.__ws_log(f"Unsubscribed from '{event}' with symbols '{symbols}'", "UNSUBSCRIBE")

    def __subscribe(self, websocket, channels, auth=False):
        self.__ws_log(f"Subscribing to '{channels}' on {websocket}", "SUBSCRIBE")

        # Authenticate on websocket if needed
        if auth:
            if not self.__is_authenticated():
                self.__authenticate()
            method = "/private/subscribe"
        else:
            method = "/public/subscribe"

        data = {
            "method": method,
            "params": {
                "channels": channels,
            }
        }

        if auth:
            data["params"]["access_token"] = self.__auth_token

        # Send subscribe message on websocket
        log(f"[{self}][SUBSCRIBE] Subscribe on {websocket} with {len(channels)} symbols")
        self.__ws_send(websocket, data)

        self.__ws_log(f"Subscribed to '{channels}' on {websocket}", "SUBSCRIBE")

    def __unsubscribe(self, websocket, channels, auth=False):
        self.__ws_log(f"Unsubscribing from '{channels}' on {websocket}", "UNSUBSCRIBE")

        if auth:
            method = "/private/unsubscribe"
        else:
            method = "/public/unsubscribe"

        # Send unsubscribe message on websocket
        log(f"[{self}][UNSUBSCRIBE] Unsubscribe on {websocket} with {len(channels)} symbols")
        self.__ws_send(websocket, {
            "method": method,
            "params": {
                "channels": channels,
            }
        })

        self.__ws_log(f"Unsubscribed from '{channels}' on {websocket}", "UNSUBSCRIBE")

    def __reset_timer(self, websocket):
        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
            self.__websocket_handlers[websocket]["reconnect_timer"] = threading.Timer(self.__heartbeat_timeout, self.__reconnect_timer, [websocket])
            self.__websocket_handlers[websocket]["reconnect_timer"].name = f"Thread-Deribit-reconnect-{hex(id(websocket))}"
            self.__websocket_handlers[websocket]["reconnect_timer"].start()

    def __reconnect_timer(self, websocket):
        log(f"[{self}][RECONNECT-TIMER] Timer triggered on {websocket}")
        #try:
        #    self.__reconnect(websocket)
        #except Exception as e:
        #    log(f"[{self}][RECONNECT] Reconnection failed : {e}")
        #    log(format_traceback(e.__traceback__))
        #    self.__delete_websocket(websocket)
        #    with self.__websocket_channels_mutex:
        #        del self.__websocket_channels[websocket]
        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = False
            self.__websocket_handlers[websocket]["is_auth"] = False
        websocket.close()

    def __reconnect(self, websocket):
        #self.__ws_log(f"Reconnecting on {websocket}", "RECONNECT")
        log(f"[{self}][RECONNECT] Reconnecting on {websocket}")

        # Reset websocket handler
        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = False
            self.__websocket_handlers[websocket]["is_auth"] = False
            if self.__websocket_handlers[websocket]["reconnect_timer"]:
                self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
                self.__websocket_handlers[websocket]["reconnect_timer"] = None

        time.sleep(1)
        # Connect websocket
        with self.__websocket_mutex:
            websocket.connect(self.ws_url)
            self.__heartbeat(websocket)

        # Set websocket handler
        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = True
            self.__websocket_handlers[websocket]["is_auth"] = False
            self.__websocket_handlers[websocket]["connection_lost"] = False
            self.__websocket_handlers[websocket]["connection_aborted"] = False
            self.__websocket_handlers[websocket]["reconnect_timer"] = threading.Timer(self.__heartbeat_timeout, self.__reconnect_timer, [websocket])
            self.__websocket_handlers[websocket]["reconnect_timer"].name = f"Thread-Deribit-reconnect-{hex(id(websocket))}"
            self.__websocket_handlers[websocket]["reconnect_timer"].start()

        # Resubscribe websocket channels
        with self.__subscribe_mutex:
            with self.__websocket_channels_mutex:
                for event in self.__websocket_channels[websocket].keys():
                    channels = self.__websocket_channels[websocket][event]["channels"]
                    auth = self.__websocket_channels[websocket][event]["auth"]
                    self.__subscribe(websocket, channels, auth)

        #self.__ws_log(f"Reconnected on {websocket}", "RECONNECT")
        log(f"[{self}][RECONNECT] Reconnected on {websocket}")

    def __reconnection_loop(self, websocket):
        #self.__ws_log(f"Start reconnection loop on websocket {websocket}", "RECONNECT")
        log(f"[{self}][RECONNECT] Start reconnection loop on websocket {websocket}")
        reconnect_tries = 1
        connection_lost = True
        while connection_lost:
            now = datetime.datetime.now()
            if self.__abortion_datetime != None and now >= self.__abortion_datetime:
                break

            #self.__ws_log(f"Tries to reconnect websocket {websocket} (try {reconnect_tries})", "RECONNECT")
            log(f"[{self}][RECONNECT] Tries to reconnect websocket {websocket} (try {reconnect_tries})")

            try:
                self.__reconnect(websocket)
                self.__abortion_datetime = None
                #self.__ws_log(f"Reconnect succeeded on websocket {websocket}", "RECONNECT")
                log(f"[{self}][RECONNECT] Reconnect succeeded on websocket {websocket}")
                break
            except:
                #self.__ws_log(f"Reconnect failed on websocket {websocket}", "RECONNECT")
                log(f"[{self}][RECONNECT] Reconnect failed on websocket {websocket}")
                reconnect_tries += 1
            time.sleep(self.__reconnect_timeout)

        if connection_lost:
            self.__connection_aborted = True

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

    def __on_close(self, websocket):

        # Do nothing if websocket deleted by connector
        with self.__websocket_mutex:
            if websocket not in self.__websockets:
                return

        #self.__ws_log(f"Websocket {websocket} closed", "ON_CLOSE")
        log(f"[{self}][ON_CLOSE] Websocket {websocket} closed")

        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = False
            self.__websocket_handlers[websocket]["is_auth"] = False
            self.__websocket_handlers[websocket]["connection_lost"] = True
            if self.__websocket_handlers[websocket]["reconnect_timer"]:
                self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
                self.__websocket_handlers[websocket]["reconnect_timer"] = None

        # Start background reconnection thread
        #threading.Thread(target=self.__reconnection_loop, args=[websocket]).start()

        try:
            self.__reconnect(websocket)
        except Exception as e:
            log(f"[{self}][RECONNECT] Reconnection failed : {e}")
            log(format_traceback(e.__traceback__))
            self.__delete_websocket(websocket)
            with self.__websocket_channels_mutex:
                del self.__websocket_channels[websocket]

    def __on_message(self, websocket, message):
        self.__ws_log(message, f"RECEIVE - {websocket}")

        data = json.loads(message)

        if "method" in data:
            method = data["method"]

            if method == "heartbeat":
                self.__reset_timer(websocket)
                if data["params"]["type"] == "test_request":
                    self.__answer_heartbeat(websocket)
            elif method == "subscription":
                channel = data["params"]["channel"]

                # Orderbook update
                if channel.startswith("book."):

                    # Check message delay
                    event_timestamp = datetime.datetime.fromtimestamp(data["params"]["data"]["timestamp"]/1000)
                    reception_timestamp = datetime.datetime.now()
                    timestamp_delay = abs(reception_timestamp - event_timestamp)
                    if timestamp_delay > datetime.timedelta(seconds=0.5):
                        self.__ws_log(f"Message delayed by {timestamp_delay} (event {event_timestamp} / received {reception_timestamp}", f"DELAY - {websocket}")
                        #return

                    data = data["params"]["data"]
                    events = ["orderbook"]

                    product = [product for key, product in self.__products.items() if product["exchange_symbol"] == data["instrument_name"]][0]
                    symbol = product["symbol"]

                    if symbol not in self.__orderbooks:
                        self.__orderbooks[symbol] = {"symbol": symbol, "buy": [], "sell": []}

                    data_refactored = {
                        "orderbook": {
                            "symbol": symbol,
                            "exchange_id": product["exchange_id"],
                            "exchange_symbol": product["exchange_symbol"],
                            "buy": [],
                            "sell": [],
                        },
                    }

                    if data["type"] == "snapshot":
                        self.__orderbooks[symbol]["buy"] = []
                        self.__orderbooks[symbol]["sell"] = []

                        for order in data["bids"]:
                            self.__orderbooks[symbol]["buy"].append({"price": float(order[1]), "size": float(order[2])})
                        for order in data["asks"]:
                            self.__orderbooks[symbol]["sell"].append({"price": float(order[1]), "size": float(order[2])})

                    elif data["type"] == "change":

                        for order in data["bids"]:
                            action = order[0]
                            price = order[1]
                            size = order[2]

                            # Size change
                            if action == "change" or action == "delete":
                                index = next((index for (index, existing_order) in enumerate(self.__orderbooks[symbol]["buy"]) if existing_order["price"] == price), None)

                                if index != None:
                                    # Remove price
                                    if size == 0:
                                        del self.__orderbooks[symbol]["buy"][index]
                                    # Update size
                                    else:
                                        self.__orderbooks[symbol]["buy"][index]["size"] = size
                            # New price
                            else:
                                # Insert price in sorted list
                                inserted = False
                                for index, existing_order in enumerate(self.__orderbooks[symbol]["buy"]):
                                    if price > existing_order["price"]:
                                        self.__orderbooks[symbol]["buy"].insert(index, {"price": price, "size": size})
                                        inserted = True
                                        break
                                if not inserted:
                                    self.__orderbooks[symbol]["buy"].append({"price": price, "size": size})

                        for order in data["asks"]:
                            action = order[0]
                            price = order[1]
                            size = order[2]

                            # Size change
                            if action == "change" or action == "delete":
                                index = next((index for (index, existing_order) in enumerate(self.__orderbooks[symbol]["sell"]) if existing_order["price"] == price), None)

                                if index != None:
                                    # Remove price
                                    if size == 0:
                                        del self.__orderbooks[symbol]["sell"][index]
                                    # Update size
                                    else:
                                        self.__orderbooks[symbol]["sell"][index]["size"] = size
                            # New price
                            else:
                                # Insert price in sorted list
                                inserted = False
                                for index, existing_order in enumerate(self.__orderbooks[symbol]["sell"]):
                                    if price < existing_order["price"]:
                                        self.__orderbooks[symbol]["sell"].insert(index, {"price": price, "size": size})
                                        inserted = True
                                        break
                                if not inserted:
                                    self.__orderbooks[symbol]["sell"].append({"price": price, "size": size})

                    data_refactored["orderbook"]["buy"] = copy.deepcopy(self.__orderbooks[symbol]["buy"])
                    data_refactored["orderbook"]["sell"] = copy.deepcopy(self.__orderbooks[symbol]["sell"])

                    """
                    symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["instrument_name"]][0]

                    data_refactored = {
                        "orderbook": {
                            "symbol": symbol,
                            "buy": [],
                            "sell": [],
                        }
                    }

                    for order in data["bids"]:
                        order_refactored = {
                            "price": order[0],
                            "size": order[1],
                        }
                        data_refactored["orderbook"]["buy"].append(order_refactored)
                    for order in data["asks"]:
                        order_refactored = {
                            "price": order[0],
                            "size": order[1],
                        }
                        data_refactored["orderbook"]["sell"].append(order_refactored)
                    """

                # Ticker update
                elif channel.startswith("incremental_ticker"):
                    events = ["mark_price", "funding_rate", "volume"]
                    data = data["params"]["data"]

                    symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["instrument_name"]][0]

                    if symbol not in self.__last_mark_price:
                        self.__last_mark_price[symbol] = None
                    if symbol not in self.__last_funding_rate:
                        self.__last_funding_rate[symbol] = None
                    if symbol not in self.__last_volume:
                        self.__last_volume[symbol] = None

                    self.__last_mark_price[symbol] = data["mark_price"] if "mark_price" in data else self.__last_mark_price[symbol]
                    self.__last_funding_rate[symbol] = data["funding_8h"] if "funding_8h" in data else self.__last_funding_rate[symbol]
                    self.__last_volume[symbol] = data["stats"]["volume_usd"] if "stats" in data and "volume_usd" in data["stats"] else self.__last_volume[symbol]

                    data_refactored = {
                        "mark_price": {
                            "symbol": symbol,
                            "mark_price": self.__last_mark_price[symbol],
                        },
                        "funding_rate": {
                            "symbol": symbol,
                            "funding_rate": self.__last_funding_rate[symbol],
                        },
                        "volume": {
                            "symbol": symbol,
                            "volume": self.__last_volume[symbol],
                        }
                    }

                # Order update
                elif channel.startswith("user.orders"):
                    events = ["orders"]
                    data = data["params"]["data"]

                    symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["instrument_name"]][0]

                    data_refactored = {
                        "orders": {
                            "id": str(data["order_id"]),
                            "type": data["order_type"],
                            "status": self.__compose_order_status(data),
                            "symbol": symbol,
                            "exchange_id": data["instrument_name"],
                            "exchange_symbol": data["instrument_name"],
                            "side": data["direction"],
                            "size": data["amount"],
                            "filled_size": data["filled_amount"],
                            "unfilled_size": data["amount"] - data["filled_amount"],
                            "average_price": data["average_price"],
                            "limit_price": data["price"],
                            "stop_price": data["trigger_price"] if "trigger_price" in data else None,
                            "time_in_force": self.__compose_time_in_force(data),
                            "commission": 0
                        }
                    }

                    # Overwrite order_id if order has been created from a trigger order
                    if "trigger_order_id" in data and data["trigger_order_id"] != data["order_id"]:
                        data_refactored["orders"]["id"] = data["trigger_order_id"]

                # Portfolio update
                elif channel.startswith("user.portfolio"):
                    events = ["portfolio"]

                    data = data["params"]["data"]

                    data_refactored = {
                        "portfolio": {
                            "symbol": data["currency"],
                            "exchange_id": data["currency"],
                            "exchange_symbol": data["currency"],
                            "balance": data["balance"],
                            "available_balance": data["available_funds"],
                            "initial_margin": data["initial_margin"],
                            "maintenance_margin": data["maintenance_margin"],
                        }
                    }

                # Positions update
                elif channel.startswith("user.changes"):
                    events = ["positions"]

                    positions = data["params"]["data"]["positions"]

                    data_refactored = {"positions": {}}
                    for position in positions:
                        product = [product for key, product in self.__products.items() if product["exchange_symbol"] == position["instrument_name"]][0]
                        symbol = product["symbol"]

                        data_refactored["positions"][symbol] = {
                            "symbol": symbol,
                            "exchange_id": position["instrument_name"],
                            "exchange_symbol": position["instrument_name"],
                            "size": position["size"] if position["direction"] == "buy" else -position["size"],
                            "entry_price": position["average_price"],
                            "maintenance_margin": position["maintenance_margin"],
                            "contract_type": product["contract_type"],
                            "base_asset_id": product["base_asset_id"],
                            "base_asset_symbol": product["base_asset_symbol"],
                            "quote_asset_id": product["quote_asset_id"],
                            "quote_asset_symbol": product["quote_asset_symbol"],
                        }

                else:
                    data_refactored = data

                with self.__websocket_channels_mutex:
                    for event in events:
                        if (websocket in self.__websocket_channels and
                            event in self.__websocket_channels[websocket]):

                            # Call channel callbacks
                            for callback in self.__websocket_channels[websocket][event]["callbacks"]:
                                try:
                                    callback(self, copy.deepcopy(data_refactored[event]))
                                except Exception as e:
                                    log(f"[{self}] An exception occured into callback with input data : '{data_refactored[event]}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __heartbeat(self, websocket, enable=True):
        if enable:
            self.__ws_log("Enable heartbeat")
            method = "public/set_heartbeat"
        else:
            self.__ws_log("Disable heartbeat")
            method = "public/disable_heartbeat"
        self.__ws_send(websocket, {
            "method": method,
            "params": {
                "interval": self.__heartbeat_timeout-5,
            }
        })

    def __answer_heartbeat(self, websocket):
        self.__ws_send(websocket, {
            "method": "/public/test",
        })

    def __ws_send(self, websocket, data):
        self.__ws_log(data, "SEND")
        websocket.send(json.dumps(data))

    def __ws_log(self, message, header=""):
        if self.__show_websocket_logs:
            if header:
                message = f"[{header}] {message}"
            log(f"[{self}] {message}")

    def __request_log(self, response):
        try:
            file_path = "deribit_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}] [{self.strategy}] [TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}] [{self.strategy}] [FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except:
            pass

    def __str__(self):
        return f"{self.PLATFORM_NAME} {self.strategy}"

