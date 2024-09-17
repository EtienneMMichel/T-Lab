from connectors.threading.Threads import StoppableThread, format_traceback
from connectors.crypto.connector.common.connector import CryptoConnector
import connectors.crypto.connector.common.websocket_codes as op_codes
from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from decimal import Decimal
import pandas as pd
import websocket
import threading
import requests
import datetime
import hashlib
import urllib
import select
import queue
import time
import hmac
import json
import copy
import sys
import io

requests.packages.urllib3.util.connection.HAS_IPV6 = False

class DeltaExchange(CryptoConnector):
    PLATFORM_ID = 2
    PLATFORM_NAME = "Delta Exchange"

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
            self.base_url = "https://testnet-api.delta.exchange"
            self.ws_url = "wss://testnet-socket.delta.exchange"
        else:
            self.base_url = "https://api.delta.exchange"
            #self.base_url = "https://cdn.deltaex.org"
            self.ws_url = "wss://socket.delta.exchange"
            #self.ws_url = "ws://localhost:12345"

        self.option_fees = 0.0005
        self.future_fees = 0.001

        self.__assets = {}
        self.__products = {}
        self.__orderbooks = {}
        self.__ignored_assets = ["XRP"]

        self.__CHANNELS_LIMITS = {
            "l2_orderbook": 20,
            "l2_updates": 20,
        }
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

######################
### Public methods ###
######################

    def get_contract_size(self, spot_symbol):
        if spot_symbol == 'BTC':
            contract_size = 0.001
        elif spot_symbol == 'ETH':
            contract_size = 0.01
        else:
            contract_size = 1
        return contract_size

    def get_formatted_price(self, symbol, price):
        return price

################
### REST API ###
################

    def is_ip_authorized(self):
        try:
            self.__api_get_wallet_balance()
        except UnauthorizedError:
            return False
        return True

    def has_read_permission(self):
        permission = False
        api_keys = self.__api_get_api_profile()
        for api_key in api_keys:
            if api_key["api_key"] == self.api_key:
                permission = api_key["account_permission"]
        return permission

    def has_write_permission(self):
        permission = False
        api_keys = self.__api_get_api_profile()
        for api_key in api_keys:
            if api_key["api_key"] == self.api_key:
                permission = api_key["trade_permission"]
        return permission

    def has_withdraw_permission(self):
        # Always returns false since API withdrawal is not implemented on delta-exchange
        return False

    def has_future_authorized(self):
        return True

    def get_fees(self):
        return {
            "taker": 0.0005,
            "maker": 0.0005,
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
        response = self.__api_get_wallet_balance()

        for asset_balances in response:
            if asset_balances["asset_symbol"] in self.__ignored_assets:
                continue
            if float(asset_balances["balance"]) > 0:
                symbol = str(asset_balances['asset_symbol'])

                if as_base_currency:
                    usd_price = self.__get_symbol_usd_price(symbol)
                    conversion_factor = usd_price*rate
                else:
                    conversion_factor = 1

                data[symbol] = {
                    "symbol" : symbol,
                    "balance" : float(asset_balances["balance"])*conversion_factor,
                    "available_balance" : float(asset_balances["available_balance"])*conversion_factor,
                    "currency" : self.base_currency if as_base_currency else symbol,
                }
        return data

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
                print(f"### Skip transaction by feeder id")
                continue
            if transaction['operation_id'] in internal_deposit_transaction_ids:
                print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['operation_id'] in internal_withdraw_transaction_ids:
                print(f"### Skip transaction by internal withdraw id")
                continue

            if transaction["type"] == "deposit":
                total_deposit += transaction["base_currency_amount"]
            else:
                total_withdraw += transaction["base_currency_amount"]

        """
        if self.strategy and self.database:
            # Remove fees cashflows from withdrawals
            fees_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'fees'")
            withdraw_fees = 0
            for index, fees_cashflow in fees_cashflows.iterrows():
                rate = CbForex(self.database).get_rate(fees_cashflow.currency, self.base_currency, datetime.datetime.strptime(str(fees_cashflow.timestamp), "%Y-%m-%d %H:%M:%S"))
                withdraw_fees += fees_cashflow.amount*rate
            total_withdraw -= withdraw_fees

            # Substract feeder cashflows from deposits and add to withdrawals
            feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
            feeder_cashflows_amount = 0
            for index, cashflow in feeder_cashflows.iterrows():
                rate = CbForex(self.database).get_rate(cashflow.currency, self.base_currency, datetime.datetime.strptime(str(cashflow.timestamp), "%Y-%m-%d %H:%M:%S"))
                feeder_cashflows_amount += cashflow.amount*rate
            total_deposit -= feeder_cashflows_amount
            total_withdraw += feeder_cashflows_amount
        """

        # Compute positions
        total_premium = 0
        total_unrealized = 0
        positions = self.get_positions()
        for symbol, position in positions.items():
            unrealized_pl = 0

            # Ignore spot positions
            if position["contract_type"] == "spot":
                continue

            contract_size = self.get_contract_size(position['base_asset_symbol'])
            mark_price = self.get_mark_price(position["symbol"])['mark_price']*contract_size

            if position['size'] < 0:
                unrealized_pl = - (float(position['entry_price'])*contract_size - float(mark_price))*float(position['size'])
            else:
                unrealized_pl = (float(mark_price) - float(position['entry_price'])*contract_size)*float(position['size'])

            if position['contract_type'] != 'perpetual_future':
                #we remove the premium from the options
                if position['size'] < 0:
                    total_premium += float(position['entry_price'])*float(position['size'])*contract_size
                else:
                    total_premium += float(position['entry_price'])*float(position['size'])*contract_size

            total_unrealized += unrealized_pl

        """
        # Compute trading credits
        operations = self.get_transaction_history()
        total_trading_credits = abs(sum(operation["amount"] for operation in operations if operation["transaction_type"] == "trading_credits"))
        total_trading_credits_paid = abs(sum(operation["amount"] for operation in operations if operation["transaction_type"] == "trading_credits_paid"))
        total_trading_credits_reverted = abs(sum(operation["amount"] for operation in operations if operation["transaction_type"] == "trading_credits_reverted"))
        total_trading_credits = total_trading_credits - total_trading_credits_paid - total_trading_credits_reverted

        # Compute fees
        sum_funding = sum(operation["amount"] for operation in operations if operation["transaction_type"] == "funding")
        total_liquidation_fees = abs(sum(operation["amount"] for operation in operations if operation["transaction_type"] == "liquidation_fee"))
        total_trading_fees = abs(sum(operation["amount"] for operation in operations if operation["transaction_type"] == "trading_fee_credits"))
        total_fees = total_liquidation_fees + total_trading_fees
        """

        total_balance = total_balance + total_unrealized + total_premium
        #total_profit = total_balance - total_deposit + total_withdraw - total_trading_credits
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
            data = {}
            assets = self.__api_get_assets()
            for asset in assets:
                asset_refactored = {
                    "id": asset["id"],
                    "symbol": asset["symbol"],
                }
                data[asset["symbol"]] = asset_refactored
                self.__assets[asset["symbol"]] = asset_refactored
            return data
        else:
            return self.__assets

    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}
            products = self.__api_get_products_redacted()
            for product in products:

                generated_symbol = self.__compose_product_symbol(product)

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["id"],
                    "exchange_symbol": product["symbol"],
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
                            "min": float(product["tick_size"]),
                            "max": None},
                        "price": {
                            "min": None,
                            "max": None},
                        "notional": {
                            "min": None,
                            "max": None},
                    },
                }

                strike_price = product["strike_price"]
                settlement_time = product["settlement_time"]
                settlement_date = settlement_time[:10] if settlement_time else None
                duration = (datetime.datetime.strptime(settlement_date, "%Y-%m-%d").date() - datetime.date.today()).days if settlement_date else None

                if not (contract_type := self.__compose_contract_type(product)):
                    # Do not handle this product types
                    continue
                product_refactored["contract_type"] = contract_type
                product_refactored["contract_size"] = float(product["contract_value"])
                product_refactored["strike_price"] = float(strike_price) if strike_price else None
                product_refactored["settlement_date"] = settlement_date
                product_refactored["settlement_time"] = settlement_time
                product_refactored["duration"] = duration

                if "underlying_asset" not in product:
                    if not self.__assets:
                        self.get_assets()
                    product_refactored["base_asset_id"] = product["underlying_asset_id"]
                    product_refactored["base_asset_symbol"] = [asset["symbol"] for asset in self.__assets.values() if asset["id"] == product["underlying_asset_id"]][0]

                else:
                    product_refactored["base_asset_id"] = product["underlying_asset"]["id"]
                    product_refactored["base_asset_symbol"] = product["underlying_asset"]["symbol"]

                if "quoting_asset" not in product:
                    if not self.__assets:
                        self.get_assets()
                    product_refactored["quote_asset_id"] = product["quoting_asset_id"]
                    product_refactored["quote_asset_symbol"] = [asset["symbol"] for asset in self.__assets.values() if asset["id"] == product["quoting_asset_id"]][0]
                else:
                    product_refactored["quote_asset_id"] = product["quoting_asset"]["id"]
                    product_refactored["quote_asset_symbol"] = product["quoting_asset"]["symbol"]

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
            response = self.__api_get_orderbook(exchange_id)

            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "buy": [],
                "sell": [],
            }

            for order in response["buy"]:
                order_refactored = {
                    "price": float(order["price"]),
                    "size": order["size"],
                }
                data["buy"].append(order_refactored)
            for order in response["sell"]:
                order_refactored = {
                    "price": float(order["price"]),
                    "size": order["size"],
                }
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

        response = self.__api_get_ticker(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "spot_price": float(response["spot_price"]),
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

            # Get spot price if product is spot
            if self.__products[symbol]["contract_type"] == "spot":
                mark_price = self.get_spot_price(symbol)["spot_price"]
            # Otherwise get mark price
            else:
                # Try using websocket pricing server
                response = None
                if pricing_client:
                    if response := pricing_client.get_mark_price(self.PLATFORM_ID, symbol):
                        mark_price = float(response["mark_price"])
                # Otherwise make API call
                if response == None:
                    response = self.__api_get_ticker(exchange_symbol)
                    mark_price = float(response["mark_price"])

            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "mark_price": mark_price,
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

        if float(response["greeks"]['delta']) == 0:
            log(f"[{self}] [WARNING] Empty delta retrieved on {symbol} : {response}")
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

    def get_open_interest(self, symbol):
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
            "market_volatility": float(response["mark_vol"]),
            "open_interest": float(response["oi"]) if 'oi' in response else 0,
            "open_interest_value": float(response["oi_value"]) if 'oi_value' in response else 0,
            "open_interest_symbol": response["oi_value_symbol"] if 'oi_value_symbol' in response else None,
            "open_interest_value_usd": float(response["oi_value_usd"]) if 'oi_value_usd' in  response else 0,
        }

        return data

    def get_positions(self):
        if not self.__products:
            self.get_products()

        rate = CbForex(self.database).get_rate("USD", self.base_currency)

        data = {}
        positions = self.__api_get_positions()
        for position in positions:
            symbol = self.__compose_product_symbol(position["product"])

            mark_price = self.get_mark_price(symbol)["mark_price"]
            quote_asset_usd_price = self.__get_symbol_usd_price(position["product"]["quoting_asset"]["symbol"])
            usd_price = mark_price*quote_asset_usd_price

            data[symbol] = {
                "symbol": symbol,
                "exchange_id": position["product_id"],
                "exchange_symbol": position["product_symbol"],
                "size": float(position["size"]),
                "entry_price": float(position["entry_price"]),
                "maintenance_margin": 0,
                "contract_type": self.__compose_contract_type(position["product"]),
                "base_asset_id": position["product"]["underlying_asset"]["id"],
                "base_asset_symbol": position["product"]["underlying_asset"]["symbol"],
                "quote_asset_id": position["product"]["quoting_asset"]["id"],
                "quote_asset_symbol": position["product"]["quoting_asset"]["symbol"],
                "base_currency_amount": position["size"]*usd_price*rate,
                "base_currency_symbol": self.base_currency,
            }

        balances = self.__api_get_wallet_balance()
        for balance in balances:
            if float(balance["available_balance"]) == 0:
                continue
            # Skip some symbols
            if balance['asset_symbol'] in self.__ignored_assets:
                continue

            symbol = balance["asset_symbol"]
            usd_price = self.__get_symbol_usd_price(symbol)
            data[symbol] = {
                "symbol": symbol,
                "exchange_id": balance["asset_id"],
                "exchange_symbol": symbol,
                "size": float(balance["available_balance"]),
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": balance["asset_id"],
                "base_asset_symbol": symbol,
                "quote_asset_id": balance["asset_id"],
                "quote_asset_symbol": symbol,
                "base_currency_amount": float(balance["available_balance"])*usd_price*rate,
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

        response = self.__api_get_position(exchange_id)
        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "size": response["size"],
            "entry_price": response["entry_price"],
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

        response = self.__api_get_position_margin(exchange_id)
        data = response
        if len(data) > 0 :
            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "auto_topup": response[0]["auto_topup"],
                "margin": float(response[0]["margin"]),
                "entry_price": float(response[0]["entry_price"]),
                "liquidation_price": float(response[0]["liquidation_price"]) if response[0]["liquidation_price"] is not None else None
            }
        return data

    def set_position_margin(self, symbol, delta_margin):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        leverage_limit_exceeded = False
        leverage_limit_exceeded_tries = 0
        leverage_limit_exceeded_retries = 3
        initial_delta_margin = delta_margin

        response = self.__api_set_position_margin(exchange_id, delta_margin)

        if "error" in response and response['error']['code'] == 'leverage_limit_exceeded':
            leverage_limit_exceeded = True

        #DEAL WITH POSITIONS WINNING A LOT
        while leverage_limit_exceeded and leverage_limit_exceeded_tries < leverage_limit_exceeded_retries:
            minimum_margin = response['error']['context']['minimum_margin_required']
            delta_margin += float(minimum_margin)
            position_margin = self.__api_get_position_margin(exchange_id)
            position_margin = position_margin[0]['margin']
            delta_margin = max(float(minimum_margin) - float(position_margin), delta_margin - float(minimum_margin))*0.98
            print(f"[ERROR] Margin leverage limit exceeded (try {leverage_limit_exceeded_tries}/{leverage_limit_exceeded_retries}). Corrective delta '{delta_margin}' instead of '{initial_delta_margin}'")
            response = self.__api_set_position_margin(exchange_id, delta_margin)

            if "error" in response and response['error']['code'] == 'leverage_limit_exceeded':
                leverage_limit_exceeded = True
            else:
                leverage_limit_exceeded = False

            leverage_limit_exceeded_tries += 1

        if leverage_limit_exceeded:
            response = {
                "auto_topup": None,
                "margin": 0,
                "entry_price": 0,
            }

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "auto_topup": response["auto_topup"],
            "margin": response["margin"],
            "entry_price": response["entry_price"],
        }
        return data

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="market_order", time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": "market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if "average_fill_price" in response and response["average_fill_price"] is not None else 0,
            "limit_price": None,
            "stop_price": None,
            "time_in_force": response["time_in_force"],
            "commission": 0,
        }

        # Sometimes market order is converted into limit_order
        if (response["order_type"] == "limit_order" and response["state"] == "open" and
            response["size"] == response["unfilled_size"]):

            # Cancel order
            self.cancel_order(symbol, response["id"])

            # Overwrite order data
            data["status"] = "cancelled"
            data["limit_price"] = response["limit_price"]

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "market_order", data["status"], response["limit_price"])

        return data

    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0, reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        limit_price = self.__round_price_down(symbol, limit_price) if side == "sell" else self.__round_price_up(symbol, limit_price)

        response = self.__api_place_order(exchange_id, round(size,0), side, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        started_at = time.time()

        data = {
            "id": str(response["id"]),
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = str(response["id"])
            order_placed = False
            elapsed_time = time.time() - started_at
            try:
                while elapsed_time < timeout:
                    active_orders = self.get_active_orders(symbol)

                    # Order placed
                    if not any(order["id"] == order_id for order in active_orders):
                        order_placed = True
                        break

                    elapsed_time = time.time() - started_at
                    time.sleep(0.1)
            except requests.exceptions.ReadTimeout:
                pass

            if not order_placed:
                data = self.cancel_order(symbol, order_id)
            else:
                order = self.__api_get_order_state(data["id"])

                data["status"] = self.__compose_order_status(order)
                data["symbol"] = symbol
                data["exchange_id"] = order["product_id"]
                data["exchange_symbol"] = order["product_symbol"]
                data["side"] = order["side"]
                data["size"] = float(order["size"])
                data["filled_size"] = float(order["size"]) - float(order["unfilled_size"])
                data["unfilled_size"] = float(order["unfilled_size"])
                data["average_price"] = float(order["average_fill_price"]) if order["average_fill_price"] else 0
                data["limit_price"] = float(order["limit_price"])
                data["stop_price"] = None
                data["time_in_force"] = order["time_in_force"]
                data["commission"] = 0

                # Overwrite status if IOC order and partially filled
                if data["status"] == "cancelled" and data["unfilled_size"] > 0 and data["filled_size"] > 0:
                    data["status"] = "partially_filled"

        # Doesn't wait for order to be placed
        else:

            data["status"] = self.__compose_order_status(response)
            data["symbol"] = symbol
            data["exchange_id"] = response["product_id"]
            data["exchange_symbol"] = response["product_symbol"]
            data["side"] = response["side"]
            data["size"] = float(response["size"])
            data["filled_size"] = float(response["size"]) - float(response["unfilled_size"])
            data["unfilled_size"] = float(response["unfilled_size"])
            data["average_price"] = float(response["average_fill_price"]) if response["average_fill_price"] else 0
            data["limit_price"] = float(response["limit_price"])
            data["stop_price"] = None
            data["time_in_force"] = response["time_in_force"]
            data["commission"] = 0

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], float(response["limit_price"]))

        return data

    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        stop_price = self.__round_price_down(symbol, stop_price) if side == "sell" else self.__round_price_up(symbol, stop_price)

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else 0,
            "limit_price": None,
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
            "commission": 0,
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_market_order", data["status"], float(response["average_fill_price"]) if response["average_fill_price"] else 0)

        return data

    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc", reduce_only=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        limit_price = self.__round_price_down(symbol, limit_price) if side == "sell" else self.__round_price_up(symbol, limit_price)
        stop_price = self.__round_price_down(symbol, stop_price) if side == "sell" else self.__round_price_up(symbol, stop_price)

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": "stop_limit",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else 0,
            "limit_price": float(response["limit_price"]),
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
            "commission": 0,
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_limit_order", data["status"], float(response["average_fill_price"]) if response["average_fill_price"] else 0)

        return data

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.0075):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

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

        # Compute prices from orderbook with coefficients
        if side == "sell":
            limit_price = orderbook["buy"][0]["price"]*(1+limit_price_coef)
            stop_price = orderbook["buy"][0]["price"]*(1-stop_price_coef)
        else:
            limit_price = orderbook["sell"][0]["price"]*(1-limit_price_coef)
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
            log(f"[{self}] {response}")
            return response

        # Check for immediate order filling
        active_orders = self.get_active_orders(symbol)
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
                    if order["id"] == limit_order_id and order["status"] == "filled":
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
                    elif order["id"] == stop_market_order_id and order["status"] == "filled":
                        stop_order_placed = True

                        log(f"[{self}] Cancel limit order '{limit_order_id}'")
                        # Cancel limit order
                        try:
                            self.cancel_order(symbol, limit_order_id)
                            log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                        except Exception as e:
                            log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")
                        break

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
                log(f"[{self}] Place {side} market order of {size} on {symbol}")
                order = self.place_market_order(symbol, side=side, size=size)
                log(f"[{self}] {order}")
            elif not limit_order_active:
                log(f"[{self}] Limit order placed")
                order = self.get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["average_price"])
            elif not stop_market_order_active:
                log(f"[{self}] Stop market order placed")
                order = self.get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["average_price"])
        else:
            log(f"[{self}] An order has been executed : {order}")

            # Complete order if partially filled
            if order["filled_size"] < size:
                log(f"[{self}] Complete partially filled order with {size-order['filled_size']} {symbol}")
                response = self.place_market_order(symbol, side=side, size=size-order['filled_size'])
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

        return order

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
        response = self.__api_get_active_orders()
        for order in response:

            if symbol and exchange_id != order["product_id"]:
                continue
            else:
                active_order = {
                    "id": str(order["id"]),
                    "type": self.__compose_order_type(order),
                    "status": self.__compose_order_status(order),
                    "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == order["product_id"]), None),
                    "exchange_id": order["product_id"],
                    "exchange_symbol": order["product_symbol"],
                    "side": order["side"],
                    "size": float(order["size"]),
                    "filled_size": float(order["size"]) - float(order["unfilled_size"]),
                    "unfilled_size": float(order["unfilled_size"]),
                    "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else 0,
                    "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                    "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                    "time_in_force": order["time_in_force"],
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
        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else 0,
            "limit_price": float(response["limit_price"]) if response["limit_price"] else None,
            "stop_price": float(response["stop_price"]) if response["stop_price"] else None,
            "time_in_force": response["time_in_force"],
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

        response = self.__api_cancel_order(order_id, exchange_id)

        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": "cancelled" if response["size"] == response["unfilled_size"] else "partially_filled",
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else 0,
            "limit_price": float(response["limit_price"]) if response["limit_price"] else None,
            "stop_price": float(response["stop_price"]) if response["stop_price"] else None,
            "time_in_force": response["time_in_force"],
        }

        return data

    def edit_order(self, order_id, symbol, limit_price=None, size=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_edit_order(order_id, exchange_id, limit_price, size)

        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["size"]) - float(response["unfilled_size"]),
            "unfilled_size": float(response["unfilled_size"]),
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else 0,
            "limit_price": float(response["limit_price"]) if response["limit_price"] else None,
            "stop_price": float(response["stop_price"]) if response["stop_price"] else None,
            "time_in_force": response["time_in_force"],
        }

        return data

    def get_order_history(self, symbol=None):
        if not self.__products:
            self.get_products()

        response = self.__api_get_order_history()

        data = []
        for order in response:

            uniformized_symbol = next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == order["product_id"]), None)

            if symbol != None and uniformized_symbol != symbol:
                continue

            order_refactored = {
                "created_at": order["created_at"],
                "updated_at": order["updated_at"],
                "id": str(order["id"]),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": uniformized_symbol,
                "exchange_id": order["product_id"],
                "exchange_symbol": order["product_symbol"],
                "side": order["side"],
                "size": float(order["size"]),
                "filled_size": float(order["size"]) - float(order["unfilled_size"]),
                "unfilled_size": float(order["unfilled_size"]),
                "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else 0,
                "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                "time_in_force": order["time_in_force"],
                "source": order["meta_data"]["source"] if "source" in order["meta_data"] else None,
                "is_liquidation": True if self.__compose_order_type(order) == "liquidation" else False,
                "cancellation_reason": order["cancellation_reason"],
            }
            data.append(order_refactored)
        return data

    def get_leverage(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_order_leverage(exchange_id)
        data = {
            "symbol": symbol,
            "leverage": int(response['leverage'])
        }

        return data

    def set_leverage(self, symbol, leverage):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        try:
            response = self.__api_set_order_leverage(exchange_id, leverage)
        except InvalidLeverage as e:
            response = self.__api_set_order_leverage(exchange_id, e.max_leverage)

        data = {
            "symbol": symbol,
            "leverage": int(response['leverage'])
        }

        return data

    def change_order_leverage(self, product_id):
        if not self.__products:
            self.get_products()
        if product_id not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_id}'")
        else:
            exchange_id = self.__products[product_id]["exchange_id"]
            exchange_symbol = self.__products[product_id]["exchange_symbol"]
        return self.api_change_order_leverage(exchange_id)

    def get_trade_history(self, symbol, start_date, end_date):
        data = []
        orders = self.get_order_history(symbol)
        for order in orders:
            date = order["update_at"]
            if date < start_date or date > end_date or order["status"] != "filled":
                continue

            data.append({
                "id": order["id"],
                "symbol": order["symbol"],
                "exchange_id": order["product_id"],
                "exchange_symbol": order["product_symbol"],
                "side" : order["side"],
                "size": order["size"],
                "price": order["average_price"],
                "datetime": date,
            })
        return data

    def get_transaction_history_old(self):
        data = []
        transactions = self.__api_download_wallet_transactions()
        for transaction in transactions:
            data.append({
                "transaction_id": transaction["id"],
                "transaction_type": transaction["Transaction type"],
                "symbol": transaction["Asset Symbol"],
                "amount": transaction["Amount"],
                "date": datetime.datetime.strptime(transaction["Date"][:-8], "%Y-%m-%d %H:%M:%S"),
            })
        return data

    def get_transaction_history(self):
        data = []

        start_date = datetime.datetime.combine(datetime.date(2022, 1, 1), datetime.datetime.min.time())
        end_date = datetime.datetime.combine(datetime.date.today(), datetime.datetime.max.time())

        index = 0
        stop = end_date
        start = end_date
        timedelta = datetime.timedelta(days=30)
        while start > start_date:

            stop = end_date - index*timedelta
            if stop - start_date > timedelta:
                start = end_date - (index+1)*timedelta
            else:
                start = start_date

            response = self.__api_get_wallet_transactions(start, stop)
            for operation in response:
                date = datetime.datetime.strptime(operation["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
                transaction_id = operation["meta_data"]["transaction_id"] if "meta_data" in operation and "transaction_id" in operation["meta_data"] else operation["uuid"]
                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(operation["asset_symbol"], date)
                if usd_amount is None:
                    usd_amount = 0
                base_currency_amount = abs(float(operation["amount"])*rate*usd_amount)

                data.append({
                    "date": date,
                    "type": type,
                    "source_symbol": operation["asset_symbol"],
                    "source_amount": abs(float(operation["amount"])),
                    "destination_symbol": operation["asset_symbol"],
                    "destination_amount": abs(float(operation["amount"])),
                    "fees": 0,
                    "operation_id": transaction_id,
                    "wallet_address": None,
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                })

            index += 1
        return data

    def get_deposit_withdraw_history_old(self, start_date=None, end_date=None):
        data = []

        if not start_date:
            start_date = datetime.date(2022, 1, 1)
        if not end_date:
            end_date = datetime.date.today()

        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        history = self.get_transaction_history()
        history = pd.DataFrame(history)

        operations = history[(history["transaction_type"] == "deposit") | (history["transaction_type"] == "withdrawal") | (history["transaction_type"] == "sub_account_transfer")]

        for index, operation in operations.iterrows():
            #date = datetime.datetime.fromtimestamp(operation["date"]/1000000000)
            date = operation["date"]
            if date < start_date or date > end_date:
                continue

            if operation["transaction_type"] == "withdrawal":
                type = "withdraw"
            elif operation["transaction_type"] == "deposit":
                type = "deposit"
            else:
                if operation["amount"] > 0:
                    type = "deposit"
                else:
                    type = "withdraw"

            transaction_id = operation["transaction_id"]
            # Filter-out fees and feeder cashflows
            if self.strategy and self.database:
                transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                if not transaction_cashflow.empty:
                    cashflow = transaction_cashflow.iloc[0]
                    if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                        continue

            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            usd_amount = self.__get_symbol_usd_price(operation["symbol"], date)
            base_currency_amount = abs(float(operation["amount"])*rate*usd_amount)

            data.append({
                "date": date,
                "type": type,
                "source_symbol": operation["symbol"],
                "source_amount": abs(operation["amount"]),
                "destination_symbol": operation["symbol"],
                "destination_amount": abs(operation["amount"]),
                "fees": 0,
                "operation_id": transaction_id,
                "wallet_address": None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
            })
        return data

    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        data = []

        if not start_date:
            start_date = datetime.date(2022, 1, 1)
        if not end_date:
            end_date = datetime.date.today()

        if isinstance(start_date, datetime.date):
            start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        if isinstance(end_date, datetime.date):
            end_date = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        index = 0
        stop = end_date
        start = end_date
        timedelta = datetime.timedelta(days=30)
        while start > start_date:

            stop = end_date - index*timedelta
            if stop - start_date > timedelta:
                start = end_date - (index+1)*timedelta
            else:
                start = start_date

            response = self.__api_get_wallet_transactions(start, stop, "deposit,withdrawal,sub_account_transfer")

            for operation in response:
                if operation["transaction_type"] == "withdrawal":
                    type = "withdraw"
                elif operation["transaction_type"] == "deposit":
                    type = "deposit"
                else:
                    if float(operation["amount"]) > 0:
                        type = "deposit"
                    else:
                        type = "withdraw"

                transaction_id = operation["meta_data"]["transaction_id"] if "meta_data" in operation and "transaction_id" in operation["meta_data"] else operation["uuid"]
                # Filter-out fees and feeder cashflows
                if self.strategy and self.database:
                    transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                    if not transaction_cashflow.empty:
                        cashflow = transaction_cashflow.iloc[0]
                        if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                            continue

                date = datetime.datetime.strptime(operation["created_at"][:19], "%Y-%m-%dT%H:%M:%S")

                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(operation["asset_symbol"], date)
                base_currency_amount = abs(float(operation["amount"])*rate*usd_amount)

                data.append({
                    "date": date,
                    "type": type,
                    "source_symbol": operation["asset_symbol"],
                    "source_amount": abs(float(operation["amount"])),
                    "destination_symbol": operation["asset_symbol"],
                    "destination_amount": abs(float(operation["amount"])),
                    "fees": 0,
                    "operation_id": transaction_id,
                    "wallet_address": None,
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                })

            index += 1
        return data

    def get_candle(self, symbol, start_date, end_date):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if start_date >= end_date:
            start_date, end_date = end_date, start_date

        timespan = end_date - start_date

        if timespan <= datetime.timedelta(hours=1):
            resolution = "1m"
        elif timespan <= datetime.timedelta(days=1):
            resolution = "1h"
        elif timespan <= datetime.timedelta(weeks=1):
            resolution = "1d"
        else:
            resolution = "1w"

        start_timestamp = round(datetime.datetime.timestamp(start_date))
        stop_timestamp = round(datetime.datetime.timestamp(end_date))

        response = self.__api_candle_history(exchange_symbol, start_timestamp, stop_timestamp, resolution)

        candle_number = len(response)
        if candle_number == 0:
            open = 0
            close = 0
            low = 0
            high = 0
            volume = 0
        else:
            open = response[0]["open"]
            close = response[candle_number-1]["close"]
            low = response[0]["low"] if response[0]["low"] else min(open, close)
            high = response[0]["high"] if response[0]["low"] else max(open, close)
            volume = 0

            for candle in response:
                low = candle["low"] if candle["low"] and candle["low"] < low else low
                high = candle["high"] if candle["high"] and candle["high"] > high else high
                volume += candle["volume"] if candle["volume"] is not None else 0

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
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if start_date >= end_date:
            start_date, end_date = end_date, start_date

        start_timestamp = round(datetime.datetime.timestamp(start_date))
        stop_timestamp = round(datetime.datetime.timestamp(end_date))

        response = self.__api_candle_history(exchange_symbol, start_timestamp, stop_timestamp, resolution)

        data = []
        for candle in response:
            data.append({
                "symbol": symbol,
                "open": candle["open"],
                "close": candle["close"],
                "low": candle["low"],
                "high": candle["high"],
                "volume": candle["volume"],
            })

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

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        #self.__subscribe_dispatcher("l2_orderbook", exchange_symbols, auth=False, callback=callback)
        self.__subscribe_dispatcher("l2_updates", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        #self.__unsubscribe_dispatcher("l2_orderbook", exchange_symbols, callback)
        self.__unsubscribe_dispatcher("l2_updates", exchange_symbols, callback=callback)

        # Cleanup associated data
        for symbol in symbols:
            if symbol in self.__orderbooks:
                del self.__orderbooks[symbol]

    def subscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(f"MARK:{self.__products[symbol]['exchange_symbol']}")

        self.__subscribe_dispatcher("mark_price", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(f"MARK:{self.__products[symbol]['exchange_symbol']}")

        self.__unsubscribe_dispatcher("mark_price", exchange_symbols, callback)

    def subscribe_funding_rate(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__subscribe_dispatcher("funding_rate", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_funding_rate(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__unsubscribe_dispatcher("funding_rate", exchange_symbols, callback)

    def subscribe_volume(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__subscribe_dispatcher("v2/ticker", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_volume(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__unsubscribe_dispatcher("v2/ticker", exchange_symbols, callback)

#########################
### Privates channels ###
#########################

    def subscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__subscribe_dispatcher("orders", exchange_symbols, auth=True, callback=callback)

    def unsubscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        exchange_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbols.append(self.__products[symbol]["exchange_symbol"])

        self.__unsubscribe_dispatcher("orders", exchange_symbols, callback=callback)

    def subscribe_balances(self, callback=None):
        if not self.__products:
            self.get_products()

        self.__subscribe_dispatcher("margins", auth=True, callback=callback)

    def unsubscribe_balances(self, callback=None):
        if not self.__products:
            self.get_products()

        self.__unsubscribe_dispatcher("margins", callback=callback)

    def subscribe_positions(self, callback=None):
        if not self.__products:
            self.get_products()

        self.__subscribe_dispatcher("positions", symbols=["all"], auth=True, callback=callback)

    def unsubscribe_positions(self, callback=None):
        if not self.__products:
            self.get_products()

        self.__unsubscribe_dispatcher("positions", symbols=["all"], callback=callback)

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_api_profile(self):
        response = self.__request("GET", "/v2/profile/api_keys", auth=True)
        return response

    def __api_get_wallet_balance(self):
        response = self.__request("GET", "/v2/wallet/balances", auth=True)
        return response

    def __api_get_assets(self):
        response = self.__request("GET", "/v2/assets", auth=False)
        return response

    def __api_get_products(self):
        response = self.__request("GET", "/v2/products", auth=False)
        return response

    def __api_get_products_redacted(self):
        response = self.__request("GET", "/v2/products/redacted", auth=False)
        return response

    def __api_get_orderbook(self, symbol):
        response = self.__request("GET", "/v2/l2orderbook/{}".format(symbol))
        return response

    def __api_get_ticker(self, symbol):
        response = self.__request("GET", "/v2/tickers/{}".format(symbol), auth=False)
        return response

    def __api_get_order_history(self):
        response = self.__request("GET", "/v2/orders/history", query={
            "page_size": str('10000')
        }, auth=True)
        return response

    def __api_get_wallet_transactions(self, start_date=None, end_date=None, transaction_types=None):
        data = []
        after = None
        query = {
            "page_size": 1000,
        }
        if start_date:
            query["start_time"] = int(start_date.timestamp()*1000000)
        if end_date:
            query["end_time"] = int(end_date.timestamp()*1000000)
        if transaction_types:
            query["transaction_types"] = transaction_types
        while True:
            if after:
                query["after"] = after
            response = self.__request("GET", "/v2/wallet/transactions", query=query, auth=True, paginated=True)
            data.extend(response["result"])
            after = response["meta"]["after"]
            if not after:
                break
        return data

    def __api_download_wallet_transactions(self):
        response = self.__request("GET", "/v2/wallet/transactions/download", auth=True, parse="csv")
        return response

    def __api_get_positions(self):
        response = self.__request("GET", "/v2/positions/margined", auth=True)
        return response

    def __api_get_position(self, product_id):
        response = self.__request("GET", "/v2/positions", query={
            "product_id": str(product_id)
        }, auth=True)
        return response

    def __api_get_position_margin(self, product_id):
        response = self.__request("GET", "/v2/positions/margined", query={
                        'product_ids': str(product_id),
                   }, auth=True)
        return response

    def __api_set_position_margin(self, product_id, delta_margin):
        response = self.__request("POST", "/v2/positions/change_margin", payload={
                        'product_id': str(product_id),
                        'delta_margin': str(delta_margin),
                   }, auth=True)
        return response

    def __api_place_order(self,
        product_id,
        size,
        side,
        order_type,
        limit_price=None,
        stop_price=None,
        time_in_force="gtc",
        stop_trigger_method='spot_price',
        reduce_only=False):

        if str(product_id) == "245":
            size = size * 10

        params = {
            "product_id": str(product_id),
            "size": str(size),
            "side": str(side),
            "time_in_force": str(time_in_force),
        }

        if order_type == "market_order":
            params["order_type"] = "market_order"
        elif order_type == "limit_order":
            params["order_type"] = "limit_order"
            params["limit_price"] = str(limit_price)
        elif order_type == "stop_market_order":
            params["order_type"] = "market_order"
            params["stop_price"] = str(stop_price)
            params["stop_order_type"] = "stop_loss_order"
            params["stop_trigger_method"] = stop_trigger_method
        elif order_type == "stop_limit_order":
            params["order_type"] = "limit_order"
            params["limit_price"] = str(limit_price)
            params["stop_price"] = str(stop_price)
            params["stop_order_type"] = "stop_loss_order"
            params["stop_trigger_method"] = stop_trigger_method

        if reduce_only:
            params["reduce_only"] = "true"

        response = self.__request("POST", "/v2/orders", params, auth=True)

        return response

    def __api_get_active_orders(self):
        response = self.__request("GET", "/v2/orders", auth=True)
        return response

    def __api_get_order_state(self, order_id):
        response = self.__request("GET", f"/v2/orders/{order_id}", auth=True)
        return response

    def __api_cancel_order(self, order_id, product_id):
        response = self.__request("DELETE", "/v2/orders", {
           "id": str(order_id),
           "product_id": str(product_id),
        }, auth=True)
        return response

    def __api_edit_order(self, order_id, product_id, limit_price=None, size=None):

        payload = {
            "id": str(order_id),
           "product_id": str(product_id),
        }

        if limit_price:
            payload["limit_price"] = str(limit_price)
        if size:
            payload["size"] = str(size)

        response = self.__request("PUT", "/v2/orders", payload, auth=True)
        return response

    def __api_get_order_leverage(self, product_id):
        response = self.__request("GET", f"/v2/products/{product_id}/orders/leverage", auth=True)
        return response

    def __api_set_order_leverage(self, product_id, leverage):
        response = self.__request("POST", f"/v2/products/{product_id}/orders/leverage", {
            "leverage": str(leverage)
        }, auth=True)
        return response

    def api_change_order_leverage(self, product_id):
        response = self.__request("POST", f"/products/{product_id}/orders/leverage", {
           "product_id": str(product_id),
        }, auth=True)
        return response

    def __api_candle_history(self, symbol, start_date, end_date, resolution):
        response = self.__request("GET", "/v2/history/candles", query = {
           "symbol": str(symbol),
           "start": str(start_date),
           "end": str(end_date),
           "resolution": str(resolution),
        })
        return response

    def __get_symbol_usd_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        price = None
        convs = ["USDT", "USD", "USDC", "BUSD"]

        if is_fiat(symbol):
            price = CbForex(self.database).get_rate(symbol, "USD", date)
        elif symbol in convs:
            price = 1
        elif not date:
            rate = None
            for conv in convs:
                if f"{symbol}_{conv}" in self.__products:
                    rate = self.get_mark_price(f"{symbol}_{conv}")
                    if rate:
                        price = rate["mark_price"]
                        break
        else:
            candle = None
            for conv in convs:
                if f"{symbol}_{conv}" in self.__products:
                    candle = self.get_candle(f"{symbol}_{conv}", date, date-datetime.timedelta(minutes=1))
                    if candle:
                        price = candle["close"]
                        break

        return price

    def __request(self, method, path, payload=None, query=None, auth=False, base_url=None, headers=None, retry=3, parse="json", paginated=False):
        if base_url is None:
            base_url = self.base_url
        if not headers:
            headers = {"Content-Type": "application/json"}

        url = '%s%s' % (base_url, path)

        query_string = self.__query_string(query)
        body_string = self.__body_string(payload)

        if auth:
            timestamp = self.__get_timestamp()
            signature_data = method + timestamp + path + query_string + body_string
            signature = self.__generate_signature(signature_data)
            headers['api-key'] = self.api_key
            headers['timestamp']  = timestamp
            headers['signature'] = signature

            signature_timestamp = timestamp

        while retry > 0:
            try:
                bef_request_timestamp = self.__get_timestamp()
                response = self.session.request(method, url, data=body_string, params=query, timeout=(3, 60), headers=headers)
                self.__request_log(response)
                af_request_timestamp = self.__get_timestamp()
                parsed_response = self.__parse_response(response, parse, paginated)
                break
            except OrderNotFoundError as e:
                # Set exception's order id
                e.order_id = payload["id"] if "id" in payload else None
                raise e
            except InternalServerError:
                # If placing order
                if method == "POST" and path == "/v2/orders":
                    log(f"[{self}] ERROR 500 on place_order")
                    # Break if order placed
                    if self.__check_order_placed(payload["product_id"], payload["side"], payload["size"], payload["order_type"]):
                        break
                retry -= 1
            #except (UnauthorizedError, requests.exceptions.ReadTimeout) as e:
            except UnauthorizedError as e:
                print("#################################################################")
                print(f"Method : {method}")
                print("#################################################################")
                print(f"URL : {url}")
                print("#################################################################")
                print(f"Data : {body_string}")
                print("#################################################################")
                print(f"Query : {query}")
                print("#################################################################")
                print(f"Headers : {headers}")
                print("#################################################################")
                #print(f"Signature timestamp : {signature_timestamp}")
                print(f"Before request timestamp : {bef_request_timestamp}")
                print(f"After request timestamp : {af_request_timestamp}")
                print("#################################################################")
                raise e

        if retry == 0:
            raise InternalServerError

        return parsed_response

    def __parse_response(self, response, parse="json", paginated=False):
        status_code = response.status_code
        if status_code != 200:
            if status_code == 429:
                raise requests.exceptions.HTTPError("{} : Too Many Requests, need to wait {} ms".format(status_code, response.headers["X-RATE-LIMIT-RESET"]))
            if status_code == 401:
                print("#################################################################")
                print(f"Response : {response}")
                print("#################################################################")
                print(f"Status code : {status_code}")
                print("#################################################################")
                print(f"Headers : {response.headers}")
                print("#################################################################")
                print("#################################################################")
                print(f"Text : {json.loads(response.text)}")
                print("#################################################################")
                raise UnauthorizedError
            else:
                try:
                    response_json = response.json()
                except:
                    raise requests.exceptions.HTTPError(status_code)

                if "error" in response_json:
                    if response_json["error"]["code"] == "invalid_api_key":
                        raise InvalidApiKeyError
                    elif response_json["error"]["code"] == "internal_server_error":
                        raise InternalServerError
                    elif response_json["error"]["code"] == "unavailable":
                        raise UnavailableProductError
                    elif response_json["error"]["code"] == "unsupported":
                        raise UnsupportedProductError
                    elif response_json["error"]["code"] == "immediate_execution_stop_order":
                        raise ImmediateExecutionStopOrderError
                    elif response_json["error"]["code"] == "open_order_not_found":
                        raise OrderNotFoundError
                    elif response_json["error"]["code"] == "insufficient_margin":
                        #symbol = response_json["error"]["context"]["asset_symbol"]
                        symbol = "USDT"
                        available_balance = response_json["error"]["context"]["available_balance"]
                        required_additional_balance = response_json["error"]["context"]["required_additional_balance"]
                        raise InsufficientMarginError(symbol, available_balance, required_additional_balance)
                    #TODO: Test this error this IP restriction
                    elif response_json["error"]["code"] == "unauthorized":
                        print("#################################################################")
                        print(f"Response : {response}")
                        print("#################################################################")
                        print(f"Status code : {status_code}")
                        print("#################################################################")
                        print(f"Headers : {response.headers}")
                        print("#################################################################")
                        print(f"Content : {response_json}")
                        print("#################################################################")
                        raise UnauthorizedError
                    # TODO: Temporary ignore this error
                    elif response_json["error"]["code"] == "leverage_limit_exceeded":
                        response_json["margin"] = 0
                        return response_json
                    elif response_json["error"]["code"] == "max_leverage_exceeded":
                        min_leverage = response_json["error"]["context"]["min_leverage"]
                        max_leverage = response_json["error"]["context"]["max_leverage"]
                        raise InvalidLeverage(min_leverage, max_leverage)
                    raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["error"]))
                else:
                    raise requests.exceptions.HTTPError(status_code)
        else:
            if parse == "json":
                response_json = response.json()
                if response_json['success']:
                    if 'result' in response_json:
                        if paginated:
                            return {
                                "meta": response_json["meta"],
                                "result": response_json["result"],
                            }
                        return response_json['result']
                elif 'error' in response_json:
                    raise requests.exceptions.HTTPError(response_json['error'])
                else:
                    raise requests.exceptions.HTTPError()
            elif parse == "csv":
                df = pd.read_csv(io.StringIO(response.content.decode('utf-8')), sep=",")
                response = df.to_dict(orient="records")
                return response

            else:
                raise Exception("Unknown parse format ''. Shall be either 'json' or 'csv'.".format(parse))

    def __get_timestamp(self):
        d = datetime.datetime.utcnow()
        epoch = datetime.datetime(1970,1,1)
        return str(int((d - epoch).total_seconds()))

    def __generate_signature(self, data):
        hash = hmac.new(self.api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    def __body_string(self, body):
        if body == None:
            return ''
        else:
            return json.dumps(body, separators=(',', ':'))

    def __query_string(self, query):
        if query == None:
            return ''
        else:
            query_strings = []
            for key, value in query.items():
                query_strings.append(key + '=' + urllib.parse.quote_plus(str(value)))
            return '?' + '&'.join(query_strings)

    def __check_order_placed(self, product_id, side, size, order_type):
        orders = self.get_order_history()

        if len(orders) == 0:
            return False

        # Find last API order
        for order in orders:
            if "source" in order["meta_data"] and order["meta_data"]["source"] == "api":
                last_order = order
                break

        if (last_order["product_id"] == product_id and
            last_order["side"] == side and
            last_order["size"] == size and
            last_order["order_type"] == order_type):
            return True
        return False

    def __compose_order_type(self, response):
        order_type = "unknown"
        if (response["stop_order_type"] == None and
           response["order_type"] == "market_order"):
            order_type = "market"
        elif (response["stop_order_type"] == None and
           response["order_type"] == "limit_order"):
            order_type = "limit"
        elif (response["stop_order_type"] == "stop_loss_order" and
           response["order_type"] == "market_order"):
            order_type = "stop_market"
        elif (response["stop_order_type"] == "stop_loss_order" and
           response["order_type"] == "limit_order"):
            order_type = "stop_limit"
        elif (response["stop_order_type"] == "liquidation_order" or
            response["order_type"] == "adl_order"):
            order_type = "liquidation"
        return order_type

    def __compose_order_status(self, response):
        order_status = "unknown"
        if (response["state"] == "open" or
            response["state"] == "pending"):
            if float(response["unfilled_size"]) < float(response["size"]):
                order_status = "partially_filled"
            else:
                order_status = "open"
        elif (response["state"] == "closed" and
            float(response["unfilled_size"]) == 0):
            order_status = "filled"
        elif (response["state"] == "closed" and
            float(response["unfilled_size"]) > 0):
            order_status = "partially_filled"
        elif response["state"] == "cancelled":
            order_status = "cancelled"
        return order_status

    def __compose_contract_type(self, product):
        contract_type = None
        if product["contract_type"] == "futures":
            contract_type = "future"
        elif product["contract_type"] == "perpetual_futures":
            contract_type = "perpetual_future"
        elif product["contract_type"] == "call_options":
            contract_type = "call_option"
        elif product["contract_type"] == "put_options":
            contract_type = "put_option"
        elif product["contract_type"] == "move_options":
            contract_type = "move_option"
        elif product["contract_type"] == "spot":
            contract_type = "spot"
        return contract_type

    def __compose_product_symbol(self, product):
        if "underlying_asset" not in product:
            if not self.__assets:
                self.get_assets()
            base_asset_symbol = [asset["symbol"] for asset in self.__assets.values() if asset["id"] == product["underlying_asset_id"]][0]
        else:
            base_asset_symbol = product["underlying_asset"]["symbol"]

        if "quoting_asset" not in product:
            if not self.__assets:
                self.get_assets()
            quote_asset_symbol = [asset["symbol"] for asset in self.__assets.values() if asset["id"] == product["quoting_asset_id"]][0]
        else:
            quote_asset_symbol = product["quoting_asset"]["symbol"]

        strike_price = product["strike_price"] if "strike_price" in product else None
        settlement_time = product["settlement_time"] if "settlement_time" in product else None
        settlement_date = settlement_time[:10] if settlement_time else None

        if product["contract_type"] == "futures":
            generated_symbol = f"{base_asset_symbol}_FUT_{settlement_date}_{quote_asset_symbol}"
        elif product["contract_type"] == "perpetual_futures":
            generated_symbol = f"{base_asset_symbol}_PERP_{quote_asset_symbol}"
        elif product["contract_type"] == "call_options":
            generated_symbol = f"{base_asset_symbol}_CALL_{strike_price}_{settlement_date}_{quote_asset_symbol}"
        elif product["contract_type"] == "put_options":
            generated_symbol = f"{base_asset_symbol}_PUT_{strike_price}_{settlement_date}_{quote_asset_symbol}"
        elif product["contract_type"] == "move_options":
            generated_symbol = f"{base_asset_symbol}_MOVE_{strike_price}_{settlement_date}_{quote_asset_symbol}"
        elif product["contract_type"] == "spot":
            generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"
        else:
            generated_symbol = None
        return generated_symbol

    def __round_price_down(self, product_symbol, price):
        # Get products if not done yet
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["price"]["min"]:
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(self.__products[product_symbol]["limits"]["price"]["min"])))
            return rounded_price
        return price

    def __round_price_up(self, product_symbol, price):
        # Get products if not done yet
        if not self.__products:
            self.get_products()
        if product_symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_symbol}'")
        elif self.__products[product_symbol]["limits"]["price"]["min"]:
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(self.__products[product_symbol]["limits"]["price"]["min"])) + Decimal(str(self.__products[product_symbol]["limits"]["price"]["min"])))
            return rounded_price
        return price

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
                    self.__receive_thread = StoppableThread(name="Thread-DeltaExchange-websocket_callback", target=self.__websocket_callback)
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
                    self.__websocket_handlers[ws]["reconnect_timer"].name = f"Thread-DeltaExchange-reconnect-{hex(id(ws))}"
                    self.__websocket_handlers[ws]["reconnect_timer"].start()

                self.__websocket_number += 1
                self.__ws_log(f"New websocket created : {ws}.", "CREATE")

        return ws

    def __delete_websocket(self, websocket):
        self.__ws_log(f"Removing websocket '{websocket}'.", "DELETE")

        # Remove websocket handler
        with self.__websocket_handlers_mutex:
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

        self.__ws_log(f"Websocket removed '{websocket}'.", "DELETE")

    def __subscribe_dispatcher(self, channel, symbols=None, auth=False, callback=None):
        self.__ws_log(f"Subscribing to '{channel}' with symbols '{symbols}'", "SUBSCRIBE")

        if symbols is None:
            symbols = []

        if len(symbols) > 0:
            # Remove symbols already registered on channel
            with self.__websocket_mutex:
                for ws in self.__websockets:
                    for symbol in list(symbols):
                        with self.__websocket_channels_mutex:
                            if channel in self.__websocket_channels[ws] and symbol in self.__websocket_channels[ws][channel]["symbols"]:
                                if callback and callback not in self.__websocket_channels[ws][channel]["callbacks"]:
                                    self.__ws_log(f"Add new callback to already registered symbol '{symbol}' on channel '{channel}'.", "SUBSCRIBE")
                                    self.__websocket_channels[ws][channel]["callbacks"].append(callback)
                                else:
                                    self.__ws_log(f"Skip already registered symbol '{symbol}' on channel '{channel}'.", "SUBSCRIBE")
                                symbols.remove(symbol)

            # Exit if no symbol to register
            if len(symbols) == 0:
                return

        with self.__subscribe_mutex:

            # No limit on channel
            if channel not in self.__CHANNELS_LIMITS:
                self.__ws_log(f"No limits on channel '{channel}'.", "SUBSCRIBE")

                # Create a websocket if not existing yet
                if self.__websocket_number == 0:
                    ws = self.__create_websocket()

                with self.__websocket_mutex:

                    # Subscribe symbols on channel on websocket
                    ws = self.__websockets[0]
                    self.__ws_log(f"Register symbols '{symbols}' on websocket '{ws}'.", "SUBSCRIBE")
                    self.__subscribe(ws, channel, symbols, auth)

                    # Add symbols to websocket channels
                    with self.__websocket_channels_mutex:

                        if ws not in self.__websocket_channels:
                            self.__websocket_channels[ws] = {
                                channel: {
                                    "auth": auth,
                                    "callbacks": [],
                                    "symbols": [],
                                }
                            }
                        elif channel not in self.__websocket_channels[ws]:
                            self.__websocket_channels[ws][channel] = {
                                "auth": auth,
                                "callbacks": [],
                                "symbols": [],
                            }

                        # Add symbols to websocket channels
                        self.__websocket_channels[ws][channel]["symbols"].extend(symbols)

                        # Add callback to websocket channels
                        if callback and callback not in self.__websocket_channels[ws][channel]["callbacks"]:
                            self.__websocket_channels[ws][channel]["callbacks"].append(callback)

            # Limits on channel
            else:
                channel_limit = self.__CHANNELS_LIMITS[channel]
                self.__ws_log(f"Limits on channel '{channel}' ({channel_limit}).", "SUBSCRIBE")

                # Register symbols on existing websockets
                with self.__websocket_mutex:
                    symbol_index = 0
                    for ws in self.__websockets:

                        # Get number of symbols registered on channel on websocket
                        with self.__websocket_channels_mutex:
                            # Channel not registered on websocket
                            if channel not in self.__websocket_channels[ws]:
                                self.__ws_log(f"Channel '{channel}' not registered on {ws}", "SUBSCRIBE")
                                self.__websocket_channels[ws][channel] = {
                                    "auth": auth,
                                    "callbacks": [],
                                    "symbols": [],
                                }
                            symbol_number_on_channel = len(self.__websocket_channels[ws][channel]["symbols"])

                            # Add callback to websocket channels
                            if callback and callback not in self.__websocket_channels[ws][channel]["callbacks"]:
                                self.__websocket_channels[ws][channel]["callbacks"].append(callback)

                        # Get symbols to register on channel on websocket
                        if symbol_number_on_channel == channel_limit:
                            self.__ws_log(f"Websocket '{ws}' already full on channel '{channel}'", "SUBSCRIBE")
                            if self.__websocket_number == self.__max_websocket:
                                self.__ws_log(f"[ERROR] Maximum number of websocket already reached. Cancelling next subscribtions on '{channel}''", "SUBSCRIBE")
                                return
                            else:
                                continue
                        elif symbol_number_on_channel + len(symbols) <= channel_limit:
                            symbol_number_to_register = len(symbols)
                            symbols_to_register = symbols
                        else:
                            symbol_number_to_register = channel_limit - symbol_number_on_channel
                            symbols_to_register = symbols[symbol_index:symbol_index+symbol_number_to_register]

                        # Subscribe symbols on channel on websocket
                        self.__ws_log(f"Register symbols '{symbols_to_register}' on websocket '{ws}'.", "SUBSCRIBE")
                        self.__subscribe(ws, channel, symbols_to_register, auth)
                        symbol_index += symbol_number_to_register

                        # Add symbols to websocket channels
                        with self.__websocket_channels_mutex:
                            self.__websocket_channels[ws][channel]["symbols"].extend(symbols_to_register)

                        # Break loop if all symbols are registered
                        if symbol_index >= len(symbols):
                            break

                # Register symbols on new websockets
                while symbol_index < len(symbols):

                    # Create new websocket
                    if not (ws := self.__create_websocket()):
                        return

                    # Get number of symbols registered on channel on websocket
                    with self.__websocket_channels_mutex:
                        self.__websocket_channels[ws] = {
                            channel: {
                                "auth": auth,
                                "callbacks": [],
                                "symbols": [],
                            }
                        }
                        symbol_number_on_channel = len(self.__websocket_channels[ws][channel]["symbols"])

                        # Add callback to websocket channels
                        if callback and callback not in self.__websocket_channels[ws][channel]["callbacks"]:
                            self.__websocket_channels[ws][channel]["callbacks"].append(callback)

                    # Get symbols to register on channel on websocket
                    if symbol_number_on_channel + len(symbols[symbol_index:]) <= channel_limit:
                        symbol_number_to_register = len(symbols[symbol_index:])
                        symbols_to_register = symbols[symbol_index:]
                    else:
                        symbol_number_to_register = channel_limit - symbol_number_on_channel
                        symbols_to_register = symbols[symbol_index:symbol_index+symbol_number_to_register]

                    # Subscribe symbols on channel on websocket
                    self.__ws_log(f"Register symbols '{symbols_to_register}' on websocket '{ws}'.", "SUBSCRIBE")
                    self.__subscribe(ws, channel, symbols_to_register, auth)
                    symbol_index += symbol_number_to_register

                    # Add symbols to websocket channels
                    with self.__websocket_channels_mutex:
                        self.__websocket_channels[ws][channel]["symbols"].extend(symbols_to_register)

    def __unsubscribe_dispatcher(self, channel, symbols=None, callback=None):
        self.__ws_log(f"Unsubscribing from '{channel}' with symbols '{symbols}'", "UNSUBSCRIBE")

        if symbols is None:
            symbols = []

        websockets_to_remove = []
        with self.__subscribe_mutex:
            with self.__websocket_channels_mutex:
                for websocket in self.__websocket_channels.keys():

                    if len(symbols) > 0:
                        for symbol in symbols:
                            if channel in self.__websocket_channels[websocket] and symbol in self.__websocket_channels[websocket][channel]["symbols"]:

                                # Unsubscribe symbol
                                self.__unsubscribe(websocket, channel, [symbol])

                                # Remove symbol from channel registered ones
                                self.__websocket_channels[websocket][channel]["symbols"].remove(symbol)

                                if len(self.__websocket_channels[websocket][channel]["symbols"]) == 0:

                                    # Remove callback if no symbol registered anymore
                                    if callback in self.__websocket_channels[websocket][channel]["callbacks"]:
                                        self.__websocket_channels[websocket][channel]["callbacks"].remove(callback)

                                    # Remove channel if no symbol registered anymore
                                    del self.__websocket_channels[websocket][channel]

                                # Remove all callbacks if none given
                                if callback is None and channel in self.__websocket_channels[websocket]:
                                    self.__websocket_channels[websocket][channel]["callbacks"] = []

                    else:
                        if channel in self.__websocket_channels[websocket]:
                            # Unsubscribe channel
                            self.__unsubscribe(websocket, channel, [])
                            # Remove channel
                            del self.__websocket_channels[websocket][channel]

                    # Delete websocket if no channel subscribed anymore
                    if len(self.__websocket_channels[websocket]) == 0:
                        self.__delete_websocket(websocket)
                        websockets_to_remove.append(websocket)

                # Remove empty websockets from subscribed channels
                for websocket in websockets_to_remove:
                    del self.__websocket_channels[websocket]

            self.__ws_log(f"Unsubscribed from '{channel}' with symbols '{symbols}'", "UNSUBSCRIBE")

    def __subscribe(self, websocket, channel, symbols, auth=False):
        self.__ws_log(f"Subscribing to '{channel}' with symbols '{symbols}' on {websocket}", "SUBSCRIBE")

        # Authenticate on websocket if needed
        if auth:
            with self.__websocket_handlers_mutex:
                is_auth = self.__websocket_handlers[websocket]["is_auth"]
            if not is_auth:
                self.__auth(websocket)

        data = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": channel,
                    }
                ]
            }
        }
        if symbols:
            data["payload"]["channels"][0]["symbols"] = symbols

        # Send subscribe message on websocket
        log(f"[{self}][SUBSCRIBE] Subscribe on {websocket} with {len(symbols)} symbols")
        self.__ws_send(websocket, data)

        self.__ws_log(f"Subscribed to '{channel}' with symbols '{symbols}' on {websocket}", "SUBSCRIBE")

    def __unsubscribe(self, websocket, channel, symbols):
        self.__ws_log(f"Unsubscribing from '{channel}' with symbols '{symbols}' on {websocket}", "UNSUBSCRIBE")

        data = {
            "type": "unsubscribe",
            "payload": {
                "channels": [
                    {
                        "name": channel,
                    }
                ]
            }
        }
        if symbols:
            data["payload"]["channels"][0]["symbols"] = symbols

        # Send subscribe message on websocket
        log(f"[{self}][UNSUBSCRIBE] Unsubscribe on {websocket} with {len(symbols)} symbols")
        self.__ws_send(websocket, data)

        self.__ws_log(f"Unsubscribed from '{channel}' with symbols '{symbols}' on {websocket}", "UNSUBSCRIBE")

    def __reset_timer(self, websocket):
        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
            self.__websocket_handlers[websocket]["reconnect_timer"] = threading.Timer(self.__heartbeat_timeout, self.__reconnect_timer, [websocket])
            self.__websocket_handlers[websocket]["reconnect_timer"].name = f"Thread-DeltaExchange-reconnect-{hex(id(websocket))}"
            self.__websocket_handlers[websocket]["reconnect_timer"].start()

    def __reconnect_timer(self, websocket):
        log(f"[{self}][RECONNECT-TIMER] Timer triggered on {websocket}")
        try:
            self.__reconnect(websocket)
        except:
            log(f"[{self}][RECONNECT] Reconnection failed.")
            self.__delete_websocket(websocket)
            with self.__websocket_channels_mutex:
                del self.__websocket_channels[websocket]

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
            self.__websocket_handlers[websocket]["reconnect_timer"].name = f"Thread-DeltaExchange-reconnect-{hex(id(websocket))}"
            self.__websocket_handlers[websocket]["reconnect_timer"].start()

        # Resubscribe websocket channels
        with self.__subscribe_mutex:
            with self.__websocket_channels_mutex:
                for channel in self.__websocket_channels[websocket].keys():
                    symbols = self.__websocket_channels[websocket][channel]["symbols"]
                    auth = self.__websocket_channels[websocket][channel]["auth"]
                    self.__subscribe(websocket, channel, symbols, auth)

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

        #self.__ws_log(f"Websocket {websocket} closed", "ON_CLOSE")
        log(f"[{self}][ON_CLOSE] Websocket {websocket} closed")

        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_connected"] = False
            self.__websocket_handlers[websocket]["is_auth"] = False
            self.__websocket_handlers[websocket]["connection_lost"] = True
            self.__websocket_handlers[websocket]["reconnect_timer"].cancel()
            self.__websocket_handlers[websocket]["reconnect_timer"] = None

        # Start background reconnection thread
        threading.Thread(target=self.__reconnection_loop, args=[websocket]).start()

    def __on_message(self, websocket, message):

        #bef = time.time()

        data = json.loads(message)

        self.__ws_log(message, f"RECEIVE - {websocket}")

        #if "type" not in data or ("type" in data and data["type"] != "heartbeat"):
        #    self.__ws_log(message, "RECEIVE")

        if "type" in data:
            event = data["type"]

            if event == "heartbeat":
                self.__reset_timer(websocket)

            # Orderbook update
            elif event == "l2_orderbook":

                # Check message delay
                event_timestamp = datetime.datetime.fromtimestamp(data["timestamp"]/1000000)
                reception_timestamp = datetime.datetime.now()
                timestamp_delay = reception_timestamp - event_timestamp
                if timestamp_delay > datetime.timedelta(seconds=0.5):
                    self.__ws_log(f"Message delayed by {timestamp_delay} (event {event_timestamp} / received {reception_timestamp}", f"DELAY - {websocket}")
                    return

                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["symbol"]][0]

                data_refactored = {
                    "symbol": symbol,
                    "buy": [],
                    "sell": [],
                }

                """
                if "buy" in data:
                    for order in data["buy"]:
                        order_refactored = {
                            "price": float(order["limit_price"]),
                            "size": order["size"],
                        }
                        data_refactored["buy"].append(order_refactored)
                if "sell" in data:
                    for order in data["sell"]:
                        order_refactored = {
                            "price": float(order["limit_price"]),
                            "size": order["size"],
                        }
                        data_refactored["sell"].append(order_refactored)
                """
                for order in data["buy"]:
                    order_refactored = {
                        "price": float(order["limit_price"]),
                        "size": order["size"],
                    }
                    data_refactored["buy"].append(order_refactored)
                for order in data["sell"]:
                    order_refactored = {
                        "price": float(order["limit_price"]),
                        "size": order["size"],
                    }
                    data_refactored["sell"].append(order_refactored)

            elif event == "l2_updates":

                # Check message delay
                event_timestamp = datetime.datetime.fromtimestamp(data["timestamp"]/1000000)
                reception_timestamp = datetime.datetime.now()
                timestamp_delay = reception_timestamp - event_timestamp
                #print(f"### LATENCY : {timestamp_delay}")
                if timestamp_delay > datetime.timedelta(seconds=0.5):
                    self.__ws_log(f"Message delayed by {timestamp_delay} (event {event_timestamp} / received {reception_timestamp}", f"DELAY - {websocket}")
                    #return

                if len(data["bids"]) == 0 and len(data["asks"]) == 0:
                    return

                product = [product for key, product in self.__products.items() if product["exchange_symbol"] == data["symbol"]][0]
                symbol = product["symbol"]

                if symbol not in self.__orderbooks:
                    self.__orderbooks[symbol] = {"symbol": symbol, "buy": [], "sell": []}

                data_refactored = {
                    "symbol": symbol,
                    "exchange_id": product["exchange_id"],
                    "exchange_symbol": product["exchange_symbol"],
                    "buy": [],
                    "sell": [],
                }

                if data["action"] == "snapshot":

                    # Cleanup remaining data
                    self.__orderbooks[symbol] = {"symbol": symbol, "buy": [], "sell": []}

                    for order in data["bids"]:
                        self.__orderbooks[symbol]["buy"].append({"price": float(order[0]), "size": float(order[1])})
                    for order in data["asks"]:
                        self.__orderbooks[symbol]["sell"].append({"price": float(order[0]), "size": float(order[1])})

                elif data["action"] == "update":

                    for order in data["bids"]:
                        price = float(order[0])
                        size = float(order[1])
                        index = next((index for (index, existing_order) in enumerate(self.__orderbooks[symbol]["buy"]) if existing_order["price"] == price), None)

                        # Price already exists in orderbook
                        if index != None:
                            # Remove price
                            if size == 0:
                                del self.__orderbooks[symbol]["buy"][index]
                            # Update size
                            else:
                                self.__orderbooks[symbol]["buy"][index]["size"] = size
                        # New price
                        elif size >0:
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
                        price = float(order[0])
                        size = float(order[1])
                        index = next((index for (index, existing_order) in enumerate(self.__orderbooks[symbol]["sell"]) if existing_order["price"] == price), None)

                        # Price already exists in orderbook
                        if index != None:
                            # Remove price
                            if size == 0:
                                del self.__orderbooks[symbol]["sell"][index]
                            # Update size
                            else:
                                self.__orderbooks[symbol]["sell"][index]["size"] = size
                        # New price
                        elif size > 0:
                            # Insert price in sorted list
                            inserted = False
                            for index, existing_order in enumerate(self.__orderbooks[symbol]["sell"]):
                                if price < existing_order["price"]:
                                    self.__orderbooks[symbol]["sell"].insert(index, {"price": price, "size": size})
                                    inserted = True
                                    break
                            if not inserted:
                                self.__orderbooks[symbol]["sell"].append({"price": price, "size": size})

                data_refactored["buy"] = copy.deepcopy(self.__orderbooks[symbol]["buy"])
                data_refactored["sell"] = copy.deepcopy(self.__orderbooks[symbol]["sell"])

            # Mark price update
            elif event == "mark_price":

                product_symbol = data["symbol"].split(":")[1]
                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == product_symbol][0]

                data_refactored = {
                    "symbol": symbol,
                    "mark_price": float(data["price"]),
                }

            # Funding rate update
            elif event == "funding_rate":

                product_symbol = data["symbol"]
                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == product_symbol][0]

                data_refactored = {
                    "symbol": symbol,
                    "funding_rate": float(data["funding_rate"]),
                }

            # Volume update
            elif event == "v2/ticker":

                product_symbol = data["symbol"]
                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == product_symbol][0]

                data_refactored = {
                    "symbol": symbol,
                    "volume": float(data["volume"]),
                }

            # Order update
            elif event == "orders":

                #TODO: Temporary ignore snapshot messages
                if "action" in data and data["action"] == "snapshot":
                    return

                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["symbol"]][0]

                data_refactored = {
                    "id": str(data["id"]),
                    "type": self.__compose_order_type(data),
                    "status": self.__compose_order_status(data),
                    "symbol": symbol,
                    "exchange_id": data["product_id"],
                    "exchange_symbol": data["product_symbol"],
                    "side": data["side"],
                    "size": data["size"],
                    "filled_size": data["size"] - data["unfilled_size"],
                    "unfilled_size": data["unfilled_size"],
                    "average_price": float(data["average_fill_price"]) if data["average_fill_price"] else 0,
                    "limit_price": float(data["limit_price"]) if data["limit_price"] else None,
                    "stop_price": float(data["stop_price"]) if data["stop_price"] else None,
                    "time_in_force": data["time_in_force"],
                }

            # Balance update
            elif event == "margins":
                data_refactored = {
                    "symbol": data["asset_symbol"],
                    "exchange_id": data["asset_id"],
                    "exchange_symbol": data["asset_symbol"],
                    "balance": float(data["balance"]),
                    "available_balance": float(data["available_balance"]),
                    "initial_margin": 0,
                    "maintenance_margin": float(data["portfolio_margin"]),
                }

            # Positions update
            elif event == "positions":
                data_refactored = {}

                if data["action"] == "snapshot":
                    positions = data["result"]
                else:
                    positions = [data]

                for position in positions:
                    if data["action"] == "snapshot":
                        product = [product for key, product in self.__products.items() if product["exchange_symbol"] == position["product_symbol"]][0]
                        exchange_symbol = position["product_symbol"]
                    else:
                        product = [product for key, product in self.__products.items() if product["exchange_symbol"] == position["symbol"]][0]
                        exchange_symbol = position["symbol"]

                    symbol = product["symbol"]

                    data_refactored[symbol] = {
                        "symbol": symbol,
                        "exchange_id": position["product_id"],
                        "exchange_symbol": exchange_symbol,
                        "size": position["size"],
                        "entry_price": float(position["entry_price"]),
                        "maintenance_margin": float(position["margin"]),
                        "contract_type": product["contract_type"],
                        "base_asset_id": product["base_asset_id"],
                        "base_asset_symbol": product["base_asset_symbol"],
                        "quote_asset_id": product["quote_asset_id"],
                        "quote_asset_symbol": product["quote_asset_symbol"],
                    }
            else:
                data_refactored = data

            #af = time.time()
            #print(f"### PROCESSED IN {af-bef}")

            with self.__websocket_channels_mutex:
                if (websocket in self.__websocket_channels and
                    event in self.__websocket_channels[websocket]):

                    # Call channel callbacks
                    for callback in self.__websocket_channels[websocket][event]["callbacks"]:
                        try:
                            callback(self, copy.deepcopy(data_refactored))
                        except Exception as e:
                            log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __heartbeat(self, websocket, enable=True):
        if enable:
            self.__ws_log("Enable heartbeat")
            type = "enable_heartbeat"
        else:
            self.__ws_log("Disable heartbeat")
            type = "disable_heartbeat"
        self.__ws_send(websocket, {
            "type": type
        })

    def __auth(self, websocket):
        self.__ws_log(f"Authenticating on {websocket}", "AUTH")
        method = 'GET'
        timestamp = self.__get_timestamp()
        path = '/live'
        data = method + timestamp + path

        self.__ws_send(websocket, {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": self.__generate_signature(data),
                "timestamp": timestamp
            }
        })

        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_auth"] = True

        self.__ws_log(f"Authenticated on {websocket}", "AUTH")

    def __unauth(self, websocket):
        self.__ws_log(f"Unauthenticating on {websocket}", "UNAUTH")

        self.__ws_send(websocket, {
            "type": "unauth",
            "payload": {}
        })

        with self.__websocket_handlers_mutex:
            self.__websocket_handlers[websocket]["is_auth"] = False

        self.__ws_log(f"Unauthenticated on {websocket}", "UNAUTH")

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
            file_path = "delta_exchange_requests.log"
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
