from connectors.crypto.connector.common.connector import CryptoConnector
from connectors.threading.Threads import StoppableThread, format_traceback
from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.crypto.connector.forex import Forex
from connectors.crypto.singleton import Singleton
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
from decimal import Decimal
import pandas as pd
import numpy as np
import datetime
import requests
import hashlib
import base64
import hmac
import json
import time

class CbForex(metaclass=Singleton):
    def __init__(self, database=None):
        self.database = database
        self.connector = Coinbase("", "")

    def get_rate(self, base_currency, dest_currency, date=None):
        usd_stable_coins = ["USD", "USDT", "USDC", "BUSD"]
        rate = None
        if base_currency == dest_currency:
            rate = 1
        elif (base_currency == "EUR" and dest_currency in usd_stable_coins) or (base_currency in usd_stable_coins and dest_currency == "EUR"):
            pair = "USDT_EUR"
            if not date:
                rate = self.connector.get_mark_price(pair)["mark_price"]
            else:
                rate = self.connector.get_candle(pair, date, date-datetime.timedelta(minutes=5))["close"]

            if base_currency == "EUR":
                rate = 1/rate
        elif base_currency in usd_stable_coins:
            rate = self.connector.get_symbol_usd_price(dest_currency, date)
            if rate:
                rate = 1/rate
        elif dest_currency in usd_stable_coins:
            rate = self.connector.get_symbol_usd_price(base_currency, date)
        else:
            rate = Forex(self.database).get_rate(base_currency, dest_currency, date)
        if not rate:
            rate = Forex(self.database).get_rate(base_currency, dest_currency, date)
        return rate

class Coinbase:
    PLATFORM_ID = 9
    PLATFORM_NAME = "Coinbase"

    def __init__(self, api_key, api_secret, testnet=False, strategy=None, passphrase="", base_currency="USD"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.session = requests.Session()
        self.brokerage_fees_profile_id = '3655dc39-8a33-4a4c-a012-da594a2031f2'
        self.tilvest_brokering_fees = 0.00125

        self.__request_rate_limit_timeout = 30

        self.accepted_IBANs = ["FR76 1027 8030 5100 0210 1930 332"]

        if self.testing:
            self.base_url = "https://api-public.sandbox.exchange.coinbase.com"
            #self.ws_url = "wss://ws-feed-public.sandbox.exchange.coinbase.com"
            #self.ws_url = "wss://ws-direct.sandbox.exchange.coinbase.com"
        else:
            self.base_url = "https://api.exchange.coinbase.com"
            #self.ws_url = "wss://ws-feed.exchange.coinbase.com"
            #self.ws_url = "wss://ws-direct.sandbox.exchange.coinbase.com"

        self.__products = {}
        self.__assets = {}

######################
### Public methods ###
######################

################
### REST API ###
################

    def is_ip_authorized(self):
        try:
            self.get_balance()
        except:
            return False
        return True

    def has_read_permission(self):
        pass

    def has_write_permission(self):
        pass

    def has_withdraw_permission(self):
        pass

    def has_future_authorized(self):
        pass

    def get_account(self, account_id):
        return self.__request("GET", f"/accounts/{account_id}", auth=True)

    def get_fees(self):
        response = self.__api_get_fees()
        return {
            "taker": float(response["taker_fee_rate"]),
            "maker": float(response["maker_fee_rate"]),
        }

    def get_balance(self, as_base_currency=False, as_eur=False):
        if not self.__products:
            self.get_products()

        if as_base_currency:
            rate = CbForex(self.database).get_rate("USD", self.base_currency)
        else:
            rate = 1

        data = {}
        accounts = self.__api_get_accounts()
        for account in accounts:
            if float(account["balance"]) > 0:
                symbol = account["currency"]

                if as_base_currency:
                    usd_price = self.__get_symbol_usd_price(symbol)
                    balance = float(account["balance"])*usd_price*rate
                    available = float(account["available"])*usd_price*rate
                else:
                    balance = float(account["balance"])
                    available = float(account["available"])

                data[symbol] = {
                    "symbol": symbol,
                    "balance": balance,
                    "available_balance": available,
                    "currency": self.base_currency if as_base_currency else symbol,
                }

        return data

    def get_profit(self, buy_crypto_history=None):
        if not self.__products:
            self.get_products()

        total_balance = 0
        total_deposit = 0
        total_withdraw = 0
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

        transaction_history = self.get_deposit_withdraw_history()
        for transaction in transaction_history:
            # print("##########")
            # print(transaction)
            # print("##########")

            if transaction['operation_id'] in feeder_cashflows_transaction_ids and not self.strategy.is_feeder_investable():
                #print(f"### Skip transaction by feeder id")
                continue
            if transaction['operation_id'] in internal_deposit_transaction_ids:
                #print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['operation_id'] in internal_withdraw_transaction_ids:
                #print(f"### Skip transaction by internal withdraw id")
                continue
            if transaction['operation_id'] in internal_fees_transaction_id:
                #print(f"### Skip transaction by internal withdraw id")
                continue
            if transaction["type"] == "deposit":
                #print("### ADD deposit")
                total_deposit += transaction["base_currency_amount"]
            else:
                #print("### ADD withdraw")
                total_withdraw += transaction["base_currency_amount"]


            """
            # Substract feeder cashflows from deposits and add to withdrawals
            feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
            feeder_cashflows_amount = 0
            for index, cashflow in feeder_cashflows.iterrows():
                rate = CbForex(self.database).get_rate(cashflow.currency, self.base_currency, datetime.datetime.strptime(str(cashflow.timestamp), "%Y-%m-%d %H:%M:%S"))
                feeder_cashflows_amount += cashflow.amount*rate
                print(f"### Cashflow tx_id {cashflow.last_tx_id}")
                print(f"### Remove feeder cashflow of {cashflow.amount*rate}")
            total_deposit -= feeder_cashflows_amount
            print(f"### Remove total feeder cashflow of {feeder_cashflows_amount}")
            total_withdraw -= feeder_cashflows_amount
            """

        data = {
            "total_balance": total_balance,
            "total_deposit": total_deposit,
            "total_withdraw": total_withdraw,
            "total_profit": total_balance - total_deposit + total_withdraw,
        }
        return data

    def get_assets(self, reload=False):
        if not self.__assets or reload:
            self.__assets = {}
            currencies = self.__api_get_currencies()
            for currency in currencies:
                self.__assets[currency["id"]] = {
                    "id": currency["id"],
                    "symbol": currency["id"],
                }
        return self.__assets

    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}
            products = self.__api_get_products()
            for product in products:
                if product["status"] == "delisted":
                    continue

                base_asset_symbol = product["base_currency"]
                quote_asset_symbol = product["quote_currency"]
                generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["id"],
                    "exchange_symbol": product["display_name"],
                    "contract_type": "spot",
                    "contract_size": 1,
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
                            "min": product["base_increment"],
                            "max": None},
                        "price": {
                            "min": product["quote_increment"],
                            "max": None},
                        "notional": {
                            "min": product["min_market_funds"],
                            "max": None},
                    },
                    "base_asset_id": base_asset_symbol,
                    "base_asset_symbol": base_asset_symbol,
                    "quote_asset_id": quote_asset_symbol,
                    "quote_asset_symbol": quote_asset_symbol,
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

        for order in response["bids"]:
                order_refactored = {
                    "price": float(order[0]),
                    "size": float(order[1]),
                }
                data["buy"].append(order_refactored)
        for order in response["asks"]:
            order_refactored = {
                "price": float(order[0]),
                "size": float(order[1]),
            }
            data["sell"].append(order_refactored)

        return data

    def get_spot_price(self, symbol):
        pass

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
            response = self.__api_get_ticker(exchange_id)
            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "mark_price": float(response["price"]),
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
        pass

    def get_positions(self):
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        data = {}
        balances = self.get_balance()
        for symbol, balance in balances.items():
            usd_price = self.__get_symbol_usd_price(symbol)
            data[symbol] = {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "size": balance["balance"],
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": symbol,
                "base_asset_symbol": symbol,
                "quote_asset_id": self.base_currency,
                "quote_asset_symbol": self.base_currency,
                "base_currency_amount": balance["balance"]*usd_price*rate,
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

        return {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "size": 0,
            "entry_price": 0,
            "total_pl": 0,
        }

    def get_position_margin(self, symbol):
        pass

    def set_position_margin(self, symbol):
        pass

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)

        try:
            response = self.__api_place_order(exchange_id, side, size, order_type="market_order", time_in_force=time_in_force, reduce_only=reduce_only)
        except LimitModeOnlyError:
            log(f"[{self}] Can't place market order on symbol '{symbol}'. Place limit order instead.")
            mark_price = self.get_mark_price(symbol)["mark_price"]
            limit_price = mark_price*1.001 if side == "buy" else mark_price*0.999
            return self.place_limit_order(symbol, size, side, limit_price=limit_price, time_in_force=time_in_force, timeout=30, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_id"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["filled_size"]),
            "unfilled_size": float(response["size"]) - float(response["filled_size"]),
            "average_price": float(response["executed_value"])/float(response["filled_size"]) if float(response["filled_size"]) != 0 else 0,
            "limit_price": float(response["price"]) if "price" in response else None,
            "stop_price": float(response["stop_price"]) if "stop_price" in response else None,
            "time_in_force": time_in_force,
            "commission": float(response["fill_fees"]) if "fill_fees" in response else None,
            "created_at": datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
            "updated_at": datetime.datetime.strptime(response["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in response else datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
        }

        time.sleep(1)
        data = self.get_order_state(symbol, data["id"])
        # Wait for order to be updated
        if wait:
            while response["status"] == "open":
                time.sleep(1)
                data = self.get_order_state(symbol, data["id"])

        # Withdraw brokering fees
        try:
            if data["status"] == "filled":
                fees_symbol = symbol.split('_')[0]
                if fees_symbol not in ["USDT", "USDC"]:
                    filled_size = data['filled_size']
                    brokering_fees_tilvest = filled_size*self.tilvest_brokering_fees
                    data['filled_size'] = filled_size - brokering_fees_tilvest

                    if side == 'sell':
                        brokering_fees_tilvest = brokering_fees_tilvest*data["average_price"]
                        fees_symbol = symbol.split('_')[1]

                    fees_symbol = "USDC" if fees_symbol == "USD" else fees_symbol

                    if self.withdraw_to_account(self.brokerage_fees_profile_id, fees_symbol, brokering_fees_tilvest):
                        now = datetime.datetime.now()

                        # Add cashflow
                        cashflow = pd.DataFrame([{
                            "strategy_id": self.strategy.id,
                            "transaction_type": "brokering_fees",
                            "last_tx_id": data["id"],
                            "timestamp": now,
                            "amount": brokering_fees_tilvest,
                            "currency": fees_symbol,
                            "number_shares_before": 0,
                            "number_shares_after": 0,
                            "nav": 1,
                            "tilvest_approved": True,
                            "created_at": now,
                            "updated_at": now,
                        }])
                        self.database.append_to_table("strategy_cashflows", cashflow)

                        # Add brokering fees
                        brokering_fees = pd.DataFrame([{
                            "strategy_id": self.strategy.id,
                            "symbol": fees_symbol,
                            "amount": brokering_fees_tilvest,
                            "eur_value": brokering_fees_tilvest*CbForex(self.database).get_rate(fees_symbol, "EUR"),
                            "timestamp": now,
                            "created_at": now,
                            "updated_at": now,
                        }])
                        self.database.append_to_table("brokering_fees", brokering_fees)
                    else:
                        log(f"[{self}] [ERROR] Failed to withdraw brokering fees")

        except Exception as e:
            log(f"[{self}] [ERROR] Unable to make transfer for brokerage fees : {type(e)} - {e}")

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "market_order", data["status"], data["average_price"])

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

        response = self.__api_place_order(exchange_id, side, size, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

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
                order_id = data["id"]
                data = {}

                for i in range(0,3):
                    time.sleep(1)
                    try:
                        data = self.get_order_state(symbol, order_id)
                        break
                    except Exception as e:
                        if i == 2:
                            raise e

                try:
                    if data["status"] == "filled":
                        fees_symbol = symbol.split('_')[0]
                        if fees_symbol not in ["USDT", "USDC"]:
                            filled_size = data['filled_size']
                            brokering_fees_tilvest = filled_size*self.tilvest_brokering_fees
                            data['filled_size'] = filled_size - brokering_fees_tilvest

                            if side == 'sell':
                                brokering_fees_tilvest = brokering_fees_tilvest*data["average_price"]
                                fees_symbol = symbol.split('_')[1]

                            if self.withdraw_to_account(self.brokerage_fees_profile_id, fees_symbol, brokering_fees_tilvest):
                                now = datetime.datetime.now()

                                # Add cashflow
                                cashflow = pd.DataFrame([{
                                    "strategy_id": self.strategy.id,
                                    "transaction_type": "brokering_fees",
                                    "last_tx_id": data["id"],
                                    "timestamp": now,
                                    "amount": brokering_fees_tilvest,
                                    "currency": fees_symbol,
                                    "number_shares_before": 0,
                                    "number_shares_after": 0,
                                    "nav": 1,
                                    "tilvest_approved": True,
                                    "created_at": now,
                                    "updated_at": now,
                                }])
                                self.database.append_to_table("strategy_cashflows", cashflow)

                                # Add brokering fees
                                brokering_fees = pd.DataFrame([{
                                    "strategy_id": self.strategy.id,
                                    "symbol": fees_symbol,
                                    "amount": brokering_fees_tilvest,
                                    "eur_value": brokering_fees_tilvest*CbForex(self.database).get_rate(fees_symbol, "EUR"),
                                    "timestamp": now,
                                    "created_at": now,
                                    "updated_at": now,
                                }])
                                self.database.append_to_table("brokering_fees", brokering_fees)

                except Exception as e:
                    log(f"[{self}] [ERROR] Unable to make transfer for brokerage fees : {type(e)} - {e}")

                """
                data["status"] = self.__compose_order_status(order)
                data["symbol"] = symbol
                data["exchange_id"] = order["product_id"]
                data["exchange_symbol"] = order["product_id"]
                data["side"] = order["side"]
                data["size"] = float(order["size"])
                data["filled_size"] = float(order["filled_size"])
                data["unfilled_size"] = float(order["size"]) - float(order["filled_size"])
                data["average_price"] = float(order["executed_value"])/float(order["filled_size"]) if float(response["filled_size"]) != 0 else 0
                data["limit_price"] = float(response["price"]) if "price" in order else None
                data["stop_price"] = None
                data["time_in_force"] = time_in_force
                data["commission"] = float(order["fill_fees"]) if "fill_fees" in order else None
                data["created_at"]: datetime.datetime.strptime(order["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
                data["updated_at"]: datetime.datetime.strptime(order["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in order else datetime.datetime.strptime(order["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
                """

        # Doesn't wait for order to be placed
        else:

            data["status"] = self.__compose_order_status(response)
            data["symbol"] = symbol
            data["exchange_id"] = response["product_id"]
            data["exchange_symbol"] = response["product_id"]
            data["side"] = response["side"]
            data["size"] = float(response["size"])
            data["filled_size"] = float(response["filled_size"])
            data["unfilled_size"] = float(response["size"]) - float(response["filled_size"])
            data["average_price"] = float(response["executed_value"])/float(response["filled_size"]) if float(response["filled_size"]) != 0 else 0
            data["limit_price"] = float(response["price"]) if "price" in response else None
            data["stop_price"] = None
            data["time_in_force"] = time_in_force
            data["commission"] = float(response["fill_fees"]) if "fill_fees" in response else None
            data["created_at"]: datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
            data["updated_at"]: datetime.datetime.strptime(response["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in response else datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S")

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], data["average_price"])

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

        response = self.__api_place_order(exchange_id, side, size, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_id"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["filled_size"]),
            "unfilled_size": float(response["size"]) - float(response["filled_size"]),
            "average_price": float(response["executed_value"])/float(response["filled_size"]) if float(response["filled_size"]) != 0 else 0,
            "limit_price": float(response["price"]) if "price" in response else None,
            "stop_price": float(response["stop_price"]) if "stop_price" in response else None,
            "time_in_force": time_in_force,
            "commission": float(response["fill_fees"]) if "fill_fees" in response else None,
            "created_at": datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
            "updated_at": datetime.datetime.strptime(response["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in response else datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_market_order", data["status"], data["average_price"])

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
        limit_price = self.__round_price(symbol, limit_price)
        stop_price = self.__round_price(symbol, stop_price)

        response = self.__api_place_order(exchange_id, side, size, order_type="stop_limit_order", limit_price=limit_price, stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)

        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_id"],
            "side": response["side"],
            "size": float(response["size"]),
            "filled_size": float(response["filled_size"]),
            "unfilled_size": float(response["size"]) - float(response["filled_size"]),
            "average_price": float(response["executed_value"])/float(response["filled_size"]) if float(response["filled_size"]) != 0 else 0,
            "limit_price": float(response["price"]) if "price" in response else None,
            "stop_price": float(response["stop_price"]) if "stop_price" in response else None,
            "time_in_force": time_in_force,
            "commission": float(response["fill_fees"]) if "fill_fees" in response else None,
            "created_at": datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
            "updated_at": datetime.datetime.strptime(response["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in response else datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_limit_order", data["status"], data["average_price"])

        return data

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.00075):
        pass

    def place_limit_custom(self, symbol, side, size, remaining_size, limit_price):
        print('size',size)
        print('remaining_size',remaining_size)

        mark_price = self.get_mark_price(symbol)["mark_price"]
        limit_price = mark_price*1.004
        log(f"Place buy limit order of '{size}' on '{symbol}' on {self.PLATFORM_NAME} at {limit_price}")
        response = self.place_limit_order(symbol, size=size, limit_price=limit_price, side='buy', timeout=30)
        log(f"{response}")
        traded_usdt = float(response['filled_size'])

        log(f"Place buy market order of '{remaining_size}' on '{symbol}' on {self.PLATFORM_NAME} at {limit_price}")
        response = self.place_market_order(symbol, size=remaining_size, side='buy', wait=True)
        log(f"{response}")
        traded_usdt += float(response['filled_size'])

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

            print("-------------------------------------------------------------------------------")
            print(json.dumps(order, indent=4))
            print("-------------------------------------------------------------------------------")


            if symbol and exchange_id != order["product_id"]:
                continue
            else:
                active_order = {
                    "id": str(order["id"]),
                    "type": self.__compose_order_type(order),
                    "status": self.__compose_order_status(order),
                    "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == order["product_id"]), None),
                    "exchange_id": order["product_id"],
                    "exchange_symbol": order["product_id"],
                    "side": order["side"],
                    "size": float(order["size"]),
                    "filled_size": float(order["filled_size"]),
                    "unfilled_size": float(order["size"]) - float(order["filled_size"]),
                    "average_price": float(order["executed_value"]),
                    "limit_price": float(order["price"]) if "price" in order else None,
                    "stop_price": float(order["stop_price"]) if "stop_price" in order else None,
                    "time_in_force": order["time_in_force"].lower(),
                    "commission": float(order["fill_fees"]) if "fill_fees" in order else None,
                    "created_at": datetime.datetime.strptime(order["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
                    "updated_at": datetime.datetime.strptime(order["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in order else datetime.datetime.strptime(order["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
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

        response = self.__api_get_order(order_id)

        print("-------------------------------------------------------------------------------")
        print(json.dumps(response, indent=4))
        print("-------------------------------------------------------------------------------")

        size = float(response["size"]) if "size" in response else float(response["filled_size"])
        data = {
            "id": str(response["id"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == response["product_id"]), None),
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_id"],
            "side": response["side"],
            "size": size,
            "filled_size": float(response["filled_size"]),
            "unfilled_size": size - float(response["filled_size"]),
            "average_price": float(response["executed_value"])/float(response["filled_size"]) if float(response["filled_size"]) != 0 else 0,
            "limit_price": float(response["price"]) if "price" in response else None,
            "stop_price": float(response["stop_price"]) if "stop_price" in response else None,
            "time_in_force": "???",
            "commission": float(response["fill_fees"]) if "fill_fees" in response else None,
            "created_at": datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
            "updated_at": datetime.datetime.strptime(response["done_at"][:19], "%Y-%m-%dT%H:%M:%S") if "done_at" in response else datetime.datetime.strptime(response["created_at"][:19], "%Y-%m-%dT%H:%M:%S"),
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

        try:
            order = self.get_order_state(symbol, order_id)
            response = self.__api_cancel_order(order_id)
        except OrderNotFoundError as e:
            # Set exception's order id
            e.order_id = order_id
            raise e

        if response == order_id:
            response = order

        data = order
        data["status"] = "cancelled"
        return data

    def get_order_history(self, symbol=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
        #response = self.__api_get_orders()
        response = self.__api_get_fills(exchange_id)
        #print("---------------------------------------")
        #print(json.dumps(response, indent=4))
        #print("---------------------------------------")
        return response

    def get_leverage(self, symbol):
        pass

    def set_leverage(self, symbol, leverage):
        pass

    def create_wallet_address(self, symbol):
        pass

    def get_wallet_address(self, symbol):
        pass

    def send_rfq(self, symbol, side):
        pass

    def get_trade_history(self, symbol, start_date, end_date):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_get_fills(exchange_id, start_date, end_date)
        return response

    def get_transaction_history(self):
        data = []
        transactions = self.get_deposit_withdraw_history()
        for transaction in transactions:
            data.append({
                "transaction_id": transaction["operation_id"],
                "transaction_type": transaction["type"],
                "symbol": transaction["source_symbol"],
                "amount": transaction["source_amount"],
                "date": transaction["date"],
            })
        return data

    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        data = []

        response = self.__api_get_all_transfers()
        for transfer in response:
            if not transfer["completed_at"]:
                continue

            date = datetime.datetime.strptime(transfer["completed_at"][0:19], '%Y-%m-%d %H:%M:%S')

            transaction_id = transfer["details"]["crypto_transaction_hash"] if "details" in transfer and "crypto_transaction_hash" in transfer["details"] else transfer["id"]
            # Filter-out fees and feeder cashflows
            if self.strategy and self.database:
                transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                if not transaction_cashflow.empty:
                    cashflow = transaction_cashflow.iloc[0]
                    if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                        continue

            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            if transfer["currency"] == 'EUROC':
                symbol = 'EUR'
            else:
                symbol = transfer["currency"]
            usd_amount = self.__get_symbol_usd_price(symbol, date)
            base_currency_amount = float(transfer["amount"])*rate*usd_amount

            data.append({
                "date": date,
                "type": "withdraw" if "withdraw" in transfer["type"] else "deposit",
                "source_symbol": transfer["currency"],
                "source_amount": float(transfer["amount"]),
                "destination_symbol": transfer["currency"],
                "destination_amount": float(transfer["amount"]),
                "fees": float(transfer["details"]["fee"]) if "fee" in transfer["details"] else 0,
                "operation_id": transfer["id"],
                "wallet_address": transfer["details"]["sent_to_address"] if "sent_to_address" in transfer["details"] else None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
                "tx_hash": transaction_id,
            })

        response = self.__api_get_all_transfers("internal_deposit")
        for transfer in response:
            date = datetime.datetime.strptime(transfer["created_at"][0:19], '%Y-%m-%d %H:%M:%S')

            transaction_id = transfer["details"]["crypto_transaction_hash"] if "details" in transfer and "crypto_transaction_hash" in transfer["details"] else transfer["id"]
            # Filter-out fees and feeder cashflows
            if self.strategy and self.database:
                transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                if not transaction_cashflow.empty:
                    cashflow = transaction_cashflow.iloc[0]
                    if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                        continue

            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            if transfer["currency"] == 'EUROC':
                symbol = 'EUR'
            else:
                symbol = transfer["currency"]
            usd_amount = self.__get_symbol_usd_price(symbol, date)

            base_currency_amount = float(transfer["amount"])*rate*usd_amount

            data.append({
                "date": date,
                "type": "withdraw" if "withdraw" in transfer["type"] else "deposit",
                "source_symbol": transfer["currency"],
                "source_amount": float(transfer["amount"]),
                "destination_symbol": transfer["currency"],
                "destination_amount": float(transfer["amount"]),
                "fees": 0,
                "operation_id": transfer["id"],
                "wallet_address": transfer["account_id"],
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
                "tx_hash": transaction_id if transaction_id else None,
            })

        response = self.__api_get_all_transfers("internal_withdraw")
        for transfer in response:
            date = datetime.datetime.strptime(transfer["created_at"][0:19], '%Y-%m-%d %H:%M:%S')

            transaction_id = transfer["details"]["crypto_transaction_hash"] if "details" in transfer and "crypto_transaction_hash" in transfer["details"] else transfer["id"]
            # Filter-out fees and feeder cashflows
            if self.strategy and self.database:
                transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                if not transaction_cashflow.empty:
                    cashflow = transaction_cashflow.iloc[0]
                    if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                        continue

            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            if transfer["currency"] == 'EUROC':
                symbol = 'EUR'
            else:
                symbol = transfer["currency"]
            usd_amount = self.__get_symbol_usd_price(symbol, date)
            base_currency_amount = float(transfer["amount"])*rate*usd_amount

            data.append({
                "date": date,
                "type": "withdraw" if "withdraw" in transfer["type"] else "deposit",
                "source_symbol": transfer["currency"],
                "source_amount": float(transfer["amount"]),
                "destination_symbol": transfer["currency"],
                "destination_amount": float(transfer["amount"]),
                "fees": 0,
                "operation_id": transfer["id"],
                "wallet_address": transfer["account_id"],
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
                "tx_hash": transaction_id if transaction_id else None,
            })

        data.sort(key=lambda transfer:transfer['date'], reverse=True)

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
            resolution = "60"
        elif timespan <= datetime.timedelta(days=1):
            resolution = "3600"
        elif timespan <= datetime.timedelta(weeks=1):
            resolution = "86400"
        else:
            resolution = "86400"

        start_timestamp = round(datetime.datetime.timestamp(start_date))
        stop_timestamp = round(datetime.datetime.timestamp(end_date))

        # Align timestamps with requested granularity
        start_timestamp = start_timestamp-start_timestamp%int(resolution)
        stop_timestamp = stop_timestamp-stop_timestamp%int(resolution)+int(resolution)-1

        response = self.__api_spot_get_candle(exchange_id, start_timestamp, stop_timestamp, resolution)

        candle_number = len(response)
        if candle_number == 0:
            open = 0
            close = 0
            low = 0
            high = 0
            volume = 0
        else:
            open = float(response[0][3])
            close = float(response[candle_number-1][4])
            low = float(response[0][1])
            high = float(response[0][2])
            volume = 0

            for candle in response:
                low = float(candle[1]) if float(candle[1]) < low else low
                high = float(candle[2]) if float(candle[2]) > high else high

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
        pass

    def get_portfolio_margins(self, currency, add_positions=True, simulated_positions={}):
        pass

    def get_margins(self, symbol, amount, price):
        pass

    def get_payment_methods(self):
        return self.__api_get_payment_methods()

    def withdraw_to_account(self, to_profile_id, symbol, amount):
        accounts = self.__api_get_accounts()
        from_profile_id = accounts[0]["profile_id"]
        print('WITHDRAW COINBASE')
        print('to_profile_id', to_profile_id)
        print('symbol', symbol)
        print('amount', amount)
        response = self.__api_profile_transfer(from_profile_id, to_profile_id, symbol, amount)
        return True if response.status_code == 200 else False

    def withdraw_to_crypto_adress(self, symbol, amount, address, network_name=None):
        response = self.__api_withdraw_to_crypto(symbol, amount, address)
        return response

    def withdraw_to_iban(self, symbol, amount, iban):
        if iban not in self.accepted_IBANs:
            raise UnauthorizedIBAN(f"Given IBAN '{iban}' is not authorized")

        payment_method_id = None
        payment_methods = self.get_payment_methods()
        for payment_method in payment_methods:
            if payment_method["picker_data"]["iban"] == iban:
                payment_method_id = payment_method["id"]

        print(payment_method_id)

        if not payment_method_id:
            raise UnknownIBAN(f"Given IBAN '{iban}' is unknown")

        accounts = self.__api_get_accounts()
        profile_id = accounts[0]["profile_id"]

        response = self.__api_withdraw_to_iban(profile_id, symbol, amount, payment_method_id)
        return response

    def get_profile_id(self):
        accounts = self.__api_get_accounts()
        profile_id = accounts[0]["profile_id"]
        return profile_id

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_fees(self):
        response = self.__request("GET", "/fees", auth=True)
        return response

    def __api_get_accounts(self):
        response = self.__request("GET", "/accounts", auth=True)
        return response

    def __api_get_currencies(self):
        response = self.__request("GET", "/currencies")
        return response

    def __api_get_products(self):
        response = self.__request("GET", "/products")
        return response

    def __api_get_orderbook(self, symbol, level=2):
        response = self.__request("GET", f"/products/{symbol}/book", payload={"level": level})
        return response

    def __api_get_ticker(self, symbol):
        response = self.__request("GET", f"/products/{symbol}/ticker")
        return response

    def __api_place_order(self, symbol, side, size, order_type, limit_price=None, stop_price=None, time_in_force="gtc", reduce_only=False):

        payload = {
            "product_id": symbol,
            "side": side,
            "size": np.format_float_positional(size, trim="-"),
            "time_in_force": time_in_force.upper(),
        }

        if order_type == "market_order":
            payload["type"] = "market"
        elif order_type == "limit_order":
            payload["type"] = "limit"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            mark_price = float(self.__api_get_ticker(symbol)["price"])
            payload["type"] = "limit"
            payload["stop_price"] = stop_price
            payload["price"] = stop_price
            if stop_price < mark_price:
                payload["stop"] = "loss"
            else:
                payload["stop"] = "entry"
        elif order_type == "stop_limit_order":
            mark_price = float(self.__api_get_ticker(symbol)["price"])
            payload["type"] = "limit"
            payload["stop_price"] = stop_price
            payload["price"] = limit_price
            if stop_price < mark_price:
                payload["stop"] = "loss"
            else:
                payload["stop"] = "entry"
        else:
            return None

        response = self.__request("POST", "/orders", payload, auth=True)
        return response

    def __api_get_active_orders(self):
        response = self.__request("GET", f"/orders", auth=True)
        return response

    def __api_get_order(self, order_id):
        response = self.__request("GET", f"/orders/{order_id}", auth=True)
        return response

    def __api_cancel_order(self, order_id, symbol=None):
        if symbol:
            response = self.__request("DELETE", f"/orders/{order_id}", payload={
                "product_id": symbol,
            }, auth=True)
        else:
            response = self.__request("DELETE", f"/orders/{order_id}", auth=True)
        return response

    def __api_get_orders(self, symbol=None):
        payload = None
        if symbol:
            payload = {"product_id": symbol}
        response = self.__request("GET", f"/api/v3/brokerage/orders/historical/batch", payload=payload, auth=True)
        return response

    def __api_get_fills(self, product_id, start_date=None, end_date=None):
        payload = {
            "product_id": product_id,
        }
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        response = self.__request("GET", f"/fills", payload=payload, auth=True)
        return response

    def __api_get_all_transfers(self, type=None):
        payload = {"type": type} if type else None
        response = self.__request("GET", f"/transfers", payload=payload, auth=True)
        return response

    def __api_spot_get_candle(self, symbol, start_time, end_time, resolution):
        response = self.__request("GET", f"/products/{symbol}/candles", {
            "granularity": resolution,
            "start": start_time,
            "end": end_time,
        })
        return response

    def __api_withdraw_to_crypto(self, symbol, amount, address):
        response = self.__request("POST", f"/withdrawals/crypto", {
            "currency": symbol,
            "amount": amount,
            "crypto_address": address,
        }, auth=True)
        return response

    def __api_profile_transfer(self, from_profile_id, to_profile_id, symbol, amount):
        response = self.__request("POST", f"/profiles/transfer", {
            "from": from_profile_id,
            "to": to_profile_id,
            "currency": symbol,
            "amount": amount,
        }, auth=True, parse=False)
        print(response)
        return response

    def __api_get_payment_methods(self):
        response = self.__request("GET", f"/payment-methods", auth=True)
        return response

    def __api_withdraw_to_iban(self, profile_id, currency, amount, payment_method_id):
        response = self.__request("POST", f"/withdrawals/payment-method", payload={
            "profile_id": profile_id,
            "amount": amount,
            "payment_method_id": payment_method_id,
            "currency": currency,
        }, auth=True)
        return response

    def get_symbol_usd_price(self, symbol, date=None):
        return self.__get_symbol_usd_price(symbol, date)

    def __get_symbol_usd_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        price = None
        convs = ["USD", "USDT", "USDC", "BUSD"]

        if symbol == "EURC":
            symbol = "EUR"

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

    def __round_size(self, symbol, size):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif self.__products[symbol]["limits"]["amount"]["min"]:
            rounded_size = float(Decimal(str(size)) - Decimal(str(size)) % Decimal(str(self.__products[symbol]["limits"]["amount"]["min"])))
            return rounded_size
        return size

    def __round_price(self, symbol, price):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif self.__products[symbol]["limits"]["price"]["min"]:
            rounded_size = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(self.__products[symbol]["limits"]["price"]["min"])))
            return rounded_size
        return price

    def __request(self, method, path, payload=None, auth=False, base_url=None, headers=None, parse=True):
        if base_url is None:
            base_url = self.base_url
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = '%s%s' % (base_url, path)
        headers["Content-Type"] = "application/json"

        if auth:
            timestamp = self.__get_timestamp()
            signature_data = timestamp + method + path
            if payload:
                if method == "GET":
                    signature_data += f"?{urlencode(payload)}"
                else:
                    signature_data += json.dumps(payload)
            signature = self.__generate_signature(signature_data)
            headers["CB-ACCESS-TIMESTAMP"] = timestamp
            headers["CB-ACCESS-KEY"] = self.api_key
            headers["CB-ACCESS-SIGN"] = signature
            headers["CB-ACCESS-PASSPHRASE"] = self.passphrase

        retries = 0
        max_retries = 3

        while retries < max_retries:
            try:
                if method == "GET":
                    payload_str = f"?{urlencode(payload)}" if payload else ""
                    response = self.session.request(method, f"{url}{payload_str}", headers=headers)
                elif method == "POST":
                    response = self.session.request(method, url, data=json.dumps(payload), headers=headers)
                else:
                    payload_str = f"?{urlencode(payload)}" if payload else ""
                    response = self.session.request(method, f"{url}{payload_str}", headers=headers)
                self.__request_log(response)

                if parse:
                    return self.__parse_response(response)
                else:
                    return response

            except RateLimitReachedError as e:
                log(f"[{self}][ERROR] Too many requests have been made. Waiting '{self.__request_rate_limit_timeout}' seconds")
                time.sleep(self.__request_rate_limit_timeout)
                retries += 1

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            if status_code == 429:
                raise RateLimitReachedError

            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)
            if "Orders not found" in response_json['message']:
                raise OrderNotFoundError
            if "Limit only mode" in response_json['message']:
                raise LimitModeOnlyError
            raise requests.exceptions.HTTPError(f"{status_code} : {response_json['message']}")
        else:
            return response.json()

    def __get_timestamp(self):
        return str(time.time())

    def __generate_signature(self, data):
        hash = hmac.new(base64.b64decode(self.api_secret), data.encode("ascii"), hashlib.sha256)
        return base64.b64encode(hash.digest())

    def __compose_order_type(self, order):
        order_type = "unknown"
        if order["type"] == "market":
            order_type = "market"
        elif order["type"] == "limit":
            if "stop" in order:
                if order["stop_price"] == order["price"]:
                    order_type = "stop_market"
                else:
                    order_type = "stop_limit"
            else:
                order_type = "limit"
        return order_type

    def __compose_order_status(self, order):
        order_status = "unknown"
        if order["status"] == "open" or order["status"] == "pending" or order["status"] == "active":
            if float(order["filled_size"]) > 0:
                order_status = "partially_filled"
            else:
                order_status = "open"
        elif order["status"] == "rejected":
            order_status = "cancelled"
        elif order["status"] == "done":
            order_status = "filled"
        return order_status

    def __log_order(self, order_id, symbol, size, side, order_type, status, execution_price):
        try:
            self.strategy.log_trade(order_id, symbol, size, side, order_type, status, execution_price)
        except Exception as e:
            log(f"[{self}][ERROR] Unable to log order : {type(e).__name__} - {e}\n{format_traceback(e.__traceback__)}")

    def __request_log(self, response):
        try:
            file_path = "coinbase_requests.log"
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
