from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.threading.Threads import format_traceback
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
from connectors.crypto.singleton import Singleton
from decimal import Decimal
from connectors.crypto.connector.forex import Forex
import requests
import datetime
import hashlib
import base64
import hmac
import time
import json

class KkForex(metaclass=Singleton):
    def __init__(self, database=None):
        self.database = database
        self.connector = Kraken("", "")

    def get_rate(self, base_currency, dest_currency, date=None):
        usd_stable_coins = ["USD", "USDT", "USDC", "BUSD"]
        rate = None
        if base_currency == dest_currency:
            rate = 1
        elif (base_currency == "EUR" and dest_currency in usd_stable_coins) or (base_currency in usd_stable_coins and dest_currency == "EUR"):
            pair = "USDT_EUR"
            rate = self.connector.get_pair_price(pair, date)
            if base_currency == "EUR":
                rate = 1/rate
        elif (base_currency == "CHF" and dest_currency in usd_stable_coins) or (base_currency in usd_stable_coins and dest_currency == "CHF"):
            pair = "USDT_CHF"
            rate = self.connector.get_pair_price(pair, date)
            if base_currency == "CHF":
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

#class Kraken(CryptoConnector):
class Kraken:

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
            self.base_url = ""
            self.ws_url = ""
        else:
            self.base_url = "https://api.kraken.com"
            self.ws_url = ""

        self.__products = {}
        self.__assets = {}

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

    def get_pair_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
        price = None
        if not date:
            price = self.get_mark_price(symbol)["mark_price"]
        else:
            candle = self.__api_get_recent_trades(exchange_symbol, timestamp=date.timestamp(), count=1)
            if len(candle[exchange_symbol]) > 0:
                price = float(candle[exchange_symbol][0][0])

        return price

    def get_balance(self, as_base_currency=False, as_eur=False):
        if not self.__assets:
            self.get_assets()
        if not self.__products:
            self.get_products()

        if as_base_currency:
            rate = KkForex(self.database).get_rate('USD', self.base_currency)
        else:
            rate = 1
        data = {}
        balances = self._api_get_extended_balance()
        print('balances',balances)
        for id, asset_balance in balances.items():
            if float(asset_balance["balance"]) == 0:
                continue
            print(id)
            if '.F' in id:
                id = id.split('.F')[0]
            symbols = [asset["symbol"] for asset in self.__assets.values() if asset["id"] == id]
            if(len(symbols) == 0):
                continue
            symbol = symbols[0]

            if symbol == 'FEE':
                continue

            if as_base_currency:
                usd_price = self.__get_symbol_usd_price(symbol)
                balance = float(asset_balance["balance"])*usd_price*rate
                available_balance = (float(asset_balance["balance"]) - float(asset_balance["hold_trade"]))*usd_price*rate
            else:
                balance = float(asset_balance["balance"])
                available_balance = (float(asset_balance["balance"]) - float(asset_balance["hold_trade"]))

            data[symbol] = {
                "symbol": symbol,
                "balance": balance,
                "available_balance": available_balance,
                "currency": self.base_currency if as_base_currency else symbol,
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

    def get_assets(self, reload=False):
        if not self.__assets or reload:
            self.__assets = {}
            assets = self.__api_get_asset_info()
            for symbol, asset in assets.items():
                self.__assets[asset["altname"]] = {
                    "id": symbol,
                    "symbol": asset["altname"],
                }
        return self.__assets

    def get_products(self, reload=False):
        if not self.__products or reload:
            self.__products = {}
            products = self.__api_get_tradable_asset_pairs()
            for symbol, product in products.items():
                base_asset_symbol = product["wsname"].split("/")[0]
                quote_asset_symbol = product["wsname"].split("/")[1]
                generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": symbol,
                    "exchange_symbol": symbol,
                    "contract_type": "spot",
                    "contract_size": 1,
                    "strike_price": None,
                    "settlement_date": None,
                    "settlement_time": None,
                    "duration": None,
                    "precision": {
                        "amount": product["pair_decimals"],
                        "price": product["cost_decimals"],
                        "notional": product["lot_decimals"],
                    },
                    "limits": {
                        "amount": {
                            "min": product["ordermin"],
                            "max": None},
                        "price": {
                            "min": product["tick_size"],
                            "max": None},
                        "notional": {
                            "min": product["costmin"],
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
        if not self.__assets:
            self.get_assets()

        if symbol not in self.__products and symbol not in self.__assets:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif symbol in self.__products:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]
            response = self.__api_get_ticker_information(exchange_id)

            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "mark_price": (float(response[exchange_symbol]["a"][0])+float(response[exchange_symbol]["b"][0]))/2,
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

    def get_positions(self):
        rate = KkForex(self.database).get_rate('USD', self.base_currency)
        data = {}
        balances = self.get_balance()
        print('balances ICI',balances)
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

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)

        response = self.__api_place_order(exchange_symbol, side, size, order_type="market_order", time_in_force=time_in_force, reduce_only=reduce_only)
        order_id = response["txid"][0]

        time.sleep(1)
        data = self.get_order_state(symbol, order_id)

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], data["average_price"])

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

        response = self.__api_place_order(exchange_symbol, side, size, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force, reduce_only=reduce_only)

        started_at = time.time()

        order_id = response["txid"][0]

        data = {
            "id": str(order_id),
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:

            order_placed = False
            elapsed_time = time.time() - started_at
            try:
                while elapsed_time < timeout:
                    active_orders = self.get_active_orders(symbol)

                    # Order placed
                    if not any(order["txid"] == order_id for order in active_orders):
                        order_placed = True
                        break

                    elapsed_time = time.time() - started_at
                    time.sleep(0.1)
            except requests.exceptions.ReadTimeout:
                pass

            if not order_placed:
                data = self.cancel_order(symbol, order_id)
            else:
                data = self.get_order_state(symbol, order_id)

        # Doesn't wait for order to be placed
        else:
            time.sleep(0.5)
            data = self.get_order_state(symbol, order_id)

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

        response = self.__api_place_order(exchange_symbol, side, size, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)
        order_id = response["txid"][0]

        time.sleep(0.1)
        data = self.get_order_state(symbol, order_id)

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], data["average_price"])

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

        response = self.__api_place_order(exchange_symbol, side, size, order_type="stop_limit_order", limit_price=limit_price, stop_price=stop_price, time_in_force=time_in_force, reduce_only=reduce_only)
        order_id = response["txid"][0]

        time.sleep(0.1)
        data = self.get_order_state(symbol, order_id)

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], data["average_price"])

        return data

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.00075):
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

        response = self.__api_get_order_state(order_id)

        order = response[order_id]
        pair = order["descr"]["pair"]
        data = {
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
            rate = KkForex(self.database).get_rate('USD', self.base_currency, date)
            usd_amount = self.__get_symbol_usd_price(operation["asset"], date)
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

        start_timestamp = int(start_date.timestamp()) if start_date else None
        stop_timestamp = int(end_date.timestamp()) if end_date else None

        deposits = {}
        response = self.__api_get_ledgers_info(start_timestamp, stop_timestamp, types="deposit")
        deposits.update(response["ledger"])

        offset = 0
        while len(response["ledger"]) >= 50:
            offset += 50
            response = self.__api_get_ledgers_info(start_timestamp, stop_timestamp, types="deposit", offset=offset)
            deposits.update(response["ledger"])

        for operation in deposits.values():
            if operation["asset"] == 'KFEE':
                continue
            date = datetime.datetime.fromtimestamp(operation["time"])
            rate = KkForex(self.database).get_rate('USD', self.base_currency, date)
            usd_amount = self.__get_symbol_usd_price(operation["asset"], date)
            base_currency_amount = float(operation["amount"])*rate*usd_amount
            data.append({
                "date": date,
                "type": "deposit",
                "source_symbol": operation["asset"],
                "source_amount": abs(float(operation["amount"])),
                "destination_symbol": operation["asset"],
                "destination_amount": abs(float(operation["amount"])),
                "fees": float(operation["fee"]),
                "operation_id": operation["refid"],
                "wallet_address": None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
            })

        withdraws = {}
        response = self.__api_get_ledgers_info(start_timestamp, stop_timestamp, types="withdrawal")
        withdraws.update(response["ledger"])

        offset = 0
        while len(response["ledger"]) >= 50:
            offset += 50
            response = self.__api_get_ledgers_info(start_timestamp, stop_timestamp, types="withdrawal", offset=offset)
            withdraws.update(response["ledger"])

        for operation in withdraws.values():
            date = datetime.datetime.fromtimestamp(operation["time"])
            rate = KkForex(self.database).get_rate('USD', self.base_currency, date)
            usd_amount = self.__get_symbol_usd_price(operation["asset"], date)
            base_currency_amount = float(operation["amount"])*rate*usd_amount
            data.append({
                "date": date,
                "type": "withdraw",
                "source_symbol": operation["asset"],
                "source_amount": abs(float(operation["amount"])),
                "destination_symbol": operation["asset"],
                "destination_amount": abs(float(operation["amount"])),
                "fees": float(operation["fee"]),
                "operation_id": operation["refid"],
                "wallet_address": None,
                "base_currency_amount": base_currency_amount,
                "base_currency_symbol": self.base_currency,
            })

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
            resolution = 1
        elif timespan <= datetime.timedelta(days=1):
            resolution = 60
        elif timespan <= datetime.timedelta(weeks=1):
            resolution = 1440
        else:
            resolution = 10080

        start_timestamp = round(datetime.datetime.timestamp(start_date))*1000

        response = self.__api_get_candle_history(exchange_symbol, start_timestamp, resolution)

        return response

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
        response = self.__api_get_candle_history(exchange_symbol, start_timestamp, resolution)

        data = []
        candle_number = len(response[exchange_symbol])
        for index in range(0, candle_number):
            data.append({
                "symbol": symbol,
                "open": float(response[exchange_symbol][index][1]),
                "close": float(response[exchange_symbol][index][4]),
                "low": float(response[exchange_symbol][index][3]),
                "high": float(response[exchange_symbol][index][2]),
                "volume": float(response[exchange_symbol][index][6]),
            })

        return data

########################
### Privates methods ###
########################

################
### REST API ###
################

    def _api_get_account_balance(self):
        response = self.__request("POST", f"/0/private/Balance", auth=True)
        return response

    def _api_get_extended_balance(self):
        response = self.__request("POST", f"/0/private/BalanceEx", auth=True)
        return response

    def __api_get_asset_info(self):
        response = self.__request("GET", f"/0/public/Assets")
        return response

    def __api_get_tradable_asset_pairs(self):
        response = self.__request("GET", f"/0/public/AssetPairs")
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

    def __api_place_order(self, symbol, side, size, order_type, limit_price=None, stop_price=None, time_in_force="GTC", reduce_only=False):
        payload = {
            "pair": symbol,
            "type": side,
            "volume": str(size),
        }

        if order_type == "market_order":
            payload["ordertype"] = "market"
        elif order_type == "limit_order":
            payload["ordertype"] = "limit"
            payload["price"] = str(limit_price)
        elif order_type == "stop_market_order":
            #payload["ordertype"] = "market_order"
            #payload["stop_price"] = str(stop_price)
            #payload["stop_order_type"] = "stop_loss_order"
            #payload["stop_trigger_method"] = stop_trigger_method
            return
        elif order_type == "stop_limit_order":
            #payload["ordertype"] = "limit_order"
            #payload["limit_price"] = str(limit_price)
            #payload["stop_price"] = str(stop_price)
            #payload["stop_order_type"] = "stop_loss_order"
            #payload["stop_trigger_method"] = stop_trigger_method
            return

        if time_in_force:
            payload["time_in_force"] = time_in_force.upper()

        if reduce_only:
            payload["reduce_only"] = "true"

        response = self.__request("POST", "/0/private/AddOrder", payload=payload, auth=True)

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
        response = self.__request("POST", "/0/private/QueryOrders", {
           "txid": str(order_id),
        }, auth=True)
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

    def __api_get_ledgers_info(self, start_timestamp=None, stop_timestamp=None, types=None, offset=0):
        payload = {}
        if start_timestamp:
            payload["start"] = start_timestamp
        if stop_timestamp:
            payload["end"] = stop_timestamp
        if types:
            payload["type"] = types
        if offset:
            payload["ofs"] = offset
        response = self.__request("POST", "/0/private/Ledgers", payload=payload, auth=True)
        return response

    def __api_get_candle_history(self, symbol, start_date, resolution):
        response = self.__request("GET", f"/0/public/OHLC", payload={
            "pair": symbol,
            "since": start_date,
            "interval": resolution,
        })
        return response

    def __get_symbol_usd_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        price = None
        convs = ["USD", "USDT", "USDC", "EUR"]

        if is_fiat(symbol):
            price = KkForex(self.database).get_rate('USD', self.base_currency, date)
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
                    candle = self.__api_get_recent_trades(f"{symbol}_{conv}", timestamp=date.timestamp(), count=1)
                    if len(candle[f"{symbol}_{conv}"]) > 0:
                        price = candle[f"{symbol}_{conv}"][0][0]
                        break

        return price

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        headers["Content-Type"] = "application/x-www-form-urlencoded"

        if auth:
            payload["nonce"] = self.__get_timestamp()
            signature = self.__generate_signature(path, payload)
            headers["API-Key"] = self.api_key
            headers["API-Sign"] = signature

        if method == "GET":
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", headers=headers)
        else:
            response = self.session.request(method, f"{url}", data=payload, headers=headers)

        self.__request_log(response)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)
            raise requests.exceptions.HTTPError(f"{status_code} : {response_json['error']}")
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

            if len(response_json["error"]) > 0:
                raise requests.exceptions.HTTPError(f"{status_code} : {response_json['error'][0]}")
            else:
                return response_json["result"]

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

    def __compose_order_type(self, order):
        order_type = "unknown"
        if order["descr"]["ordertype"] == "market":
            order_type = "market"
        elif order["descr"]["ordertype"] == "limit":
            order_type = "limit"
        elif (order["descr"]["ordertype"] == "stop-loss" or
                order["descr"]["ordertype"] == "take-profit"):
            order_type = "stop_market"
        elif (order["descr"]["ordertype"] == "stop-loss-limit" or
                order["descr"]["ordertype"] == "take-profit-limit"):
            order_type = "stop_limit"
        return order_type

    def __compose_order_status(self, order):
        order_status = "unknown"
        if order["status"] == "open" or order["status"] == "pending":
            if float(order["vol_exec"]) > 0:
                order_status = "partially_filled"
            else:
                order_status = "open"
        elif order["status"] == "canceled" or order["status"] == "expired":
            order_status = "cancelled"
        elif order["status"] == "closed":
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
            file_path = "kraken_requests.log"
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