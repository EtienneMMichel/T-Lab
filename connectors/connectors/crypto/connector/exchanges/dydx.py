from connectors.crypto.connector.common.connector import CryptoConnector
from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.threading.Threads import StoppableThread
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
import pandas as pd
import websocket
import threading
import requests
import datetime
import hashlib
import urllib
import select
import time
import hmac
import json
import sys
import io

class DyDx(CryptoConnector):
    PLATFORM_ID = 10
    PLATFORM_NAME = "DyDx"

    def __init__(self, user, use_websocket=False):
        self.user = user
        self.testing = user.testnet
        self.session = requests.Session()
        if self.testing:
            self.base_url = "https://api.stage.dydx.exchange"
            self.ws_url = "https://api.stage.dydx.exchange"
        else:
            self.base_url = "https://api.dydx.exchange"
            self.ws_url = "https://api.dydx.exchange"

        self.api_key = user.get_api_key(self.PLATFORM_ID)
        self.api_secret = user.get_api_secret(self.PLATFORM_ID)

        if self.api_key is None or self.api_secret is None:
            raise InvalidApiKeyError('Api_key or Api_secret missing')

        self.future_fees = 0.004

        self.__products = {}

        self.use_websocket = use_websocket
        self.__websocket = None
        self.__reconnect_timeout = 35
        self.__receive_thread = None
        self.__reconnect_timer = None
        self.channel_callbacks = {}
        self.is_connected = False
        self.is_auth = False
        self.__websocket = websocket.WebSocket()

        if use_websocket:
            self.connect()

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

    def get_balance(self, as_usd=False):
        data = {}
        response = self.__api_get_wallet_balance()
        for asset_balances in response:
            if float(asset_balances["balance"]) > 0:
                symbol = str(asset_balances['asset_symbol'])
                if not as_usd or is_fiat(symbol) or is_stable_coin(symbol):
                    spot_price = 1
                else:
                    if spot_price := self.__api_get_ticker(symbol+"USDT"):
                        spot_price = float(spot_price["spot_price"])
                    elif spot_price := self.__api_get_ticker(symbol+"_"+"USDT"):
                        spot_price = float(spot_price["spot_price"])
                    else:
                        spot_price = 1
                data[symbol] = {}
                data[symbol]["symbol"] = symbol
                data[symbol]["balance"] = float(asset_balances["balance"])*spot_price
                data[symbol]["available_balance"] = float(asset_balances["available_balance"])*spot_price
                data[symbol]["currency"] = "USDT" if as_usd else symbol
                data[symbol]["currency"] = "USDT" if as_usd else symbol
        return data

    def get_profit(self):
        data = {}
        total_unrealized = 0
        total_premium = 0

        # Get wallet history
        history = self.get_wallet_transactions()
        history = pd.DataFrame(history)

        if history.empty:
            total_deposit = 0
            total_withdraw = 0
            total_trading_credits = 0
            sum_funding = 0
            total_liquidation_fees = 0
            total_trading_fees = 0
            balance = 0
        else:
            balances = self.get_balance(as_usd=False)
            balance = balances['USDT']['balance']

            history['date'] = pd.to_datetime(history.date)
            history['date'] = history['date'].dt.strftime('%Y-%m-%d')
            history['date'] = pd.to_datetime(history.date).dt.date

            # Compute sum of deposits and withdrawals
            total_deposit = abs(history[history.transaction_type == 'deposit']['amount'].sum())
            total_withdraw = abs(history[history.transaction_type == 'withdrawal']['amount'].sum())
            total_trading_credits = abs(history[history.transaction_type == 'trading_credits']['amount'].sum()) - abs(history[history.transaction_type == 'trading_credits_paid']['amount'].sum()) - abs(history[history.transaction_type == 'trading_credits_reverted']['amount'].sum())
            sum_funding = history[history.transaction_type == 'funding']['amount'].sum()

            # Compute sum of fees
            total_liquidation_fees = abs(history[history.transaction_type == 'liquidation_fee']['amount'].sum())
            total_trading_fees = abs(history[history.transaction_type == 'trading_fee_credits']['amount'].sum())

        # Loop open positions
        positions = self.get_positions()
        for symbol, position in positions.items():
            unrealized_pl = 0

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

        total_balance = balance + total_unrealized + total_premium
        total_fees = total_liquidation_fees + total_trading_fees
        total_profit = total_balance - (total_deposit - total_withdraw) - total_fees - total_trading_credits + sum_funding

        data['USDT'] = {}
        data['USDT']["total_deposit"] = total_deposit
        data['USDT']["total_withdraw"] = total_withdraw
        data['USDT']["trading_credit"] = total_trading_credits
        data['USDT']["total_fees"] = total_fees
        data['USDT']["total_balance"] = total_balance
        data['USDT']["total_profit"] = total_profit
        return data

    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}
            products = self.__api_get_products()
            for product in products:

                generated_symbol = self.__compose_product_symbol(product)

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["id"],
                    "exchange_symbol": product["symbol"],
                }

                strike_price = product["strike_price"]
                settlement_time = product["settlement_time"]
                settlement_date = settlement_time[:10] if settlement_time else None

                if not (contract_type := self.__compose_contract_type(product)):
                    # Do not handle this product types
                    continue
                product_refactored["contract_type"] = contract_type

                product_refactored["strike_price"] = float(strike_price) if strike_price else None
                product_refactored["settlement_date"] = settlement_date
                product_refactored["settlement_time"] = settlement_time
                product_refactored["tick_size"] = float(product["tick_size"])
                product_refactored["contract_size"] = float(product["contract_value"])
                product_refactored["base_asset_id"] = product["underlying_asset"]["id"]
                product_refactored["base_asset_symbol"] = product["underlying_asset"]["symbol"]
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

    def get_mark_price(self, symbol):
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
            "mark_price": float(response["mark_price"]),
        }
        return data

    def get_positions(self):
        if not self.__products:
            self.get_products()
        data = {}
        response = self.__api_get_positions()
        for position in response:
            symbol = self.__compose_product_symbol(position["product"])
            data[symbol] = {
                "symbol": symbol,
                "exchange_id": position["product_id"],
                "exchange_symbol": position["product_symbol"],
                "size": float(position["size"]),
                "entry_price": float(position["entry_price"]),
                "contract_type": self.__compose_contract_type(position["product"]),
                "base_asset_id": position["product"]["underlying_asset"]["id"],
                "base_asset_symbol": position["product"]["underlying_asset"]["symbol"],
                "quote_asset_id": position["product"]["quoting_asset"]["id"],
                "quote_asset_symbol": position["product"]["quoting_asset"]["symbol"],
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

    def place_market_order(self, symbol, size, side, time_in_force="gtc", wait=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="market_order", time_in_force=time_in_force)

        data = {
            "id": response["id"],
            "type": "market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": response["size"],
            "filled_size": response["size"] - response["unfilled_size"],
            "unfilled_size": response["unfilled_size"],
            "average_price": float(response["average_fill_price"]),
            "limit_price": None,
            "stop_price": None,
            "time_in_force": response["time_in_force"],
        }

        return data

    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force)

        started_at = time.time()

        data = {
            "id": response["id"],
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = response["id"]
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
                order = self.__api_get_order_state(data["id"])

                data["status"] = self.__compose_order_status(order)
                data["symbol"] = symbol
                data["exchange_id"] = order["product_id"]
                data["exchange_symbol"] = order["product_symbol"]
                data["side"] = order["side"]
                data["size"] = order["size"]
                data["filled_size"] = order["size"] - order["unfilled_size"]
                data["unfilled_size"] = order["unfilled_size"]
                data["average_price"] = float(order["average_fill_price"]) if order["average_fill_price"] else None
                data["limit_price"] = float(order["limit_price"])
                data["stop_price"] = None
                data["time_in_force"] = order["time_in_force"]

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
            data["size"] = response["size"]
            data["filled_size"] = response["size"] - response["unfilled_size"]
            data["unfilled_size"] = response["unfilled_size"]
            data["average_price"] = float(response["average_fill_price"]) if response["average_fill_price"] else None
            data["limit_price"] = float(response["limit_price"])
            data["stop_price"] = None
            data["time_in_force"] = response["time_in_force"]

        return data

    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc"):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force)

        data = {
            "id": response["id"],
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": response["size"],
            "filled_size": response["size"] - response["unfilled_size"],
            "unfilled_size": response["unfilled_size"],
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": None,
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
        }

        return data

    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc"):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force)

        data = {
            "id": response["id"],
            "type": "stop_limit",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": response["size"],
            "filled_size": response["size"] - response["unfilled_size"],
            "unfilled_size": response["unfilled_size"],
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": float(response["limit_price"]),
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
        }

        return data

    def get_active_orders(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = []
        response = self.__api_get_active_orders()
        for order in response:

            if exchange_id == order["product_id"]:
                active_order = {
                    "id": order["id"],
                    "type": self.__compose_order_type(order),
                    "status": self.__compose_order_status(order),
                    "symbol": symbol,
                    "exchange_id": order["product_id"],
                    "exchange_symbol": order["product_symbol"],
                    "side": order["side"],
                    "size": order["size"],
                    "filled_size": order["size"] - order["unfilled_size"],
                    "unfilled_size": order["unfilled_size"],
                    "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else None,
                    "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                    "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                    "time_in_force": order["time_in_force"],
                }
                data.append(active_order)

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
            "id": response["id"],
            "type": self.__compose_order_type(response),
            "status": "cancelled" if response["size"] == response["unfilled_size"] else "partially_filled",
            "symbol": symbol,
            "exchange_id": response["product_id"],
            "exchange_symbol": response["product_symbol"],
            "side": response["side"],
            "size": response["size"],
            "filled_size": response["size"] - response["unfilled_size"],
            "unfilled_size": response["unfilled_size"],
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": float(response["limit_price"]) if response["limit_price"] else None,
            "stop_price": float(response["stop_price"]) if response["stop_price"] else None,
            "time_in_force": response["time_in_force"],
        }

        return data

    def get_order_history(self):
        if not self.__products:
            self.get_products()

        response = self.__api_get_order_history()

        data = []
        for order in response:

            symbol = next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == order["product_id"]), None)

            order_refactored = {
                "created_at": order["created_at"],
                "updated_at": order["updated_at"],
                "id": order["id"],
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol,
                "exchange_id": order["product_id"],
                "exchange_symbol": order["product_symbol"],
                "side": order["side"],
                "size": order["size"],
                "filled_size": float(order["size"]) - float(order["unfilled_size"]),
                "unfilled_size": order["unfilled_size"],
                "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else None,
                "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                "time_in_force": order["time_in_force"],
                "source": order["meta_data"]["source"] if "source" in order["meta_data"] else None,
                "is_liquidation": True if self.__compose_order_type(order) == "liquidation" else False,
            }
            data.append(order_refactored)
        return data

    def get_wallet_transactions(self):
        response_df = self.__api_get_wallet_transactions()
        response_df = response_df.rename(columns=str.lower)
        response_df.columns = response_df.columns.str.replace(' ','_')
        response_df.columns = response_df.columns.str.replace('/','_')
        response_df['date'] = pd.to_datetime(response_df.date)
        response_df['date'] = response_df['date'].dt.strftime('%Y-%m-%d')
        #response_df['date'] = response_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        response_df['date'] = pd.to_datetime(response_df.date).dt.date
        response = json.loads(response_df.to_json(orient="records"))
        return response

    def change_order_leverage(self, product_id):
        if not self.__products:
            self.get_products()
        if product_id not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_id}'")
        else:
            exchange_id = self.__products[product_id]["exchange_id"]
            exchange_symbol = self.__products[product_id]["exchange_symbol"]
        return self.api_change_order_leverage(exchange_id)

    def get_order_leverage(self, product_id):
        if not self.__products:
            self.get_products()
        if product_id not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_id}'")
        else:
            exchange_id = self.__products[product_id]["exchange_id"]
            exchange_symbol = self.__products[product_id]["exchange_symbol"]
        return self.api_get_order_leverage(exchange_id)

    def get_deposit_withdraws(self):
        history = self.get_wallet_transactions()
        history = pd.DataFrame(history)


        deposits = history[history.transaction_type == 'deposit'].copy()
        withdraws = history[history.transaction_type == 'withdrawal'].copy()

        data = []
        for symbol in deposits['asset_symbol']:
            print('symbol',symbol)
            depos = {
                "type": 'deposit',
                "symbol": symbol,
                "balance": deposits.loc[deposits["asset_symbol"] == symbol, 'balance'].values[0],
                "amount": deposits.loc[deposits["asset_symbol"] == symbol, 'amount'].values[0],
                "date": deposits.loc[deposits["asset_symbol"] == symbol, 'date'].values[0],
                "operation_id": deposits.loc[deposits["asset_symbol"] == symbol, 'id'].values[0]
            }
            data.append(depos)

        for symbol in withdraws['asset_symbol']:
            depos = {
                "type": 'deposit',
                "symbol": symbol,
                "balance": withdraws.loc[withdraws["asset_symbol"] == symbol, 'balance'].values[0],
                "amount": withdraws.loc[withdraws["asset_symbol"] == symbol, 'amount'].values[0],
                "date": withdraws.loc[withdraws["asset_symbol"] == symbol, 'date'].values[0],
                "operation_id": withdraws.loc[withdraws["asset_symbol"] == symbol, 'id'].values[0]
            }
            data.append(depos)

        return data

#################
### Websocket ###
#################

    def connect(self):
        log("[CONNECT] Connecting")
        # Connect to websocket server
        self.__websocket.connect(self.ws_url)

        # Launch reception thread
        self.__receive_thread = StoppableThread(name="on_message", target=self.__on_message)
        self.__receive_thread.start()

        # Tell server to enable heartbeat
        self.__heartbeat()

        # Enable timed reconnection to detect connection drops
        self.__reconnect_timer = threading.Timer(self.__reconnect_timeout, self.__reconnect)
        self.__reconnect_timer.start()
        log("[CONNECT] Connected")
        self.is_connected = True

    def disconnect(self):
        log("[DISCONNECT] Disconnecting")

        # Stop reception thread
        self.__receive_thread.stop()
        self.__receive_thread.join()
        self.__receive_thread = None

        # Cancel timed reconnection
        self.__reconnect_timer.cancel()
        self.__reconnect_timer = None

        # Close websocket connection
        self.__websocket.close()
        log("[DISCONNECT] Disconnected")
        self.is_connected = False

#######################
### Public channels ###
#######################

    def subscribe_ticker(self, symbol, callback=None):
        self.__subscribe("ticker", symbol, auth=False, callback=callback)

    def unsubscribe_ticker(self, symbol):
        self.__unsubscribe("ticker", symbol)

    def subscribe_orderbook(self, symbol, callback=None):
        self.__subscribe("l2_orderbook", symbol, auth=False, callback=callback)

    def unsubscribe_orderbook(self, symbol):
        self.__unsubscribe("l2_orderbook", symbol)

    def subscribe_trades(self, symbol, callback=None):
        self.__subscribe("all_trades", symbol, auth=False, callback=callback)

    def unsubscribe_trades(self, symbol):
        self.__unsubscribe("all_trades", symbol)

    def subscribe_mark_price(self, symbol, callback=None):
        self.__subscribe("mark_price", f"MARK:{symbol}", auth=False, callback=callback)

    def unsubscribe_mark_price(self, symbol):
        self.__unsubscribe("mark_price", f"MARK:{symbol}")

    def subscribe_spot_price(self, symbol, callback=None):
        self.__subscribe("spot_price", symbol, auth=False, callback=callback)

    def unsubscribe_spot_price(self, symbol):
        self.__unsubscribe("spot_price", symbol)

    def subscribe_funding_rate(self, symbol, callback=None):
        self.__subscribe("funding_rate", symbol, auth=False, callback=callback)

    def unsubscribe_funding_rate(self, symbol):
        self.__unsubscribe("funding_rate", symbol)

    def subscribe_product_updates(self, callback=None):
        self.__subscribe("product_updates", None, auth=False, callback=callback)

    def unsubscribe_product_updates(self):
        self.__unsubscribe("product_updates", None)

    def subscribe_announcements(self, callback=None):
        self.__subscribe("announcements", None, auth=False, callback=callback)

    def unsubscribe_announcements(self):
        self.__unsubscribe("announcements", None)

    def subscribe_candlestick(self, resolution, callback=None):
        self.__subscribe(f"candlestick_{resolution}", None, auth=False, callback=callback)

    def unsubscribe_candlestick(self, resolution):
        self.__unsubscribe(f"candlestick_{resolution}", None)

#########################
### Privates channels ###
#########################

    def subscribe_margins(self, symbol, callback=None):
        self.__subscribe("margins", symbol, auth=True, callback=callback)

    def unsubscribe_margins(self, symbol):
        self.__unsubscribe("margins", symbol)

    def subscribe_positions(self, symbol, callback=None):
        self.__subscribe("positions", symbol, auth=True, callback=callback)

    def unsubscribe_positions(self, symbol):
        self.__unsubscribe("positions", symbol)

    def subscribe_orders(self, symbol, callback=None):
        self.__subscribe("orders", symbol, auth=True, callback=callback)

    def unsubscribe_orders(self, symbol):
        self.__unsubscribe("orders", symbol)

    def subscribe_user_trades(self, symbol, callback=None):
        self.__subscribe("user_trades", symbol, auth=True, callback=callback)

    def unsubscribe_user_trades(self, symbol):
        self.__unsubscribe("user_trades", symbol)

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

    def __api_get_products(self):
        response = self.__request("GET", "/v2/products", auth=True)
        return response

    def __api_get_orderbook(self, symbol):
        response = self.__request("GET", "/v2/l2orderbook/{}".format(symbol), auth=True)
        return response

    def __api_get_ticker(self, symbol):
        response = self.__request("GET", "/v2/tickers/{}".format(symbol), auth=True)
        return response

    def __api_get_order_history(self):
        response = self.__request("GET", "/v2/orders/history", query={
            "page_size": str('10000')
        }, auth=True)
        return response

    def __api_get_wallet_transactions(self):
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
        stop_trigger_method='spot_price'):

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

    def api_change_order_leverage(self, product_id):
        response = self.__request("POST", "/products/{product_id}/orders/leverage", {
           "product_id": str(product_id),
        }, auth=True)
        return response

    def api_get_order_leverage(self, product_id):
        response = self.__request("GET", "/products/{product_id}/orders/leverage", {
           "product_id": str(product_id),
        }, auth=True)
        return response

    def __request(self, method, path, payload=None, query=None, auth=False, base_url=None, headers=None, retry=3, parse="json"):
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

        while retry > 0:
            try:
                response = self.session.request(method, url, data=body_string, params=query, timeout=(3, 27), headers=headers)
                parsed_response = self.__parse_response(response, parse)
                break
            except InternalServerError:
                # If placing order
                if method == "POST" and path == "/v2/orders":
                    log("ERROR 500 on place_order")
                    # Break if order placed
                    if self.__check_order_placed(payload["product_id"], payload["side"], payload["size"], payload["order_type"]):
                        break
                retry -= 1

        if retry == 0:
            raise InternalServerError

        return parsed_response

    def __parse_response(self, response, parse="json"):
        status_code = response.status_code
        if status_code != 200:
            if status_code == 429:
                raise requests.exceptions.HTTPError("{} : Too Many Requests, need to wait {} ms".format(status_code, response.headers["X-RATE-LIMIT-RESET"]))
            if status_code == 401:
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
                    #TODO: Test this error this IP restriction
                    elif response_json["error"]["code"] == "unauthorized":
                        raise UnauthorizedError
                    # TODO: Temporary ignore this error
                    elif response_json["error"]["code"] == "leverage_limit_exceeded":
                        response_json["margin"] = 0
                        return response_json
                    raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["error"]))
                else:
                    raise requests.exceptions.HTTPError(status_code)
        else:
            if parse == "json":
                response_json = response.json()
                if response_json['success']:
                    if 'result' in response_json:
                        return response_json['result']
                elif 'error' in response_json:
                    raise requests.exceptions.HTTPError(response_json['error'])
                else:
                    raise requests.exceptions.HTTPError()
            elif parse == "csv":
                response = pd.read_csv(io.StringIO(response.content.decode('utf-8')), sep=",")
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
        base_asset_symbol = product["underlying_asset"]["symbol"]
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

#################
### Websocket ###
#################

    def __subscribe(self, channel, symbol, auth=False, callback=None):
        #if not self.use_websocket:
        #    raise Exception("Can't subscribe, websockets are disabled for connector")

        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if not self.is_connected:
            self.connect()
        log(f"[SUBSCRIBE] Suscribing to '{channel}'")
        if auth and not self.is_auth:
            self.__auth()

        self.__websocket.send(json.dumps({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": channel,
                        "symbols": [exchange_symbol],
                    }
                ]
            }
        }))
        self.channel_callbacks[channel] = callback
        log(f"[SUBSCRIBE] Suscribed to '{channel}'")

    def __unsubscribe(self, channel, symbol):
        log(f"[UNSUBSCRIBE] Unsuscribing from '{channel}'")
        self.__websocket.send(json.dumps({
            "type": "unsubscribe",
            "payload": {
                "channels": [{
                    "name": channel,
                    "symbols": [symbol],
                }]
            }
        }))
        if channel in self.channel_callbacks:
            del self.channel_callbacks
        log(f"[UNSUBSCRIBE] Unsubscribed from '{channel}'")

    def __reset_timer(self):
        self.__reconnect_timer.cancel()
        self.__reconnect_timer = threading.Timer(self.__reconnect_timeout, self.__reconnect)
        self.__reconnect_timer.start()

    def __reconnect(self):
        log("[RECONNECT] Reconnecting")
        self.disconnect()
        self.connect()
        log("[RECONNECT] Reconnected")

    def __on_message(self):
        while not self.__receive_thread.is_stopped():
            has_data = select.select([self.__websocket], [], [], 1)
            if has_data[0]:
                message = self.__websocket.recv()
                log(f"[MESSAGE] {message}")

                data = json.loads(message)

                if "type" in data:
                    event = data["type"]

                    if event == "heartbeat":
                        self.__reset_timer()
                    elif event == "orders":

                        #TODO: Temporary ignore snapshot messages
                        if "action" in data and data["action"] == "snapshot":
                            continue

                        symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["product_symbol"]][0]

                        data_refactored = {
                            "id": data["id"],
                            "type": self.__compose_order_type(data),
                            "status": self.__compose_order_status(data),
                            "symbol": symbol,
                            "exchange_id": data["product_id"],
                            "exchange_symbol": data["product_symbol"],
                            "side": data["side"],
                            "size": data["size"],
                            "filled_size": data["size"] - data["unfilled_size"],
                            "unfilled_size": data["unfilled_size"],
                            "average_price": float(data["average_fill_price"]) if data["average_fill_price"] else None,
                            "limit_price": float(data["limit_price"]) if data["limit_price"] else None,
                            "stop_price": float(data["stop_price"]) if data["stop_price"] else None,
                            "time_in_force": data["time_in_force"],
                        }

                    else:
                        data_refactored = data

                    if event in self.channel_callbacks and self.channel_callbacks[event]:
                        self.channel_callbacks[event](self, data_refactored)

    def __heartbeat(self, enable=True):
        if enable:
            log("Enable heartbeat")
            type = "enable_heartbeat"
        else:
            log("Disable heartbeat")
            type = "disable_heartbeat"
        self.__websocket.send(json.dumps({
            "type": type
        }))

    def __auth(self):
        log("[AUTH] Authenticating")
        method = 'GET'
        timestamp = self.__get_timestamp()
        path = '/live'
        data = method + timestamp + path

        self.__websocket.send(json.dumps({
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": self.__generate_signature(data),
                "timestamp": timestamp
            }
        }))
        self.is_auth = True
        log("[AUTH] Authenticated")

    def __unauth(self):
        log("[UNAUTH] Unauthenticating")
        self.__websocket.send(json.dumps({
            "type": "unauth",
            "payload": {}
        }))
        self.is_auth = False
        log("[UNAUTH] Unauthenticated")
