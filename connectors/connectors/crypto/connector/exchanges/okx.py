from connectors.threading.Threads import StoppableThread, format_traceback
from connectors.crypto.connector.common.connector import CryptoConnector
import connectors.crypto.connector.common.websocket_codes as op_codes
from connectors.crypto.connector.core import is_stable_coin, is_fiat
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
import time
import hmac
import json
import sys
import io

class OKX(CryptoConnector):
    PLATFORM_ID = 11
    PLATFORM_NAME = "OKX"

    def __init__(self, api_key, api_secret, testnet=False, user=None, passphrase=""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.user = user
        self.session = requests.Session()

        if self.testing:
            self.base_url = "https://www.okx.com"
            self.ws_url = "wss://testnet-socket.delta.exchange"
        else:
            self.base_url = "https://www.okx.com/"
            self.ws_url = "wss://socket.delta.exchange"
            #self.base_url = "ws://localhost:12345"
            #self.ws_url = "ws://localhost:12345"

        self.option_fees = 0.0005
        self.future_fees = 0.001

        self.__products = {}

        self.__websocket = None
        self.__heartbeat_timeout = 35
        self.__receive_thread = None
        self.__reconnect_timer = None
        self.__channel_callbacks_mutex = threading.Lock()
        self.__channel_callbacks = {}
        self.__connection_mutex = threading.Lock()
        self.__is_connected = False
        self.__is_auth = False
        self.__connection_lost = False
        self.__connection_aborted = False
        self.__reconnect_timeout = 30
        self.__abortion_datetime = None
        self.__websocket = websocket.WebSocket()
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

################
### REST API ###
################

    #TODO
    def is_ip_authorized(self):
        try:
            self.__api_get_wallet_balance()
        except UnauthorizedError:
            return False
        return True
    #TODO
    def has_read_permission(self):
        permission = False
        api_keys = self.__api_get_api_profile()
        for api_key in api_keys:
            if api_key["api_key"] == self.api_key:
                permission = api_key["account_permission"]
        return permission
    #TODO
    def has_write_permission(self):
        permission = False
        api_keys = self.__api_get_api_profile()
        for api_key in api_keys:
            if api_key["api_key"] == self.api_key:
                permission = api_key["trade_permission"]
        return permission
    #TODO
    def has_withdraw_permission(self):
        # Always returns false since API withdrawal is not implemented on delta-exchange
        return False
    #TODO
    def has_future_authorized(self):
        return True
    #TODO
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
    #TODO
    def get_profit(self, buy_crypto_history=None):
        data = {}
        total_unrealized = 0
        total_premium = 0
        total_deposit = 0
        total_withdraw = 0

        # Get wallet history
        history = self.get_wallet_transactions()

        if len(history) == 0:
            total_trading_credits = 0
            sum_funding = 0
            total_liquidation_fees = 0
            total_trading_fees = 0
            balance = 0
        else:
            balances = self.get_balance(as_usd=False)
            balance = balances['USDT']['balance']

            deposits = [deposit for deposit in history if deposit["transaction_type"] == "deposit"]
            withdraws = [withdraw for withdraw in history if withdraw["transaction_type"] == "withdrawal"]

            for deposit in deposits:
                amount = deposit["amount"]
                asset_symbol = deposit["asset_symbol"]

                if asset_symbol != "USDT":
                    date_deposit = datetime.datetime.fromtimestamp(deposit["date"]/1000)
                    amount = amount*self.get_candle(f"{asset_symbol}_PERP_USDT", date_deposit, date_deposit + datetime.timedelta(minutes=1))["open"]
                total_deposit += amount
            for withdraw in withdraws:
                amount = withdraw["amount"]
                asset_symbol = withdraw["asset_symbol"]

                if asset_symbol != "USDT":
                    date_withdraw = datetime.datetime.fromtimestamp(withdraw["date"]/1000)
                    amount = amount*self.get_candle(f"{asset_symbol}_PERP_USDT", date_withdraw, date_withdraw + datetime.timedelta(minutes=1))["open"]
                total_withdraw -= amount

            total_trading_credits = abs(sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "trading_credits"))
            total_trading_credits_paid = abs(sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "trading_credits_paid"))
            total_trading_credits_reverted = abs(sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "trading_credits_reverted"))
            total_trading_credits = total_trading_credits - total_trading_credits_paid - total_trading_credits_reverted
            sum_funding = sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "funding")

            # Compute sum of fees
            total_liquidation_fees = abs(sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "liquidation_fee"))
            total_trading_fees = abs(sum(transaction["amount"] for transaction in history if transaction["transaction_type"] == "trading_fee_credits"))

        # Loop open positions
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

        total_balance = balance + total_unrealized + total_premium
        total_fees = total_liquidation_fees + total_trading_fees

        #total_profit = total_balance - (total_deposit - total_withdraw) - total_fees - total_trading_credits + sum_funding

        total_profit = total_balance - (total_deposit - total_withdraw) - total_trading_credits

        data['USDT'] = {}
        data['USDT']["total_deposit"] = total_deposit
        data['USDT']["total_withdraw"] = total_withdraw
        data['USDT']["trading_credit"] = total_trading_credits
        data['USDT']["total_fees"] = total_fees
        data['USDT']["total_balance"] = total_balance
        data['USDT']["total_profit"] = total_profit
        return data
    #TOTEST
    def get_products(self, reload=False):
        if not self.__products or reload:
            data = {}
            products = self.__api_get_products()
            for product in products:

                generated_symbol = self.__compose_product_symbol(product)

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["instId"],
                    "exchange_symbol": product["instId"],
                }

                strike_price = product["stk"]
                settlement_time = product["expTime"]
                settlement_date = settlement_time
                #settlement_date = settlement_time[:10] if settlement_time else None
                duration = (datetime.datetime.strptime(settlement_date, "%Y-%m-%d").date() - datetime.date.today()).days if settlement_date else None

                if not (contract_type := self.__compose_contract_type(product)):
                    # Do not handle this product types
                    continue
                product_refactored["instType"] = contract_type
                product_refactored["strike_price"] = float(strike_price) if strike_price else None
                product_refactored["settlement_date"] = settlement_date
                product_refactored["settlement_time"] = settlement_time
                product_refactored["duration"] = duration
                product_refactored["tick_size"] = float(product["tickSz"])
                product_refactored["contract_size"] = float(product["lotSz"])
                product_refactored["min_notional"] = float(product["minSz"])
                product_refactored["base_asset_id"] = product["baseCcy"]
                product_refactored["base_asset_symbol"] = product["baseCcy"]
                product_refactored["quote_asset_id"] = product["quoteCcy"]
                product_refactored["quote_asset_symbol"] = product["quoteCcy"]

                data[generated_symbol] = product_refactored
                self.__products[generated_symbol] = product_refactored

            return data
        else:
            return self.__products
    #TOTEST
    def get_product(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            return self.__products[symbol]
    #TOTEST
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

            for order in response["bids"]:
                order_refactored = {
                    "price": float(order[0]),
                    "size": order[1],
                }
                data["buy"].append(order_refactored)
            for order in response["asks"]:
                order_refactored = {
                    "price": float(order[0]),
                    "size": order[1],
                }
                data["sell"].append(order_refactored)

        return data
    #TOTEST
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
            "spot_price": float(response["last"]),
        }
        return data
    #TOTEST
    def get_mark_price(self, symbol, pricing_client=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "mark_price": None
        }

        if symbol != 'DETO_PERP_USDT' and symbol != 'DETO_USDT':
            # Get spot price if product is spot
            if self.__products[symbol]["contract_type"] == "spot":
                data["mark_price"] = self.get_spot_price(symbol)["spot_price"]
            # Otherwise get mark price
            else:
                # Try using websocket pricing server
                response = None
                if pricing_client:
                    if response := pricing_client.get_mark_price(self.PLATFORM_ID, symbol):
                        data["mark_price"] =float(response["mark_price"])
                # Otherwise make API call
                if response == None:
                    response = self.__api_get_ticker(exchange_symbol)
                    data["mark_price"] = float(response["last"])

        return data
    #TODO
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
    #TODO
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
    #TODO
    def get_positions(self):
        if not self.__products:
            self.get_products()

        data = {}
        response_positions = self.__api_get_positions()
        for position in response_positions:
            symbol = self.__compose_product_symbol(position["product"])
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
            }

        response_balance = self.__api_get_wallet_balance()
        for position in response_balance:
            if float(position["available_balance"]) == 0:
                continue

            if not is_stable_coin(position["asset_symbol"]):
                product = [product for product in self.__products.values() if product["contract_type"] == "spot" and product["base_asset_symbol"] == position['asset_symbol'] and product["quote_asset_symbol"] == "USDT"][0]
                symbol = product["symbol"]
                data[symbol] = {
                    "symbol": symbol,
                    "exchange_id": product["exchange_id"],
                    "exchange_symbol": product["exchange_symbol"],
                    "size": float(position["available_balance"]),
                    "entry_price": 0,
                    "maintenance_margin": 0,
                    "contract_type": "spot",
                    "base_asset_id": product["base_asset_id"],
                    "base_asset_symbol": product["base_asset_symbol"],
                    "quote_asset_id": product["quote_asset_id"],
                    "quote_asset_symbol": product["quote_asset_symbol"],
                }
            else:
                symbol = position["asset_symbol"]
                data[symbol] = {
                    "symbol": symbol,
                    "exchange_id": position["asset_id"],
                    "exchange_symbol": position["asset_symbol"],
                    "size": float(position["available_balance"]),
                    "entry_price": 0,
                    "maintenance_margin": 0,
                    "contract_type": "spot",
                    "base_asset_id": position["asset_id"],
                    "base_asset_symbol": position["asset_symbol"],
                    "quote_asset_id": position["asset_id"],
                    "quote_asset_symbol": position["asset_symbol"],
                }

        return data
    #TODO
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
    #TODO
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
    #TODO
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
    #TODO
    def place_market_order(self, symbol, size, side, time_in_force="gtc", wait=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_place_order(exchange_id, size, side, order_type="market_order", time_in_force=time_in_force)

        self.__log_order(symbol, size, side, "market_order")

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
            "average_price": float(response["average_fill_price"]) if "average_fill_price" in response and response["average_fill_price"] is not None else None,
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

        return data
    #TODO
    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        limit_price = self.__round_price_down(symbol, limit_price) if side == "sell" else self.__round_price_up(symbol, limit_price)

        response = self.__api_place_order(exchange_id, size, side, order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force)

        self.__log_order(symbol, size, side, "limit_order")

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
                data["average_price"] = float(order["average_fill_price"]) if order["average_fill_price"] else None
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
            data["average_price"] = float(response["average_fill_price"]) if response["average_fill_price"] else None
            data["limit_price"] = float(response["limit_price"])
            data["stop_price"] = None
            data["time_in_force"] = response["time_in_force"]
            data["commission"] = 0

        return data
    #TODO
    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc"):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        stop_price = self.__round_price_down(symbol, stop_price) if side == "sell" else self.__round_price_up(symbol, stop_price)

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force)

        self.__log_order(symbol, size, side, "stop_market_order")

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
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": None,
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
            "commission": 0,
        }

        return data
    #TODO
    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc"):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        stop_price = self.__round_price_down(symbol, limit_price) if side == "sell" else self.__round_price_up(symbol, limit_price)
        stop_price = self.__round_price_down(symbol, stop_price) if side == "sell" else self.__round_price_up(symbol, stop_price)

        response = self.__api_place_order(exchange_id, size, side, order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force)

        self.__log_order(symbol, size, side, "stop_limit_order")

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
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": float(response["limit_price"]),
            "stop_price": float(response["stop_price"]),
            "time_in_force": response["time_in_force"],
            "commission": 0,
        }

        return data
    #TODO
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
                    "id": str(order["id"]),
                    "type": self.__compose_order_type(order),
                    "status": self.__compose_order_status(order),
                    "symbol": symbol,
                    "exchange_id": order["product_id"],
                    "exchange_symbol": order["product_symbol"],
                    "side": order["side"],
                    "size": float(order["size"]),
                    "filled_size": float(order["size"]) - float(order["unfilled_size"]),
                    "unfilled_size": float(order["unfilled_size"]),
                    "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else None,
                    "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                    "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                    "time_in_force": order["time_in_force"],
                }
                data.append(active_order)

        return data
    #TODO
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
            "average_price": float(response["average_fill_price"]) if response["average_fill_price"] else None,
            "limit_price": float(response["limit_price"]) if response["limit_price"] else None,
            "stop_price": float(response["stop_price"]) if response["stop_price"] else None,
            "time_in_force": response["time_in_force"],
        }

        return data
    #TODO
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
                "average_price": float(order["average_fill_price"]) if order["average_fill_price"] else None,
                "limit_price": float(order["limit_price"]) if order["limit_price"] else None,
                "stop_price": float(order["stop_price"]) if order["stop_price"] else None,
                "time_in_force": order["time_in_force"],
                "source": order["meta_data"]["source"] if "source" in order["meta_data"] else None,
                "is_liquidation": True if self.__compose_order_type(order) == "liquidation" else False,
            }
            data.append(order_refactored)
        return data
    #TODO
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
    #TODO
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
    #TODO
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
    #TODO
    def change_order_leverage(self, product_id):
        if not self.__products:
            self.get_products()
        if product_id not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{product_id}'")
        else:
            exchange_id = self.__products[product_id]["exchange_id"]
            exchange_symbol = self.__products[product_id]["exchange_symbol"]
        return self.api_change_order_leverage(exchange_id)
    #TODO
    def get_deposit_withdraw_history(self, start_date=None, end_date=None):
        data = []

        if not start_date:
            start_date = datetime.date(datetime.date.today().year, 1, 1)
        if not end_date:
            end_date = datetime.date.today()

        min_time = datetime.datetime.min.time()
        start_date = datetime.datetime.combine(start_date, min_time)
        end_date = datetime.datetime.combine(end_date, min_time)

        history = self.get_wallet_transactions()
        history = pd.DataFrame(history)

        operations = history[(history["transaction_type"] == "deposit") | (history["transaction_type"] == "withdrawal")]

        for index, operation in operations.iterrows():
            date = datetime.datetime.fromtimestamp(operation["date"]/1000)
            if date < start_date or date > end_date:
                continue
            data.append({
                    "date": date,
                    "type": "withdraw" if operation["transaction_type"] == "withdrawal" else "deposit",
                    "source_symbol": operation["asset_symbol"],
                    "source_amount": abs(operation["amount"]),
                    "destination_symbol": operation["asset_symbol"],
                    "destination_amount": abs(operation["amount"]),
                    "fees": 0,
                    "operation_id": operation["id"],
                    "wallet_address": None,
                })
        return data
    #TODO
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

        if timespan < datetime.timedelta(hours=1):
            resolution = "1m"
        elif timespan < datetime.timedelta(days=1):
            resolution = "1h"
        elif timespan < datetime.timedelta(weeks=1):
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
            low = response[0]["low"]
            high = response[0]["high"]
            volume = 0

            for candle in response:
                low = candle["low"] if candle["low"] < low else low
                high = candle["high"] if candle["high"] > high else high
                volume += candle["volume"]

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
    #TODO
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

    def connect(self):
        self.__connection_mutex.acquire()
        try:
            if self.__is_connected:
                return

            self.__ws_log(f"Connecting to {self.ws_url}", "CONNECT")
            # Connect to websocket server
            self.__websocket.connect(self.ws_url)

            # Launch reception thread
            self.__receive_thread = StoppableThread(name="websocket_callback", target=self.__websocket_callback)
            self.__receive_thread.start()

            # Tell server to enable heartbeat
            self.__heartbeat()

            # Enable timed reconnection to detect connection drops
            self.__reconnect_timer = threading.Timer(self.__heartbeat_timeout, self.__reconnect)
            self.__reconnect_timer.start()
            self.__is_connected = True
            self.__connection_lost = False
            self.__connection_aborted = False
            self.__ws_log("Connected", "CONNECT")
        finally:
            self.__connection_mutex.release()

    def disconnect(self):
        self.__connection_mutex.acquire()
        try:
            if not self.__is_connected:
                return

            self.__ws_log("Disconnecting", "DISCONNECT")
            # Cancel timed reconnection
            if self.__reconnect_timer:
                self.__reconnect_timer.cancel()
                self.__reconnect_timer = None

            if not self.__connection_lost:

                # Stop reception thread
                self.__receive_thread.stop()
                self.__receive_thread.join()
                self.__receive_thread = None

                # Unauth
                if self.__is_auth:
                    self.__unauth()

                # Close websocket connection
                self.__websocket.close()

            self.__is_connected = False
            self.__is_auth = False
            self.__ws_log("Disconnected", "DISCONNECT")
        finally:
            self.__connection_mutex.release()

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

    def cleanup(self):
        self.__channel_callbacks_mutex.acquire()
        try:
            channel_callbacks = self.__channel_callbacks.copy()
        finally:
            self.__channel_callbacks_mutex.release()

        if len(channel_callbacks) == 0:
            self.disconnect()

#######################
### Public channels ###
#######################

    """
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
    """

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

        self.__subscribe("l2_orderbook", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_orderbook(self, symbols):
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

        self.__unsubscribe("l2_orderbook", exchange_symbols)

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

        self.__subscribe("mark_price", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_mark_price(self, symbols):
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

        self.__unsubscribe("mark_price", exchange_symbols)

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

        self.__subscribe("funding_rate", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_funding_rate(self, symbols):
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

        self.__unsubscribe("funding_rate", exchange_symbols)

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

        self.__subscribe("v2/ticker", exchange_symbols, auth=False, callback=callback)

    def unsubscribe_volume(self, symbols):
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

        self.__unsubscribe("v2/ticker", exchange_symbols)

#########################
### Privates channels ###
#########################

    """
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
    """

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

        self.__subscribe("orders", exchange_symbols, auth=True, callback=callback)

    def unsubscribe_orders(self, symbols):
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

        self.__unsubscribe("orders", exchange_symbols)

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
        response = self.__request("GET", "/api/v5/public/instruments", auth=True)
        return response

    def __api_get_orderbook(self, symbol):
        response = self.__request("GET", "/api/v5/market/books/{}".format(symbol))
        return response

    def __api_get_ticker(self, symbol):
        response = self.__request("GET", "/api/v5/market/ticker/{}".format(symbol), auth=True)
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
        response = self.__request("GET", "/api/v5/market/history-candles", query = {
           "instId": str(symbol),
           "after": str(start_date),
           "before": str(end_date),
           "bar": str(resolution),
        })
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

            signature_timestamp = timestamp

        while retry > 0:
            try:
                bef_request_timestamp = self.__get_timestamp()
                response = self.session.request(method, url, data=body_string, params=query, timeout=(3, 27), headers=headers)
                af_request_timestamp = self.__get_timestamp()
                parsed_response = self.__parse_response(response, parse)
                break
            except InternalServerError:
                # If placing order
                if method == "POST" and path == "/v2/orders":
                    log(f"[{self}] ERROR 500 on place_order")
                    # Break if order placed
                    if self.__check_order_placed(payload["product_id"], payload["side"], payload["size"], payload["order_type"]):
                        break
                retry -= 1
            except UnauthorizedError:
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
                print(f"Signature timestamp : {signature_timestamp}")
                print(f"Before request timestamp : {bef_request_timestamp}")
                print(f"After request timestamp : {af_request_timestamp}")
                print("#################################################################")
                raise UnauthorizedError

        if retry == 0:
            raise InternalServerError

        return parsed_response

    def __parse_response(self, response, parse="json"):
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
                    elif response_json["error"]["code"] == "insufficient_margin":
                        symbol = response_json["error"]["context"]["asset_symbol"]
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
        base_asset_symbol = product["settleCcy"]
        quote_asset_symbol = product["ctValCcy"]

        strike_price = product["stk"] if "stk" in product else None
        settlement_time = product["expTime"] if "expTime" in product else None
        settlement_date = settlement_time
        #settlement_date = settlement_time[:10] if settlement_time else None

        if product["instType"] == "FUTURES":
            generated_symbol = f"{base_asset_symbol}_FUT_{settlement_date}_{quote_asset_symbol}"
        elif product["instType"] == "SWAP":
            generated_symbol = f"{base_asset_symbol}_PERP_{quote_asset_symbol}"
        elif product["instType"] == "OPTION":
            generated_symbol = f"{base_asset_symbol}_CALL_{strike_price}_{settlement_date}_{quote_asset_symbol}"
        elif product["instType"] == "SPOT":
            generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"
        else:
            generated_symbol = None
        return generated_symbol

    def __round_price_down(self, product_symbol, price):
        # Get products if not done yet
        if not self.__products:
            self.get_products()

        if product_symbol in self.__products:
            tick_size = self.__products[product_symbol]["tick_size"]
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(tick_size)))
            return rounded_price
        return price

    def __round_price_up(self, product_symbol, price):
        # Get products if not done yet
        if not self.__products:
            self.get_products()

        if product_symbol in self.__products:
            tick_size = self.__products[product_symbol]["tick_size"]
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(tick_size)) + Decimal(str(tick_size)))
            return rounded_price
        return price

    def __log_order(self, symbol, size, side, order_type, execution_price):
        try:
            self.user.log_trade(self.PLATFORM_ID, symbol, size, side, order_type, execution_price)
        except Exception as e:
            log(f"[{self}][ERROR] Unable to log order : {type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

#################
### Websocket ###
#################

    def __subscribe(self, channel, symbols, auth=False, callback=None):

        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Subscribing to '{channel}'", "SUBSCRIBE")

        if auth and not self.__is_auth:
            self.__auth()

        self.__ws_send({
            "type": "subscribe",
            "payload": {
                "channels": [
                    {
                        "name": channel,
                        "symbols": symbols,
                    }
                ]
            }
        })

        self.__channel_callbacks_mutex.acquire()
        try:
            if channel not in self.__channel_callbacks:
                self.__channel_callbacks[channel] = {}
            if "symbols" not in self.__channel_callbacks[channel]:
                self.__channel_callbacks[channel]["symbols"] = []

            self.__channel_callbacks[channel]["auth"] = True if auth else False
            self.__channel_callbacks[channel]["callback"] = callback
            for symbol in symbols:
                if symbol not in self.__channel_callbacks[channel]["symbols"]:
                    self.__channel_callbacks[channel]["symbols"].append(symbol)
        finally:
            self.__channel_callbacks_mutex.release()

        self.__ws_log(f"Subscribed to '{channel}'", "SUBSCRIBE")

    def __unsubscribe(self, channel, symbols):

        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Unsubscribing from '{channel}'", "UNSUBSCRIBE")

        self.__ws_send({
            "type": "unsubscribe",
            "payload": {
                "channels": [{
                    "name": channel,
                    "symbols": symbols,
                }]
            }
        })

        self.__channel_callbacks_mutex.acquire()
        try:
            if channel in self.__channel_callbacks:
                for symbol in symbols:
                    if symbol in self.__channel_callbacks[channel]["symbols"]:
                        self.__channel_callbacks[channel]["symbols"].remove(symbol)
                    if len(self.__channel_callbacks[channel]["symbols"]) == 0:
                        del self.__channel_callbacks[channel]
        finally:
            self.__channel_callbacks_mutex.release()

        self.__ws_log(f"Unsubscribed from '{channel}'", "UNSUBSCRIBE")

    def __reset_timer(self):
        self.__reconnect_timer.cancel()
        self.__reconnect_timer = threading.Timer(self.__heartbeat_timeout, self.__reconnect)
        self.__reconnect_timer.start()

    def __reconnect(self):
        self.__ws_log("Reconnecting", "RECONNECT")
        self.disconnect()
        self.connect()

        self.__channel_callbacks_mutex.acquire()
        try:
            channel_callbacks = self.__channel_callbacks.copy()
        finally:
            self.__channel_callbacks_mutex.release()

        # Resubscribe channels
        for channel in channel_callbacks:
            for symbol in channel_callbacks[channel]["symbols"]:
                needs_auth = channel_callbacks[channel]["auth"]
                self.__subscribe(channel, symbol, needs_auth, channel_callbacks[channel]["callback"])

        self.__ws_log("Reconnected", "RECONNECT")

    def __websocket_callback(self):
        while not self.__receive_thread.is_stopped():
            has_data = select.select([self.__websocket], [], [], 1)
            if has_data[0]:

                try:
                    op_code, frame = self.__websocket.recv_data_frame()
                except websocket._exceptions.WebSocketConnectionClosedException:
                    log(f"[{self}][RECEIVE] Connection closed by remote host")
                    self.__connection_lost = True
                    self.__on_close()
                    return

                if op_code == op_codes.OPCODE_CLOSE:
                    self.__ws_log(f"Close frame", "RECEIVE")
                    self.__on_close()
                    return
                elif op_code == op_codes.OPCODE_PING:
                    self.__ws_log(f"Ping frame : Not supported yet", "RECEIVE")
                elif op_code == op_codes.OPCODE_PONG:
                    self.__ws_log(f"Pong frame : Not supported yet", "RECEIVE")
                elif op_code == op_codes.OPCODE_CONT:
                    self.__ws_log(f"Continuation frame : Not supported yet", "RECEIVE")
                elif op_code == op_codes.OPCODE_BINARY:
                    self.__ws_log(f"Binary frame : Not supported yet", "RECEIVE")
                else:
                    self.__on_message(frame.data.decode("utf-8"))

    def __on_close(self):
        reconnect_tries = 1

        # Cancel timed reconnection
        if self.__reconnect_timer:
            self.__reconnect_timer.cancel()
            self.__reconnect_timer = None

        while self.__connection_lost:
            now = datetime.datetime.now()
            if self.__abortion_datetime != None and now >= self.__abortion_datetime:
                break

            time.sleep(self.__reconnect_timeout)
            self.__ws_log(f"Tries to reconnect (try {reconnect_tries})", "ON_CLOSE")
            try:
                self.__reconnect()
                self.__abortion_datetime = None
                self.__ws_log(f"Reconnect succeeded", "ON_CLOSE")
                break
            except:
                self.__ws_log(f"Reconnect failed", "ON_CLOSE")
                reconnect_tries += 1

        if self.__connection_lost:
            self.__connection_aborted = True

    def __on_message(self, message):

        data = json.loads(message)

        if "type" not in data or ("type" in data and data["type"] != "heartbeat"):
            self.__ws_log(message, "RECEIVE")

        if "type" in data:
            event = data["type"]

            if event == "heartbeat":
                self.__reset_timer()

            # Orderbook update
            elif event == "l2_orderbook":

                symbol = [key for key, product in self.__products.items() if product["exchange_symbol"] == data["symbol"]][0]

                data_refactored = {
                    "symbol": symbol,
                    "buy": [],
                    "sell": [],
                }

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
                    "average_price": float(data["average_fill_price"]) if data["average_fill_price"] else None,
                    "limit_price": float(data["limit_price"]) if data["limit_price"] else None,
                    "stop_price": float(data["stop_price"]) if data["stop_price"] else None,
                    "time_in_force": data["time_in_force"],
                }

            else:
                data_refactored = data

            self.__channel_callbacks_mutex.acquire()
            try:
                if event in self.__channel_callbacks and self.__channel_callbacks[event]["callback"]:
                    self.__channel_callbacks[event]["callback"](self, data_refactored)
            finally:
                self.__channel_callbacks_mutex.release()

    def __heartbeat(self, enable=True):
        if enable:
            self.__ws_log("Enable heartbeat")
            type = "enable_heartbeat"
        else:
            self.__ws_log("Disable heartbeat")
            type = "disable_heartbeat"
        self.__ws_send({
            "type": type
        })

    def __auth(self):
        self.__ws_log("Authenticating", "AUTH")
        method = 'GET'
        timestamp = self.__get_timestamp()
        path = '/live'
        data = method + timestamp + path

        self.__ws_send({
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": self.__generate_signature(data),
                "timestamp": timestamp
            }
        })
        self.__is_auth = True
        self.__ws_log("Authenticated", "AUTH")

    def __unauth(self):
        self.__ws_log("Unauthenticating", "UNAUTH")
        self.__ws_send({
            "type": "unauth",
            "payload": {}
        })
        self.__is_auth = False
        self.__ws_log("Unauthenticated", "UNAUTH")

    def __ws_send(self, data):
        self.__ws_log(data, "SEND")
        self.__websocket.send(json.dumps(data))

    def __ws_log(self, message, header=""):
        if self.__show_websocket_logs:
            if header:
                message = f"[{header}] {message}"
            log(f"[{self}] {message}")

    def __str__(self):
        return f"{self.PLATFORM_NAME} {self.user}"