from connectors.threading.Threads import StoppableThread, format_traceback
from connectors.crypto.connector.common.connector import CryptoConnector
from connectors.crypto.connector.fireblocks import Fireblocks
from connectors.crypto.connector.binance_link import BinanceLink
import connectors.crypto.connector.common.websocket_codes as op_codes
from connectors.crypto.connector.core import is_stable_coin, is_fiat
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.crypto.common.randomizer import TradeRandomizer
from connectors.crypto.connector.core import get_contract_size
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
import connectors.config as connectors_config
from decimal import Decimal
from enum import Enum
import pandas as pd
import numpy as np
import threading
import websocket
import requests
import datetime
import hashlib
import select
import queue
import hmac
import time
import json
import copy

class BinanceEndpoints(str, Enum):
    SPOT = "SPOT"
    FUTURE = "FUTURE"
    DELIVERY = "DELIVERY"
    VANILLA = "VANILLA"
    def __str__(self):
        return f"{self.value}"

class Binance(CryptoConnector):
    PLATFORM_ID = 1
    PLATFORM_NAME = "Binance"

    def __init__(self, api_key, api_secret, testnet=False, strategy=None, passphrase="", base_currency="USD", brokerage_strategy=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.brokerage_strategy = brokerage_strategy
        self.session = requests.Session()

        self.tilvest_brokering_fees = 0.00125
        #TODO: Uses strategy brokering fees instead of hard coded values
        if self.strategy and self.strategy.id == 146:
            self.tilvest_brokering_fees = 0.00225

        if self.strategy:
            self.__has_flexible_earn = self.strategy.is_flexible_earn
        else:
            self.__has_flexible_earn = False

        self.fireblocks_api_key = connectors_config.get("FIREBLOCKS_API_KEY")
        self.fireblocks_private_key_path = connectors_config.get("FIREBLOCKS_PRIVATE_KEY_PATH")
        self.binance_link_api_key = connectors_config.get("BINANCE_LINK_API_KEY")
        self.binance_link_api_secret = connectors_config.get("BINANCE_LINK_API_SECRET")
        self.binance_link = BinanceLink(self.binance_link_api_key, self.binance_link_api_secret)
        # self.fireblocks = Fireblocks(self.fireblocks_api_key, self.fireblocks_private_key_path)

        self.session.headers.update({
            "Content-Type": "application/json;charset=utf-8",
                "X-MBX-APIKEY": self.api_key,
        })

        if self.testing:
            self.urls = {
                "api": {
                    BinanceEndpoints.SPOT:     "https://testnet.binance.vision",
                    BinanceEndpoints.FUTURE:   "https://testnet.binancefuture.com",
                    BinanceEndpoints.DELIVERY: "https://testnet.binancefuture.com",
                    BinanceEndpoints.VANILLA:  "https://testnet.binanceops.com",
                },
                "websocket": {
                    BinanceEndpoints.SPOT:     "wss://testnet.binance.vision/ws",
                    BinanceEndpoints.FUTURE:   "wss://stream.binancefuture.com/ws",
                    BinanceEndpoints.DELIVERY: "wss://dstream.binancefuture.com/ws",
                    BinanceEndpoints.VANILLA:  "wss://testnetws.binanceops.com/ws",
                }
            }
            """
            self.spot_base_url =     "https://testnet.binancefuture.com"
            self.future_base_url =   "https://testnet.binancefuture.com"
            self.delivery_base_url = "https://testnet.binancefuture.com"
            self.vanilla_base_url =  "https://testnet.binanceops.com"

            self.spot_ws_url =       "wss://testnet.binance.vision/ws"
            self.future_ws_url =     "wss://stream.binancefuture.com/ws"
            self.delivery_ws_url =   "wss://dstream.binancefuture.com/ws"
            self.options_ws_url =    "wss://testnetws.binanceops.com/ws"
            """
        else:
            self.urls = {
                "api": {
                    BinanceEndpoints.SPOT:     "https://api.binance.com",
                    BinanceEndpoints.FUTURE:   "https://fapi.binance.com",
                    BinanceEndpoints.DELIVERY: "https://dapi.binance.com",
                    BinanceEndpoints.VANILLA:  "https://vapi.binance.com",
                },
                "websocket": {
                    BinanceEndpoints.SPOT:     "wss://stream.binance.com/ws",
                    BinanceEndpoints.FUTURE:   "wss://fstream.binance.com/ws",
                    BinanceEndpoints.DELIVERY: "wss://dstream.binance.com/ws",
                    BinanceEndpoints.VANILLA:  "wss://vstream.binance.com/ws",
                }
            }
            """
            self.spot_base_url =     "https://api.binance.com"
            self.future_base_url =   "https://fapi.binance.com"
            self.delivery_base_url = "https://dapi.binance.com"
            self.vanilla_base_url =  "https://vapi.binance.com"

            self.spot_ws_url =       "wss://stream.binance.com:9443"
            self.future_ws_url =     "wss://fstream.binance.com/ws"
            self.delivery_ws_url =   "wss://dstream.binance.com/ws"
            self.options_ws_url =    "wss://vstream.binance.com/ws"
            """

        # Define weights
        self.__weight_limits = {
            "X-SAPI-USED-IP-WEIGHT-1M": {"limit": 12000, "timeout": 60},
            "X-SAPI-USED-UID-WEIGHT-1M": {"limit": 180000, "timeout": 60},
        }
        self.__weights = {
            "/api/v3/openOrders": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 40},
            "/fapi/v1/openOrders": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 40},
            "/sapi/v1/fiat/payments": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 1},
            "/sapi/v1/fiat/orders": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 90000},
            "/sapi/v1/pay/transactions": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 3000},
            "/sapi/v1/capital/deposit/hisrec": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 1},
            "/sapi/v1/capital/withdraw/history": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 1},
            "/sapi/v1/sub-account/sub/transfer/history": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 1},
            "/sapi/v1/sub-account/transfer/subUserHistory": {"header": "X-SAPI-USED-UID-WEIGHT-1M", "weight": 1},
        }

        # Initialize weight limits
        self.__current_weights = {}
        for header in self.__weight_limits.keys():
            self.__current_weights[header] = 0

        self.__products = {}
        self.__assets = {}
        self.__margin_assets = {}
        self.__margin_products = {}
        self.__coin_info = []

        self.__spot_exchange_info = None
        self.__future_exchange_info = None

        # Check if future access is authorized
        try:
            # Make a request on an unauthorized endpoint if futures are disabled
            self.__api_future_get_position_margin_history("BTCUSDT")
            self.__has_future_enabled = True
        except:
            self.__has_future_enabled = False

        # Check if staking access is authorized
        try:
            # Make a request on an unauthorized endpoint if stakings are disabled
            self.__api_get_staking_product_list()
            self.__has_staking_enabled = True
        except:
            self.__has_staking_enabled = False

        # Check if margin access is authorized
        try:
            # Make a request on an unauthorized endpoint if stakings are disabled
            self.__api_spot_margin_get_account_information()
            self.__has_margin_enabled = True
        except:
            self.__has_margin_enabled = False

        self.__connection_mutex = threading.Lock()
        self.__is_connected = False
        self.__private_spot_connection_mutex = threading.Lock()
        self.__is_private_spot_connected = False
        self.__private_future_connection_mutex = threading.Lock()
        self.__is_private_future_connected = False

        self.__connection_lost = False
        self.__connection_aborted = False
        self.__reconnect_timeout = 30
        self.__abortion_datetime = None
        self.__ws_count = 1

        self.__spot_public_websocket = websocket.WebSocket()
        self.__future_public_websocket = websocket.WebSocket()
        self.__spot_public_thread = None
        self.__future_public_thread = None
        self.__public_callbacks_mutex = threading.Lock()
        self.__public_callbacks = {}

        self.__spot_private_websocket = None
        self.__future_private_websocket = None
        self.__spot_private_thread = None
        self.__future_private_thread = None
        self.__private_callbacks_mutex = threading.Lock()
        self.__private_callbacks = {}

        self.__spot_auth_mutex = threading.Lock()
        self.__is_spot_auth = False
        self.__spot_auth_timeout = 30*60
        self.__spot_auth_timer = None
        self.__spot_auth_channel_number = 0

        self.__future_auth_mutex = threading.Lock()
        self.__is_future_auth = False
        self.__future_auth_timeout = 30*60
        self.__future_auth_timer = None
        self.__future_auth_channel_number = 0

        self.__spot_orderbooks = {}

        self.__show_websocket_logs = False

######################
### Public methods ###
######################

    def get_formatted_price(self, symbol, price):
        price = self.__round_price(symbol, price)
        return price

################
### REST API ###
################

    def is_ip_authorized(self):
        try:
            self.__api_spot_get_account_information()
        except UnauthorizedError:
            return False
        return True

    def has_read_permission(self):
        response = self.__api_spot_get_key_restriction()
        if response["enableReading"]:
            return True
        return False

    def has_write_permission(self):
        response = self.__api_spot_get_key_restriction()
        if response["enableSpotAndMarginTrading"]:
            return True
        return False

    def has_withdraw_permission(self):
        response = self.__api_spot_get_key_restriction()
        if response["enableWithdrawals"]:
            return True
        return False

    def has_future_authorized(self):
        return self.__has_future_enabled

    def get_fees(self):
        #TODO: Get from BinanceLink for subaccounts
        return {
            "taker": 0.001,
            "maker": 0.001,
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
        response = self.__api_spot_get_account_information()
        for asset_balances in response['balances']:
            if float(asset_balances["free"]) > 0:
                symbol = asset_balances["asset"]
                if as_base_currency:
                    usd_price = self.__get_symbol_usd_price(symbol)
                    if usd_price is None:
                        log(f"[{self}] [ERROR] Can't get price for {symbol}")
                        usd_price = 0
                    #log(f"[{self}] symbol : {symbol}")
                    #log(f"[{self}] rate : {rate}")
                    #log(f"[{self}] usd_price : {usd_price}")
                    balance = (float(asset_balances["free"]) + float(asset_balances["locked"]))*usd_price*rate
                    available = float(asset_balances["free"])*usd_price*rate
                    currency = self.base_currency
                else:
                    balance = float(asset_balances["free"]) + float(asset_balances["locked"])
                    available = float(asset_balances["free"])
                    currency = symbol

                data[symbol] = {}
                data[symbol]["symbol"] = symbol
                data[symbol]["balance"] = balance
                data[symbol]["available_balance"] = available
                data[symbol]["currency"] = currency

        #ADD FUTURE ACCOUNT
        if self.__has_future_enabled:
            response = self.__api_future_get_account_information()
            sum_future = 0
            for asset_balances in response['assets']:
                symbol = asset_balances["asset"]
                if not as_base_currency or is_fiat(symbol) or is_stable_coin(symbol):
                    spot_price = 1
                    currency = symbol
                else:
                    if spot_price := self.get_spot_price(symbol+"_USDT"):
                        spot_price = spot_price["spot_price"]*rate
                        currency = self.base_currency
                    else:
                        spot_price = 1
                        currency = symbol

                if symbol in data:
                    data[symbol]["balance"] += (float(asset_balances["walletBalance"]) + float(asset_balances['unrealizedProfit']))*spot_price
                    data[symbol]["available_balance"] += (float(asset_balances["walletBalance"]) - float(asset_balances["positionInitialMargin"]))*spot_price
                else:
                    data[symbol] = {}
                    data[symbol]["balance"] = (float(asset_balances["walletBalance"]) + float(asset_balances['unrealizedProfit']))*spot_price
                    data[symbol]["available_balance"] = (float(asset_balances["walletBalance"]) - float(asset_balances["positionInitialMargin"]))*spot_price
                    data[symbol]["currency"] = currency
                sum_future += (float(asset_balances["walletBalance"]) + float(asset_balances['unrealizedProfit']))*spot_price

        #ADD STAKING ACCOUNT
        if self.__has_staking_enabled:
            response = self.get_staking_product_position('STAKING')
            sum_staking = 0
            for staking in response:
                symbol = staking["symbol"]
                if not as_base_currency or is_fiat(symbol) or is_stable_coin(symbol):
                    spot_price = 1
                    currency = symbol
                else:
                    if spot_price := self.get_spot_price(symbol+"_USDT"):
                        spot_price = spot_price["spot_price"]*rate
                        currency = self.base_currency
                    else:
                        spot_price = 1
                        currency = symbol

                if symbol in data:
                    data[symbol]["balance"] += float(staking["amount"])*spot_price
                else:
                    data[symbol] = {}
                    data[symbol]["symbol"] = symbol
                    data[symbol]["balance"] = float(staking["amount"])*spot_price
                    data[symbol]["available_balance"] = 0
                    data[symbol]["currency"] = currency
                    sum_staking += float(staking["amount"])*spot_price
        if self.__has_flexible_earn:
            response = self.__api_get_flexible_earn_balance()

            if 'USDC' in data:
                data['USDC']["balance"] += float(response['totalAmountInUSDT'])
            else:
                data['USDC'] = {}
                data['USDC']["symbol"] = "USDC"
                data['USDC']["balance"] = float(response['totalAmountInUSDT'])
                data['USDC']["available_balance"] = float(response['totalAmountInUSDT'])
                data['USDC']["currency"] = 'USDC'

        if self.__has_margin_enabled:
            if not self.__margin_products:
                self.margin_get_products()

            if as_base_currency:
                rate = CbForex(self.database).get_rate("USD", self.base_currency)
            else:
                rate = 1

            response = self.__api_spot_margin_get_account_information()

            for asset_balances in response['userAssets']:
                if float(asset_balances["free"]) != 0 or float(asset_balances["borrowed"]) != 0:
                    symbol = asset_balances["asset"]
                    if as_base_currency:
                        usd_price = self.__get_symbol_usd_price(symbol)
                        if usd_price is None:
                            log(f"[{self}] [ERROR] Can't get price for {symbol}")
                            usd_price = 0

                        if float(asset_balances["borrowed"]) == 0:
                            balance = (float(asset_balances["free"]) + float(asset_balances["locked"]))*usd_price*rate
                            available = float(asset_balances["free"])*usd_price*rate
                            borrowed = 0
                        else:
                            balance = float(asset_balances["netAsset"])*usd_price*rate
                            available = float(asset_balances["netAsset"])*usd_price*rate
                            borrowed = float(asset_balances["borrowed"])*usd_price*rate
                        currency = self.base_currency
                    else:
                        if float(asset_balances["borrowed"]) == 0:
                            balance = float(asset_balances["free"]) + float(asset_balances["locked"])
                            available = float(asset_balances["free"])
                            borrowed = float(asset_balances["borrowed"])
                        else:
                            balance = float(asset_balances["netAsset"])
                            available = float(asset_balances["netAsset"])
                            borrowed = float(asset_balances["borrowed"])
                        currency = symbol

                    if symbol in data:
                        data[symbol]["balance"] += float(balance)
                    else:
                        data[symbol] = {}
                        data[symbol]["symbol"] = symbol
                        data[symbol]["balance"] = float(balance)
                        data[symbol]["available_balance"] = available
                        data[symbol]["currency"] = currency
        return data

    def get_profit(self, buy_crypto_history=None):
        if not self.__products:
            self.get_products()

        total_balance = 0
        total_deposit = 0
        total_withdraw = 0
        balances = self.get_balance(as_base_currency=True)

        print('balances',balances)
        for balance in balances.values():
            total_balance += balance["balance"]

        feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
        feeder_cashflows_transaction_ids = feeder_cashflows["last_tx_id"].values
        internal_deposit_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_deposit'")
        internal_fees_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'fees'")
        internal_fees_transaction_id = internal_fees_cashflows["last_tx_id"].values
        internal_deposit_transaction_ids = internal_deposit_cashflows["last_tx_id"].values
        internal_withdraw_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_withdraw'")
        internal_withdraw_transaction_ids = internal_withdraw_cashflows["last_tx_id"].values

        for index, transaction in buy_crypto_history.iterrows():
            if transaction['order_number'] in feeder_cashflows_transaction_ids and not self.strategy.is_feeder_investable():
                continue
            if transaction['order_number'] in internal_deposit_transaction_ids:
                continue
            if transaction['order_number'] in internal_fees_transaction_id:
                continue
            if transaction['order_number'] in internal_withdraw_transaction_ids:
                continue

            if transaction["transaction_type"] == "deposit":
                total_deposit += transaction["base_currency_amount"]
            else:
                total_withdraw += transaction["base_currency_amount"]

            """
            # Substract feeder cashflows from deposits and add to withdrawals
            feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
            feeder_cashflows_amount = 0
            for index, cashflow in feeder_cashflows.iterrows():
                rate = CbForex(self.database).get_rate(cashflow.currency, self.base_currency, datetime.datetime.strptime(str(cashflow.timestamp), "%Y-%m-%d %H:%M:%S"))
                feeder_cashflows_amount += cashflow.amount*rate
            total_deposit -= feeder_cashflows_amount
            total_withdraw += feeder_cashflows_amount
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

            # Spot part
            self.__spot_exchange_info = self.__api_spot_get_exchange_info()
            for product in self.__spot_exchange_info["symbols"]:
                if product["status"] != "TRADING":
                    continue

                base_asset_symbol = product["baseAsset"]
                quote_asset_symbol = product["quoteAsset"]
                generated_symbol = f"{base_asset_symbol}_{quote_asset_symbol}"

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["symbol"],
                    "exchange_symbol": product["symbol"],
                    "contract_type": "spot",
                    # Contract size is always 1 for spot
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
                        "amount": {"min": None, "max": None},
                        "price": {"min": None, "max": None},
                        "notional": {"min": None, "max": None},
                    },
                }

                for filter in product["filters"]:
                    if filter["filterType"] == "LOT_SIZE":
                        product_refactored["precision"]["amount"] = float(filter["stepSize"])
                        product_refactored["limits"]["amount"]["min"] = float(filter["minQty"])
                        product_refactored["limits"]["amount"]["max"] = float(filter["maxQty"])
                    if filter["filterType"] == "PRICE_FILTER":
                        product_refactored["precision"]["price"] = float(filter["tickSize"])
                        product_refactored["limits"]["price"]["min"] = float(filter["minPrice"])
                        product_refactored["limits"]["price"]["max"] = float(filter["maxPrice"])
                    if filter["filterType"] == "NOTIONAL":
                        product_refactored["precision"]["notional"] = float(filter["maxNotional"])
                        product_refactored["limits"]["notional"]["min"] = float(filter["minNotional"])
                        product_refactored["limits"]["notional"]["max"] = float(filter["maxNotional"])

                product_refactored["base_asset_id"] = base_asset_symbol
                product_refactored["base_asset_symbol"] = base_asset_symbol
                product_refactored["quote_asset_id"] = quote_asset_symbol
                product_refactored["quote_asset_symbol"] = quote_asset_symbol

                data[generated_symbol] = product_refactored

            # Future part
            self.__future_exchange_info = self.__api_future_get_exchange_info()
            for product in self.__future_exchange_info["symbols"]:

                if product["status"] != "TRADING":
                    continue

                base_asset_symbol = product["baseAsset"]
                quote_asset_symbol = product["quoteAsset"]

                if product["contractType"] == "PERPETUAL":
                    settlement_time = None
                    settlement_date = None
                    duration = None
                    generated_symbol = f"{base_asset_symbol}_PERP_{quote_asset_symbol}"
                    contract_type = "perpetual_future"
                else:
                    settlement_datetime = datetime.datetime.fromtimestamp(product["deliveryDate"]/1000,
                                        tz=datetime.timezone.utc)
                    settlement_time = settlement_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
                    settlement_date = settlement_time[:10]
                    duration = (settlement_datetime.date() - datetime.date.today()).days
                    generated_symbol = f"{base_asset_symbol}_FUT_{settlement_date}_{quote_asset_symbol}"
                    contract_type = "future"

                product_refactored = {
                    "symbol": generated_symbol,
                    "exchange_id": product["symbol"],
                    "exchange_symbol": product["symbol"],
                    "contract_type": contract_type,
                    "contract_size": float(product["contractSize"]) if "contractSize" in product else 1,
                    "strike_price": None,
                    "settlement_date": settlement_date,
                    "settlement_time": settlement_time,
                    "duration": duration,
                    "precision": {
                        "amount": None,
                        "price": None,
                        "notional": None,
                    },
                    "limits": {
                        "amount": {"min": None, "max": None},
                        "price": {"min": None, "max": None},
                        "notional": {"min": None, "max": None},
                    }
                }

                for filter in product["filters"]:
                    if filter["filterType"] == "LOT_SIZE":
                        product_refactored["precision"]["amount"] = float(filter["stepSize"])
                        product_refactored["limits"]["amount"]["min"] = float(filter["minQty"])
                        product_refactored["limits"]["amount"]["max"] = float(filter["maxQty"])
                    if filter["filterType"] == "PRICE_FILTER":
                        product_refactored["precision"]["price"] = float(filter["tickSize"])
                        product_refactored["limits"]["price"]["min"] = float(filter["minPrice"])
                        product_refactored["limits"]["price"]["max"] = float(filter["maxPrice"])
                    if filter["filterType"] == "NOTIONAL":
                        product_refactored["precision"]["notional"] = float(filter["maxNotional"])
                        product_refactored["limits"]["notional"]["min"] = float(filter["minNotional"])
                        product_refactored["limits"]["notional"]["max"] = float(filter["maxNotional"])

                product_refactored["base_asset_id"] = base_asset_symbol
                product_refactored["base_asset_symbol"] = base_asset_symbol
                product_refactored["quote_asset_id"] = quote_asset_symbol
                product_refactored["quote_asset_symbol"] = quote_asset_symbol

                data[generated_symbol] = product_refactored

            if self.__has_staking_enabled:
                staking_products = self.__api_get_staking_product_list()
                for product in staking_products:

                    generated_symbol = product["projectId"]
                    product_refactored = {
                        "symbol": generated_symbol,
                        "exchange_id": generated_symbol,
                        "exchange_symbol": generated_symbol,
                        "contract_type": "staking",
                        "contract_size": 0,
                        "strike_price": None,
                        "settlement_date": None,
                        "settlement_time": None,
                        "duration": product["detail"]["duration"],
                        "precision": {
                            "amount": None,
                            "price": None,
                            "notional": None,
                        },
                        "limits": {
                            "amount": {"min": None, "max": None},
                            "price": {"min": None, "max": None},
                            "notional": {"min": product["quota"]["minimum"], "max": None},
                        },
                        "base_asset_id": product["detail"]["asset"],
                        "base_asset_symbol": product["detail"]["asset"],
                        "quote_asset_id": product["detail"]["rewardAsset"],
                        "quote_asset_symbol": product["detail"]["rewardAsset"],
                    }

                    data[generated_symbol] = product_refactored

            self.__products = data

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

    def get_orderbook(self, symbol, pricing_client=None, depth=None):
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
            if self.__is_product_spot(symbol):
                orderbook = self.__api_spot_get_orderbook(exchange_symbol, depth)
            elif self.__is_product_future(symbol):
                orderbook = self.__api_future_get_orderbook(exchange_symbol)
            else:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

            data = {
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "buy": [],
                "sell": [],
            }

            for order in orderbook["bids"]:
                order_refactored = {}
                order_refactored["price"] = float(order[0])
                order_refactored["size"] = float(order[1])
                data["buy"].append(order_refactored)
            for order in orderbook["asks"]:
                order_refactored = {}
                order_refactored["price"] = float(order[0])
                order_refactored["size"] = float(order[1])
                data["sell"].append(order_refactored)

        return data

    def get_spot_price(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products and not 'BNFCR' in symbol:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif 'BNFCR' in symbol:
            exchange_id =''
            exchange_symbol = ''
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if self.__is_product_spot(symbol):
            data = self.__api_spot_get_spot_price(exchange_symbol)
        elif self.__is_product_future(symbol):
            data = self.__api_spot_get_spot_price(self.__products[symbol]["base_asset_symbol"])
        elif 'BNFCR' in symbol:
            exchange_id =''
            exchange_symbol = ''
            data = {}
            data['price'] = 1
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data_refactored = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "spot_price": float(data["price"]),
        }
        return data_refactored

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
                    data_refactored = {
                        "symbol": symbol,
                        "exchange_id": exchange_id,
                        "exchange_symbol": exchange_symbol,
                        "mark_price": float(response["mark_price"]),
                    }
            if response == None:
                if self.__is_product_spot(symbol):
                    is_spot = True
                    data = self.__api_spot_get_mark_price(exchange_symbol)
                elif self.__is_product_future(symbol):
                    is_spot = False
                    data = self.__api_future_get_mark_price(exchange_symbol)
                elif self.__is_product_staking(symbol):
                    is_spot = True
                    asset_symbol = self.__products[symbol]["base_asset_symbol"]
                    if asset_symbol+"_USDT" in self.__products:
                        data = self.__api_spot_get_mark_price(self.__products[asset_symbol+"_USDT"]["exchange_symbol"])
                    elif asset_symbol+"_BUSD" not in self.__products:
                        data = self.__api_spot_get_mark_price(self.__products[asset_symbol+"_BUSD"]["exchange_symbol"])
                    else:
                        quoting_symbols = [product["quote_asset_symbol"] for product in self.__products if product["base_asset_symbol"] == asset_symbol]
                        for quoting_symbol in quoting_symbols:
                            if quoting_symbol+"_USDT" in self.__products:
                                data = self.__api_spot_get_mark_price(self.__products[quoting_symbol+"_USDT"]["exchange_symbol"])
                            elif quoting_symbol+"_BUSD" not in self.__products:
                                data = self.__api_spot_get_mark_price(self.__products[quoting_symbol+"_USDT"]["exchange_symbol"])
                else:
                    raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

                data_refactored = {
                    "symbol": symbol,
                    "exchange_id": exchange_id,
                    "exchange_symbol": exchange_symbol,
                    "mark_price": float(data["price"]) if is_spot else float(data["markPrice"]),
                }
        else:
            price = self.__get_symbol_usd_price(symbol)
            data_refactored = {
                "symbol": symbol,
                "exchange_id": self.__assets[symbol]["id"],
                "exchange_symbol": self.__assets[symbol]["symbol"],
                "mark_price": price,
            }

        return data_refactored

    def get_positions(self):
        if not self.__products:
            self.get_products()

        data = {}
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        response = self.__api_spot_get_account_information()
        for asset_balances in response['balances']:
            if float(asset_balances["free"]) > 0:
                symbol = asset_balances["asset"]

                if symbol == "LDUSDT" or symbol == "LDUSDC":
                    usd_price = 1
                else:
                    usd_price = self.__get_symbol_usd_price(symbol)

                data[symbol] = {
                    "symbol": symbol,
                    "exchange_id": symbol,
                    "exchange_symbol": symbol,
                    "size": float(asset_balances["free"]) + float(asset_balances["locked"]),
                    "entry_price": 0,
                    "maintenance_margin": 0,
                    "contract_type": 'spot',
                    "base_asset_id": symbol,
                    "base_asset_symbol": symbol,
                    "quote_asset_id": self.base_currency,
                    "quote_asset_symbol": self.base_currency,
                    "base_currency_amount": (float(asset_balances["free"]) + float(asset_balances["locked"]))*usd_price*rate,
                    "base_currency_symbol": self.base_currency,
                }

        if self.__has_future_enabled:
            response = self.__api_future_get_account_information()
            for position in response["positions"]:
                if float(position["positionAmt"]) != 0:

                    # Find corresponding product
                    for product in self.__products.values():
                        if (product["exchange_symbol"] == position["symbol"] and
                            product["contract_type"] == "perpetual_future" or
                            product["contract_type"] == "future"):
                            symbol = product["symbol"]
                            contract_type = product["contract_type"]
                            base_asset_id = product["base_asset_id"]
                            base_asset_symbol = product["base_asset_symbol"]
                            quote_asset_id = product["quote_asset_id"]
                            quote_asset_symbol = product["quote_asset_symbol"]
                            break

                    data[symbol] = {
                        "symbol": symbol,
                        "exchange_id": position["symbol"],
                        "exchange_symbol": position["symbol"],
                        "size": float(position["positionAmt"]),
                        "entry_price": position["entryPrice"],
                        "maintenance_margin": 0,
                        "contract_type": contract_type,
                        "base_asset_id": base_asset_id,
                        "base_asset_symbol": base_asset_symbol,
                        "quote_asset_id": quote_asset_id,
                        "quote_asset_symbol": quote_asset_symbol,
                    }

        if self.__has_staking_enabled:
            stakings = self.get_staking_product_position('STAKING')
            for staking in stakings:
                symbol = staking['product_id']
                if symbol in data:
                    data[symbol]['size'] += float(staking['amount'])
                else:
                    data[symbol] = {
                        "symbol": staking['product_id'],
                        "exchange_id": staking['product_id'],
                        "exchange_symbol": staking['symbol'],
                        "size": float(staking['amount']),
                        "entry_price": 0,
                        "maintenance_margin": 0,
                        "contract_type": 'staking',
                        "base_asset_id": staking['symbol'],
                        "base_asset_symbol": staking['symbol'],
                        "quote_asset_id": "USD",
                        "quote_asset_symbol": "USD",
                    }

        for symbol in ["LDUSDT", "LDUSDC"]:
            if symbol in data:
                if "USDC" in data:
                    data["USDC"]["size"] += data[symbol]["size"]
                    data["USDC"]["base_currency_amount"] += data[symbol]["base_currency_amount"]
                else:
                    data["USDC"] = {
                        "symbol": "USDC",
                        "exchange_id": "USDC",
                        "exchange_symbol": "USDC",
                        "size": data[symbol]["size"],
                        "entry_price": 0,
                        "maintenance_margin": 0,
                        "contract_type": 'spot',
                        "base_asset_id": "USDC",
                        "base_asset_symbol": "USDC",
                        "quote_asset_id": "USD",
                        "quote_asset_symbol": "USD",
                        "base_currency_amount": data[symbol]["base_currency_amount"],
                        "base_currency_symbol": self.base_currency,
                    }
                del data[symbol]

        return data

    def get_earn_positions(self):
        if not self.__products:
            self.get_products()

        data = {}
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        response = self.__api_spot_get_account_information()
        for asset_balances in response['balances']:
            if float(asset_balances["free"]) > 0:
                symbol = asset_balances["asset"]

                usd_price = self.__get_symbol_usd_price(symbol)
                data[symbol] = {
                        "symbol": symbol,
                        "exchange_id": symbol,
                        "exchange_symbol": symbol,
                        "size": float(asset_balances["free"]) + float(asset_balances["locked"]),
                        "entry_price": 0,
                        "maintenance_margin": 0,
                        "contract_type": 'spot',
                        "base_asset_id": symbol,
                        "base_asset_symbol": symbol,
                        "quote_asset_id": self.base_currency,
                        "quote_asset_symbol": self.base_currency,
                        "base_currency_amount": (float(asset_balances["free"]) + float(asset_balances["locked"]))*usd_price*rate,
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

        response = self.__api_future_get_position_information(exchange_symbol)

        for position in response:
            if position["symbol"] == exchange_symbol:
                data = {
                    "symbol": symbol,
                    "exchange_id": position["symbol"],
                    "exchange_symbol": position["symbol"],
                    "size": position["positionAmt"],
                    "entry_price": position["entryPrice"],
                }
                break
        return data

    def get_position_margin(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        response = self.__api_future_get_position_information(exchange_symbol)

        for position in response:
            if position["symbol"] == exchange_symbol:
                data = {
                    "symbol": symbol,
                    "exchange_id": position["symbol"],
                    "exchange_symbol": position["symbol"],
                    "auto_topup": None,
                    "margin": position["isolatedMargin"],
                    "entry_price": position["entryPrice"],
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

        response = self.__api_future_set_position_margin(exchange_symbol, delta_margin)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "auto_topup": None,
            "margin": response["amount"],
            "entry_price": None,
        }

        return data

    def redeem_flexible_earning(self, symbol, size):
        product_id = self.__api_get_flexible_earn_product_list('USDC')['rows'][0]['productId']

        flexible_earn_position = self.__api_get_flexible_earn_position(product_id)

        flexible_earn_position_amount = flexible_earn_position['size']
        size_reedem = round(min(float(flexible_earn_position_amount), size), 0)
        response = self.__api_redeem_flexible_earn(product_id, size_reedem)

        return response

    def manage_flexible_earn(self, symbol, size, side):
        if self.strategy.is_flexible_earn and 'USDC' in symbol:
            #get flexible product id
            product_id = self.__api_get_flexible_earn_product_list('USDC')[0]['productId']

            flexible_earn_position = self.__api_get_flexible_earn_position(product_id)

            if side == 'buy':
                #SHOULD Reedem USDC
                flexible_earn_position_amount = flexible_earn_position['size']
                size_reedem = round(min(float(flexible_earn_position_amount), size), 0)
                response = self.__api_redeem_flexible_earn(product_id, size_reedem)
                suscribe_flexible_earn = False
            elif side == 'sell':
                #SHOULD Suscribe USDC
                suscribe_flexible_earn = True
        return suscribe_flexible_earn

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):

        if hide_log:
            self.brokerage_strategy = False
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)

        suscribe_flexible_earn = False

        if self.__is_product_spot(symbol):
            #"""
            if self.strategy.is_flexible_earn and 'USDC' in symbol:
                try:
                    #get flexible product id
                    #product_id = "USDT001"
                    product_id = self.__api_get_flexible_earn_product_list('USDC')['rows'][0]['productId']
                    flexible_earn_position = self.__api_get_flexible_earn_position(product_id)
                    flexible_earn_position_amount = flexible_earn_position['size']
                    usd_price = CbForex(self.database).get_rate(symbol.split('_')[0] , "USD")
                    if usd_price is None:
                        usd_price = self.get_mark_price(symbol.split('_')[0]+'_USDC')['mark_price']
                    size_reedem = round(min(float(flexible_earn_position_amount), size*usd_price), 0)
                    if side == 'sell':
                        #SHOULD Reedem USDC
                        if size_reedem > 10:
                            log(f"[{self}] Redeem {size_reedem} on product id {product_id}")
                            response = self.__api_redeem_flexible_earn(product_id, amount=size_reedem)
                        log(f"[{self}] Redeem {size_reedem} on product id {product_id}")
                        response = self.__api_redeem_flexible_earn(product_id, amount=size_reedem)
                    # elif side == 'sell':
                    #    #SHOULD Suscribe USDC
                    #     suscribe_flexible_earn = True
                except Exception as e:
                    log(f"[{self}] Failed to redeem USDT001 : {type(e)} - {e}")
                    pass
            #"""

            response = self.__api_spot_place_order(exchange_symbol, size, side.upper(), order_type="market_order", time_in_force=time_in_force)
        elif self.__is_product_future(symbol):
            response = self.__api_future_place_order(exchange_symbol, size, side.upper(), order_type="market_order", time_in_force=time_in_force)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data = {
            "id": str(response["orderId"]),
            "type": "market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
        }

        # Compute average price and commissions
        if self.__is_product_spot(symbol):
            total_price = 0
            total_size = 0
            total_commission = 0
            for fill in response["fills"]:
                if self.brokerage_strategy and self.strategy.id == 81:
                    if side =='buy':
                        fill["price"] = float(fill["price"])*(1 + self.tilvest_brokering_fees)
                    else:
                        fill["price"] = float(fill["price"])*(1 - self.tilvest_brokering_fees)
                total_price += float(fill["price"])*float(fill["qty"])
                total_size += float(fill["qty"])
                total_commission += float(fill["commission"])
            average_price = total_price / total_size
        else:
            average_price = response["avgPrice"]
            total_commission = 0

        data["average_price"] = average_price
        data["limit_price"] = None
        data["stop_price"] = None
        data["time_in_force"] = response["timeInForce"].lower()
        data["commission"] = total_commission
        data["created_at"] = datetime.datetime.fromtimestamp(response["workingTime"]/1000)
        data["updated_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)

        try:
            time.sleep(2)
            if self.brokerage_strategy and self.strategy.id != 15:
                fees_symbol = symbol.split('_')[0]
                if fees_symbol not in ["USDT", "USDC"]:
                    filled_size = data['filled_size']
                    brokering_fees_tilvest = filled_size*self.tilvest_brokering_fees
                    data['filled_size'] = filled_size - brokering_fees_tilvest

                    if side == 'sell':
                        brokering_fees_tilvest = brokering_fees_tilvest*data["average_price"]
                        fees_symbol = symbol.split('_')[1]

                    # Withdraw brokerage fees
                    transactions = self.strategy.transfer_to_strategy(self.fireblocks, self.binance_link, fees_symbol, brokering_fees_tilvest, self.brokerage_strategy, transaction_type="brokering_fees")
                    now = datetime.datetime.now()

                    try:
                        eur_value = brokering_fees_tilvest*CbForex(self.database).get_rate(fees_symbol, "EUR")
                    except Exception as e:
                        log(f"[{self}] [ERROR] Unable to compute eur value : {type(e)} - {e}")
                        log(format_traceback(e.__traceback__))
                        eur_value = 0

                    # Add brokering fees
                    brokering_fees = pd.DataFrame([{
                        "strategy_id": self.strategy.id,
                        "symbol": fees_symbol,
                        "amount": brokering_fees_tilvest,
                        "eur_value": eur_value,
                        "timestamp": now,
                        "created_at": now,
                        "updated_at": now,
                    }])
                    self.database.append_to_table("brokering_fees", brokering_fees)

        except Exception as e:
            log(f"[{self}] [ERROR] Unable to make transfer for brokerage fees : {type(e)} - {e}")
            log(format_traceback(e.__traceback__))

        if not hide_log:
            self.__log_order(data["id"], symbol, data['size'], side, "market_order", data["status"], average_price)

        if suscribe_flexible_earn:
            product_id = self.__api_get_flexible_earn_product_list('USDC')['rows'][0]['productId']
            flexible_earn_position = self.__api_get_flexible_earn_position(product_id)
            size_suscribe = round((self.get_positions()['USDC']['size'] - flexible_earn_position['size'])*0.99, 0)
            #TEMP USe LDUSDT
            size_suscribe = round((self.get_positions()['USDC']['size'] - self.get_positions()['USDC']['size'])*0.99, 0)
            if size_suscribe > 10:
                log(f"[{self}] Suscribe {size_suscribe} on product id {product_id} ")
                response = self.__api_subscribe_flexible_earn(product_id, amount=size_suscribe)


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

        if self.__is_product_spot(symbol):
            response = self.__api_spot_place_order(exchange_symbol, size, side.upper(), order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force)
        elif self.__is_product_future(symbol):
            response = self.__api_future_place_order(exchange_symbol, size, side.upper(), order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        started_at = time.time()

        data = {
            "id": str(response["orderId"]),
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = str(response["orderId"])
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
                order = self.__get_order_state(symbol, order_id)

                data["status"] = self.__compose_order_status(order)
                data["symbol"] = symbol
                data["exchange_id"] = order["symbol"]
                data["exchange_symbol"] = order["symbol"]
                data["side"] = order["side"].lower()
                data["size"] = float(order["origQty"])
                data["filled_size"] = float(order["executedQty"])
                data["unfilled_size"] = float(order["origQty"]) - float(order["executedQty"])
                data["average_price"] = order["avgPrice"]
                data["limit_price"] = order["price"]
                data["stop_price"] = None
                data["time_in_force"] = order["timeInForce"].lower()

                # Compute average price and commissions
                if self.__is_product_spot(symbol):
                    total_price = 0
                    total_size = 0
                    total_commission = 0
                    for fill in order["fills"]:
                        total_price += float(fill["price"])*float(fill["qty"])
                        total_size += float(fill["qty"])
                        total_commission += float(fill["commission"])
                    average_price = total_price / total_size
                else:
                    average_price = order["avgPrice"]
                    total_commission = 0

                data["average_price"] = average_price
                data["limit_price"] = None
                data["stop_price"] = None
                data["time_in_force"] = response["timeInForce"].lower()
                data["commission"] = total_commission
                data["created_at"] = datetime.datetime.fromtimestamp(order["time"]/1000),
                data["updated_at"] = datetime.datetime.fromtimestamp(order["updateTime"]/1000),


        # Doesn't wait for order to be placed
        else:
            data["status"] = self.__compose_order_status(response)
            data["symbol"] = symbol
            data["exchange_id"] = response["symbol"]
            data["exchange_symbol"] = response["symbol"]
            data["side"] = response["side"].lower()
            data["size"] = float(response["origQty"])
            data["filled_size"] = float(response["executedQty"])
            data["unfilled_size"] = float(response["origQty"]) - float(response["executedQty"])
            data["average_price"] = None
            data["limit_price"] = response["price"]
            data["stop_price"] = None
            data["time_in_force"] = response["timeInForce"].lower()
            data["commission"] = 0
            data["created_at"] = datetime.datetime.fromtimestamp(response["workingTime"]/1000)
            data["updated_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], response["price"])

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

        if self.__is_product_spot(symbol):
            response = self.__api_spot_place_order(exchange_symbol, size, side.upper(), order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force)
        elif self.__is_product_future(symbol):
            response = self.__api_future_place_order(exchange_symbol, size, side.upper(), order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data = {
            "id": str(response["orderId"]),
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": None,
            "limit_price": None,
            "stop_price": response["stopPrice"],
            "time_in_force": response["timeInForce"].lower(),
            "commission": 0,
            "created_at" : datetime.datetime.fromtimestamp(response["workingTime"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_market_order", data["status"], response["stopPrice"])

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

        if self.__is_product_spot(symbol):
            response = self.__api_spot_place_order(exchange_symbol, size, side.upper(), order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force)
        elif self.__is_product_future(symbol):
            response = self.__api_future_place_order(exchange_symbol, size, side.upper(), order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data = {
            "id": str(response["orderId"]),
            "type": "stop_limit",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": None,
            "limit_price": response["price"],
            "stop_price": response["stopPrice"],
            "time_in_force": response["timeInForce"].lower(),
            "commission": 0,
            "created_at" : datetime.datetime.fromtimestamp(response["workingTime"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_limit_order", data["status"], response["price"])

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
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["price"])
            elif not stop_market_order_active:
                log(f"[{self}] Stop market order placed")
                order = self.get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["price"])
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

    def place_randomized_order(self, symbol, side, size, trade_margin=False):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        trade_randomizer = TradeRandomizer(self, symbol, side, size, trade_margin=trade_margin)
        trade_randomizer.run()

    def get_active_orders(self, symbol=None):
        if not self.__products:
            self.get_products()

        if symbol:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_id = self.__products[symbol]["exchange_id"]
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                response = self.__api_spot_get_active_orders(exchange_symbol)
            elif self.__is_product_future(symbol):
                response = self.__api_future_get_active_orders(exchange_symbol)
            else:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            response = self.__api_spot_get_active_orders()
            if self.__has_future_enabled:
                response.extend(self.__api_future_get_active_orders())

        data = []
        for order in response:
            active_order = {
                "id": str(order["orderId"]),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol if symbol else next((product["symbol"] for product in self.__products.values() if product["exchange_id"] == order["symbol"]), None),
                "exchange_id": order["symbol"],
                "exchange_symbol": order["symbol"],
                "side": order["side"].lower(),
                "size": float(order["origQty"]),
                "filled_size": float(order["executedQty"]),
                "unfilled_size": float(order["origQty"]) - float(order["executedQty"]),
                "average_price": order["avgPrice"] if "avgPrice" in order else None,
                "limit_price": order["price"],
                "stop_price": order["stopPrice"],
                "time_in_force": order["timeInForce"].lower(),
                "created_at" : datetime.datetime.fromtimestamp(order["time"]/1000),
                "updated_at" : datetime.datetime.fromtimestamp(order["updateTime"]/1000),
            }
            data.append(active_order)
        return data

    def get_order_state(self, symbol, order_id):
        response = self.__get_order_state(symbol, order_id)

        if float(response["executedQty"]) > 0:
            average_price = response["avgPrice"] if "avgPrice" in response else float(response["cummulativeQuoteQty"]) / float(response["executedQty"])
        else:
            average_price = None

        data = {
            "id": str(response["orderId"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": average_price,
            "limit_price": response["price"],
            "stop_price": response["stopPrice"] if "stopPrice" in response else None,
            "time_in_force": response["timeInForce"].lower(),
            "created_at" : datetime.datetime.fromtimestamp(response["time"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["updateTime"]/1000),
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

        if self.__is_product_spot(symbol):
            response = self.__api_spot_cancel_order(exchange_symbol, order_id)
        elif self.__is_product_future(symbol):
            response = self.__api_future_cancel_order(exchange_symbol, order_id)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data = {}
        if "order_id" in response:
            data = {
                "id": str(response["orderId"]),
                "type": self.__compose_order_type(response),
                "status": self.__compose_order_status(response),
                "symbol": symbol,
                "exchange_id": response["symbol"],
                "exchange_symbol": response["symbol"],
                "side": response["side"].lower(),
                "size": float(response["origQty"]),
                "filled_size": float(response["executedQty"]),
                "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
                "average_price": response["avgPrice"] if "avgPrice" in response else None,
                "limit_price": response["price"],
                "stop_price": response["stopPrice"] if "stopPrice" in response else None,
                "time_in_force": response["timeInForce"].lower(),
            }
        elif "orderReports" in response:
            for order_report in response["orderReports"]:
                if str(order_report["orderId"]) == order_id:
                    data = {
                        "id": str(order_report["orderId"]),
                        "type": self.__compose_order_type(order_report),
                        "status": self.__compose_order_status(order_report),
                        "symbol": symbol,
                        "exchange_id": order_report["symbol"],
                        "exchange_symbol": order_report["symbol"],
                        "side": order_report["side"].lower(),
                        "size": float(order_report["origQty"]),
                        "filled_size": float(order_report["executedQty"]),
                        "unfilled_size": float(order_report["origQty"]) - float(order_report["executedQty"]),
                        "average_price": order_report["avgPrice"] if "avgPrice" in order_report else None,
                        "limit_price": order_report["price"],
                        "stop_price": order_report["stopPrice"] if "stopPrice" in order_report else None,
                        "time_in_force": order_report["timeInForce"].lower(),
                    }

        return data

    def get_order_history(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if self.__is_product_spot(symbol):
            response = self.__api_spot_get_order_history(exchange_symbol)
        elif self.__is_product_future(symbol):
            response = self.__api_future_get_order_history(exchange_symbol)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        data = []
        for order in response:
            data.append({
                "id": str(order["orderId"]),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol,
                "exchange_id": order["symbol"],
                "exchange_symbol": order["symbol"],
                "side": order["side"].lower(),
                "size": float(order["origQty"]),
                "filled_size": float(order["executedQty"]),
                "unfilled_size": float(order["origQty"]) - float(order["executedQty"]),
                "average_price": order["avgPrice"] if "avgPrice" in order else None,
                "limit_price": order["price"],
                "stop_price": order["stopPrice"],
                "time_in_force": order["timeInForce"].lower(),
                "source": "???",
                "is_liquidation": False,
                "cancellation_reason": None,
                "created_at": datetime.datetime.fromtimestamp(order["time"]/1000),
                "updated_at": datetime.datetime.fromtimestamp(order["updateTime"]/1000),
            })

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
        response = self.__api_future_get_account_information()
        for position in response["positions"]:
            if position["symbol"] == exchange_symbol:
                data = {
                    "symbol": symbol,
                    "leverage": int(position["leverage"]),
                }

        if len(data) == 0:
            data = {
                "symbol": symbol,
                "leverage": 0,
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

        response = self.__api_future_set_leverage(exchange_symbol, leverage)
        data = {
            "symbol": symbol,
            "leverage": int(leverage),
        }
        return data

    def get_trade(self, symbol, trade_id):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = {}
        response = self.__api_spot_get_trade_history(exchange_symbol, trade_id, limit=1)
        if len(response) > 0:
            data = {
                "id": str(trade_id),
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "size": float(response[0]["qty"]),
                "price": float(response[0]["price"]),
                "datetime": datetime.datetime.fromtimestamp(response[0]['time']/1000000)
            }
        return data

    def get_trade_history(self, symbol, start_date, end_date):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = []

        index = 0
        stop = end_date
        start = end_date
        delta = datetime.timedelta(hours=24)
        while start > start_date:

            stop = end_date - index*delta
            if stop - start_date > delta:
                start = end_date - (index+1)*delta
            else:
                start = start_date

            start_timestamp = round(datetime.datetime.timestamp(start))*1000
            stop_timestamp = round(datetime.datetime.timestamp(stop))*1000

            response = self.__api_get_trade_history(exchange_symbol, start_timestamp, stop_timestamp)

            for trade in response:
                data.append(
                    {
                        "id": trade["orderId"],
                        "symbol": symbol,
                        "exchange_id": trade["symbol"],
                        "exchange_symbol": trade["symbol"],
                        "side" : "buy" if trade["isBuyer"] else "sell",
                        "size": trade["qty"],
                        "price": trade["price"],
                        "datetime": datetime.datetime.fromtimestamp(trade['time']/1000)
                    }
                )
            index += 1

        return data

    def get_public_trades(self, symbol, duration=None):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        data = []
        response = self.__api_spot_get_historical_trades(exchange_symbol)
        for trade in response:
            trade_date = datetime.datetime.fromtimestamp(trade['time']/1000)
            data.insert(0, {
                "id": trade["id"],
                "symbol": symbol,
                "exchange_id": exchange_id,
                "exchange_symbol": exchange_symbol,
                "side" : "sell" if trade["isBuyerMaker"] else "buy",
                "size": float(trade["qty"]),
                "price": float(trade["price"]),
                "datetime": trade_date,
            })

        """
        data = []
        end_date = datetime.datetime.now()
        start_date = end_date - duration if duration is not None else end_date
        last_date = end_date
        last_trade_id = None

        while last_date >= start_date:
            print("---")
            print(f"### CALL : {last_trade_id}")
            response = self.__api_spot_get_historical_trades(exchange_symbol, from_id=last_trade_id)
            print(f"### RESP : {len(response)}")
            for trade in response:
                trade_date = datetime.datetime.fromtimestamp(trade['time']/1000)
                data.insert(0, {
                    "id": trade["id"],
                    "symbol": symbol,
                    "exchange_id": exchange_id,
                    "exchange_symbol": exchange_symbol,
                    "side" : "sell" if trade["isBuyerMaker"] else "buy",
                    "size": float(trade["qty"]),
                    "price": float(trade["price"]),
                    "datetime": trade_date,
                })
                last_date = trade_date
                if duration is not None and trade_date < start_date:
                    print("BREAK LOOP 1")
                    break
            if duration is None:
                print("BREAK LOOP 2")
                break
            else:
                last_trade_id = data[-1]["id"] - len(response)
                print(f'{data[0]["id"]} - {data[0]["datetime"]}')
                print(f'{data[-1]["id"]} - {data[-1]["datetime"]}')
                print(last_trade_id)
            #break
        """
        return data

    def get_staking_product_list(self):
        stakings = []

        response = self.__api_get_staking_product_list()

        for product in response:
            staking = {
                "id": product["projectId"],
                "symbol": product["detail"]['asset'],
                "duration": product["detail"]['duration'],
                "apy": product["detail"]['apy'],
                "quota": product["quota"]['totalPersonalQuota'],
                "minimum": product["quota"]['minimum'],
            }
            stakings.append(staking)

        return stakings

    def redeem_staking_product(self, product, product_id, position_id=0, amount=0):
        return self.__api_redeem_staking_product(product, product_id, position_id, amount)

    def purchase_staking_product(self, product, product_id, amount=0):
        return self.__api_purchase_staking_product(product, product_id, amount)

    def get_staking_quota_left(self, product, product_id):
        return self.__api_staking_quota_left(product, product_id)

    def get_staking_product_position(self, product, symbol=None):
        positions = []

        response = self.__api_get_staking_product_position(product)
        for product in response:
            position = {
                "position_id": product["positionId"],
                "product_id": product["productId"],
                "symbol": product["asset"],
                "amount": product["amount"],
                "purchase_time": product["purchaseTime"],
                "duration": product['duration'],
                "deliver_date": product['deliverDate'],
            }
            positions.append(position)
        if symbol is not None:
            position = {"symbol": symbol, "size": 0}
            for product in positions:
                if product["symbol"] == symbol:
                    position["size"] += float(product["amount"])
            return position
        return positions

    def get_left_quota(self, product_id, product):
        return self.__api_get_left_quota(product_id, product)

    def transfer_account(self, asset, amount, type):
        return self.__api_transfer_account(asset, amount, type)

    def get_wallet_address(self, symbol):
        return self.__api_spot_get_deposit_address(symbol)

    def withdraw(self, symbol, amount, address, network_name=None):
        if not self.__coin_info:
            self.__coin_info = self.__api_spot_get_coin_information()

        # Check that given coin symbol is supported for withdraw
        coin = None
        for coin_info in self.__coin_info:
            if coin_info["coin"] == symbol:
                coin = coin_info
        if not coin:
            raise UnknownCoinError(f"Unknown coin symbol '{symbol}'")

        # Take first available network
        if not network_name:
            network = coin["networkList"][0]["network"]
        # Check that given network is supported for withdraw
        else:
            network_allowed = False
            allowed_networks = []
            for coin_network in coin["networkList"]:
                allowed_networks.append(coin_network["network"])
                if coin_network["network"] == network_name:
                    network_allowed = True
                    network = coin_network
            if not network_allowed:
                raise UnknownNetworkError(f"Unknown network '{network_name}' for coin '{symbol}'. Allowed networks are : {allowed_networks}")

        # Check size
        withdraw_multiple = network["withdrawIntegerMultiple"]
        rounded_amount = float(Decimal(str(amount)) - Decimal(str(amount)) % Decimal(str(withdraw_multiple)))

        return self.__api_spot_withdraw(symbol, rounded_amount, address, network_name)

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

        if not start_date:
            start_date = datetime.date(datetime.date.today().year, 1, 1)
            start_date = datetime.date(2022, 1, 1)
        if not end_date:
            end_date = datetime.date.today()

        start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
        end_date = datetime.datetime.combine(end_date, datetime.datetime.max.time())

        index = 0
        stop = end_date
        start = end_date
        while start > start_date:

            stop = end_date - index*datetime.timedelta(days=90)
            if stop - start_date > datetime.timedelta(days=90):
                start = end_date - (index+1)*datetime.timedelta(days=90)
            else:
                start = start_date



            # /sapi/v1/fiat/payments
            fiat_payments_deposits = self.__api_spot_get_fiat_payments("deposit", start, stop)
            if "data" in fiat_payments_deposits and len(fiat_payments_deposits["data"]) > 0:
                for deposit in fiat_payments_deposits["data"]:
                    # Keep only payment which suceeded
                    if deposit["status"] == "Completed":
                        date = datetime.datetime.fromtimestamp(deposit['updateTime']/1000)
                        rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                        usd_amount = self.__get_symbol_usd_price(deposit["cryptoCurrency"], date)
                        base_currency_amount = float(deposit["obtainAmount"])*rate*usd_amount
                        data.append({
                            "date": date,
                            "type": "deposit",
                            "source_symbol": deposit["fiatCurrency"],
                            "source_amount": float(deposit["sourceAmount"]),
                            "destination_symbol": deposit["cryptoCurrency"],
                            "destination_amount": float(deposit["obtainAmount"]),
                            "fees": 0,
                            "operation_id": str(deposit["orderNo"]),
                            "wallet_address": None,
                            "base_currency_amount": base_currency_amount,
                            "base_currency_symbol": self.base_currency,
                            "tx_hash": None,
                        })

            # /sapi/v1/fiat/payments
            fiat_payments_withdrawals = self.__api_spot_get_fiat_payments("withdraw", start, stop)
            if "data" in fiat_payments_withdrawals and len(fiat_payments_withdrawals["data"]) > 0:
                for withdraw in fiat_payments_withdrawals["data"]:
                    # Keep only payment which suceeded
                    if withdraw["status"] == "Completed":
                        date = datetime.datetime.fromtimestamp(withdraw['updateTime']/1000)
                        rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                        usd_amount = self.__get_symbol_usd_price(withdraw["cryptoCurrency"], date)
                        base_currency_amount = float(withdraw["obtainAmount"])*rate*usd_amount
                        data.append({
                            "date": date,
                            "type": "withdraw",
                            "source_symbol": withdraw["fiatCurrency"],
                            "source_amount": float(withdraw["sourceAmount"]),
                            "destination_symbol": withdraw["cryptoCurrency"],
                            "destination_amount": float(withdraw["obtainAmount"]),
                            "fees": 0,
                            "operation_id": str(withdraw["orderNo"]),
                            "wallet_address": None,
                            "base_currency_amount": base_currency_amount,
                            "base_currency_symbol": self.base_currency,
                            "tx_hash": None,
                        })

            # /sapi/v1/fiat/orders
            fiat_deposits = self.__api_spot_get_fiat_deposit_withdraw_history("deposit", start, stop)
            if "data" in fiat_deposits and len(fiat_deposits["data"]) > 0:
                for deposit in fiat_deposits["data"]:
                    # Keep only deposit which suceeded
                    if deposit["status"] == "Successful" or deposit["status"] == "Finished":
                        date = datetime.datetime.fromtimestamp(deposit['updateTime']/1000)
                        rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                        usd_amount = self.__get_symbol_usd_price(deposit["fiatCurrency"], date)
                        base_currency_amount = float(deposit["amount"])*rate*usd_amount
                        data.append({
                            "date": date,
                            "type": "deposit",
                            "source_symbol": deposit["fiatCurrency"],
                            "source_amount": float(deposit["indicatedAmount"]),
                            "destination_symbol": deposit["fiatCurrency"],
                            "destination_amount": float(deposit["amount"]),
                            "fees": float(deposit["totalFee"]),
                            "operation_id": str(deposit["orderNo"]),
                            "wallet_address": None,
                            "base_currency_amount": base_currency_amount,
                            "base_currency_symbol": self.base_currency,
                            "tx_hash": None,
                        })

            # /sapi/v1/fiat/orders
            fiat_withdrawals = self.__api_spot_get_fiat_deposit_withdraw_history("withdraw", start, stop)
            if "data" in fiat_withdrawals and len(fiat_withdrawals["data"]) > 0:
                for withdraw in fiat_withdrawals["data"]:
                    # Keep only withdrawals which suceeded
                    if deposit["status"] == "Successful" or deposit["status"] == "Finished":
                        date = datetime.datetime.fromtimestamp(withdraw['insertTime']/1000)
                        rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                        usd_amount = self.__get_symbol_usd_price(withdraw["fiatCurrency"], date)
                        base_currency_amount = float(withdraw["sourceAmount"])*rate*usd_amount
                        data.append({
                            "date": date,
                            "type": "withdraw",
                            "source_symbol": withdraw["fiatCurrency"],
                            "source_amount": float(withdraw["sourceAmount"]),
                            "destination_symbol": withdraw["fiatCurrency"],
                            "destination_amount": float(withdraw["sourceAmount"]),
                            "fees": float(withdraw["totalFee"]),
                            "operation_id": str(withdraw["orderNo"]),
                            "wallet_address": None,
                            "base_currency_amount": base_currency_amount,
                            "base_currency_symbol": self.base_currency,
                            "tx_hash": None,
                        })

            # /sapi/v1/capital/deposit/hisrec
            deposit_history = self.__api_spot_get_deposit_history(start, stop)
            for deposit in deposit_history:
                # Append only external transfers which suceeded
                if deposit["status"] == 1 and deposit["transferType"] == 0:
                    date = datetime.datetime.fromtimestamp(deposit['insertTime']/1000)
                    transaction_id = str(deposit["txId"])

                    # Filter-out fees and feeder cashflows
                    if self.strategy and self.database:
                        transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                        if not transaction_cashflow.empty:
                            cashflow = transaction_cashflow.iloc[0]
                            if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                                continue

                    rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                    usd_amount = self.__get_symbol_usd_price(deposit["coin"], date)
                    base_currency_amount = float(deposit["amount"])*rate*usd_amount

                    data.append({
                        "date": date,
                        "type": "deposit",
                        "source_symbol": deposit["coin"],
                        "source_amount": float(deposit["amount"]),
                        "destination_symbol": deposit["coin"],
                        "destination_amount": float(deposit["amount"]),
                        "fees": 0,
                        "operation_id": transaction_id,
                        "wallet_address": deposit["address"],
                        "base_currency_amount": base_currency_amount,
                        "base_currency_symbol": self.base_currency,
                        "tx_hash": transaction_id,
                    })

            # /sapi/v1/capital/withdraw/history
            withdraw_history = self.__api_spot_get_withdraw_history(start, stop)
            for withdraw in withdraw_history:
                # Append only external transfers which suceeded
                if withdraw["status"] == 6 and withdraw["transferType"] == 0:
                    date = datetime.datetime.strptime(withdraw['applyTime'], "%Y-%m-%d %H:%M:%S")
                    transaction_id = str(withdraw["txId"])

                    # Filter-out fees and feeder cashflows
                    if self.strategy and self.database:
                        transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                        if not transaction_cashflow.empty:
                            cashflow = transaction_cashflow.iloc[0]
                            if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                                continue

                    rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                    usd_amount = self.__get_symbol_usd_price(withdraw["coin"], date)
                    base_currency_amount = float(withdraw["amount"])*rate*usd_amount

                    data.append({
                        "date": date,
                        "type": "withdraw",
                        "source_symbol": withdraw["coin"],
                        "source_amount": float(withdraw["amount"]),
                        "destination_symbol": withdraw["coin"],
                        "destination_amount": float(withdraw["amount"]),
                        "fees": float(withdraw["transactionFee"]),
                        "operation_id": str(withdraw["txId"]),
                        "wallet_address": withdraw["address"],
                        "base_currency_amount": base_currency_amount,
                        "base_currency_symbol": self.base_currency,
                        "tx_hash": withdraw['txId'],
                    })

            # /sapi/v1/pay/transactions
            # Only requests within last 18 months
            withdraw_payments = self.__api_spot_get_pay_history(start, stop) if (datetime.datetime.now() - start).days/30 < 18 else []
            if "data" in withdraw_payments and len(withdraw_payments["data"]) > 0:
                for withdraw in withdraw_payments["data"]:
                    date = datetime.datetime.fromtimestamp(withdraw['transactionTime']/1000)
                    rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                    usd_amount = self.__get_symbol_usd_price(withdraw["currency"], date)
                    base_currency_amount = abs(float(withdraw["amount"]))*rate*usd_amount
                    data.append({
                        "date": date,
                        "type": "withdraw",
                        "source_symbol": withdraw["currency"],
                        "source_amount": abs(float(withdraw["amount"])),
                        "destination_symbol": withdraw["currency"],
                        "destination_amount": abs(float(withdraw["amount"])),
                        "fees": 0,
                        "operation_id": str(withdraw["transactionId"]),
                        "wallet_address": None,
                        "base_currency_amount": base_currency_amount,
                        "base_currency_symbol": self.base_currency,
                        "tx_hash": None,
                    })

            try:
                # /sapi/v1/sub-account/sub/transfer/history
                master_transfers = self.__api_get_masteraccount_transfers(start, stop)
            except (MasterAccountOnlyEndpointError,UnsupportedOperationError):
                master_transfers = []
            for transfer in master_transfers:
                if transfer["status"] != "SUCCESS":
                    continue
                date = datetime.datetime.fromtimestamp(transfer['time']/1000)
                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(transfer["asset"], date)
                base_currency_amount = float(transfer["qty"])*rate*usd_amount
                data.append({
                    "date": date,
                    "type": "withdraw",
                    "source_symbol": transfer["asset"],
                    "source_amount": float(transfer["qty"]),
                    "destination_symbol": transfer["asset"],
                    "destination_amount": float(transfer["qty"]),
                    "fees": 0,
                    "operation_id": str(transfer["tranId"]),
                    "wallet_address": None,
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": None,
                })

            try:
                # /sapi/v1/sub-account/transfer/subUserHistory
                subaccount_in_transfers = self.__api_get_subaccount_transfers(start, stop, 1)
            except (SubAccountOnlyEndpointError,UnsupportedOperationError):
                subaccount_in_transfers = []
            for transfer in subaccount_in_transfers:
                if transfer["status"] != "SUCCESS":
                    continue
                date = datetime.datetime.fromtimestamp(transfer['time']/1000)
                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(transfer["asset"], date)
                base_currency_amount = float(transfer["qty"])*rate*usd_amount
                data.append({
                    "date": date,
                    "type": "deposit",
                    "source_symbol": transfer["asset"],
                    "source_amount": float(transfer["qty"]),
                    "destination_symbol": transfer["asset"],
                    "destination_amount": float(transfer["qty"]),
                    "fees": 0,
                    "operation_id": str(transfer["tranId"]),
                    "wallet_address": None,
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": None,
                })

            try:
                # /sapi/v1/sub-account/transfer/subUserHistory
                subaccount_out_transfers = self.__api_get_subaccount_transfers(start, stop, 2)
            except (SubAccountOnlyEndpointError,UnsupportedOperationError):
                subaccount_out_transfers = []
            for transfer in subaccount_out_transfers:
                if transfer["status"] != "SUCCESS":
                    continue
                date = datetime.datetime.fromtimestamp(transfer['time']/1000)
                rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
                usd_amount = self.__get_symbol_usd_price(transfer["asset"], date)
                base_currency_amount = float(transfer["qty"])*rate*usd_amount
                data.append({
                    "date": date,
                    "type": "withdraw",
                    "source_symbol": transfer["asset"],
                    "source_amount": float(transfer["qty"]),
                    "destination_symbol": transfer["asset"],
                    "destination_amount": float(transfer["qty"]),
                    "fees": 0,
                    "operation_id": str(transfer["tranId"]),
                    "wallet_address": None,
                    "base_currency_amount": base_currency_amount,
                    "base_currency_symbol": self.base_currency,
                    "tx_hash": None,
                })

            index += 1

        if len(data) > 0:
            data.sort(key=lambda operation:operation["date"])
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

        start_timestamp = round(datetime.datetime.timestamp(start_date))*1000
        stop_timestamp = round(datetime.datetime.timestamp(end_date))*1000

        response = self.__api_spot_get_candle(exchange_symbol, start_timestamp, stop_timestamp, resolution)

        candle_number = len(response)
        if candle_number == 0:
            open = 0
            close = 0
            low = 0
            high = 0
            volume = 0
        else:
            open = float(response[0][1])
            close = float(response[candle_number-1][4])
            low = float(response[0][3])
            high = float(response[0][2])
            volume = 0

            for candle in response:
                low = float(candle[3]) if float(candle[3]) < low else low
                high = float(candle[2]) if float(candle[2]) > high else high
                volume += float(candle[5])

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

    def spot_to_margin_transfer(self, symbol, amount):
        response = self.__api_spot_make_universal_transfer("MAIN_MARGIN", symbol, amount)
        return response

    def margin_to_spot_transfer(self, symbol, amount):
        response = self.__api_spot_make_universal_transfer("MARGIN_MAIN", symbol, amount)
        return response

    def get_personnal_left_quota(self, product_id):
        response = self.__api_get_personnal_left_quota(product_id)
        return response

    def margin_get_assets(self, reload=False):
        if not self.__margin_assets or reload:
            self.__margin_assets = {}
            assets = self.__api_spot_margin_get_assets()
            for asset in assets:
                self.__margin_assets[asset["assetName"]] = {
                    "exchange_id": asset["assetName"],
                    "exchange_symbol": asset["assetName"],
                    "symbol": asset["assetName"],
                    "is_borrowable": asset["isBorrowable"],
                    "is_mortgageable": asset["isMortgageable"],
                    "user_min_borrow": asset["userMinBorrow"],
                    "user_min_repay": asset["userMinRepay"],
                }
        return self.__margin_assets

    def margin_get_products(self, reload=False):
        if not self.__products:
            self.get_products()
        if not self.__margin_products or reload:
            self.__margin_products = {}
            products = self.__api_spot_margin_get_products()
            for product in products:
                symbol = f"{product['base']}_{product['quote']}"
                self.__margin_products[symbol] = {
                    "exchange_id": product["id"],
                    "exchange_symbol": product["symbol"],
                    "symbol": symbol,
                    "is_margin_trade": product["isMarginTrade"],
                    "is_buy_allowed": product["isBuyAllowed"],
                    "is_sell_allowed": product["isSellAllowed"],
                    "contract_type": self.__products[symbol]["contract_type"] if symbol in self.__products else None,
                    "contract_size": 1,
                    "strike_price": self.__products[symbol]["strike_price"] if symbol in self.__products else None,
                    "settlement_date": self.__products[symbol]["settlement_date"] if symbol in self.__products else None,
                    "settlement_time": self.__products[symbol]["settlement_time"] if symbol in self.__products else None,
                    "duration": self.__products[symbol]["duration"] if symbol in self.__products else None,
                    "precision": {
                        "amount": self.__products[symbol]["precision"]["amount"] if symbol in self.__products else None,
                        "price": self.__products[symbol]["precision"]["price"] if symbol in self.__products else None,
                        "notional": self.__products[symbol]["precision"]["notional"] if symbol in self.__products else None,
                    },
                    "limits": {
                        "amount": {
                            "min": self.__products[symbol]["limits"]["amount"]["min"] if symbol in self.__products else None,
                            "max": self.__products[symbol]["limits"]["amount"]["max"] if symbol in self.__products else None},
                        "price": {
                            "min": self.__products[symbol]["limits"]["price"]["min"] if symbol in self.__products else None,
                            "max": self.__products[symbol]["limits"]["price"]["max"] if symbol in self.__products else None},
                        "notional": {
                            "min": self.__products[symbol]["limits"]["notional"]["min"] if symbol in self.__products else None,
                            "max": self.__products[symbol]["limits"]["notional"]["max"] if symbol in self.__products else None},
                    },
                    "base_asset_id": product["base"],
                    "base_asset_symbol": product["base"],
                    "quote_asset_id": product["base"],
                    "quote_asset_symbol": product["base"],
                }
        return self.__margin_products

    def margin_get_balance(self, as_base_currency=False):
        if not self.__margin_products:
            self.margin_get_products()

        if as_base_currency:
            rate = CbForex(self.database).get_rate("USD", self.base_currency)
        else:
            rate = 1

        data = {}
        response = self.__api_spot_margin_get_account_information()

        for asset_balances in response['userAssets']:
            if float(asset_balances["free"]) != 0 or float(asset_balances["borrowed"]) != 0:
                symbol = asset_balances["asset"]
                if as_base_currency:
                    usd_price = self.__get_symbol_usd_price(symbol)
                    if usd_price is None:
                        log(f"[{self}] [ERROR] Can't get price for {symbol}")
                        usd_price = 0


                    if float(asset_balances["borrowed"]) == 0:
                        balance = (float(asset_balances["free"]) + float(asset_balances["locked"]))*usd_price*rate
                        available = float(asset_balances["free"])*usd_price*rate
                        borrowed = 0
                    else:
                        balance = float(asset_balances["netAsset"])*usd_price*rate
                        available = float(asset_balances["netAsset"])*usd_price*rate
                        borrowed = float(asset_balances["borrowed"])*usd_price*rate
                    currency = self.base_currency
                else:
                    if float(asset_balances["borrowed"]) == 0:
                        balance = float(asset_balances["free"]) + float(asset_balances["locked"])
                        available = float(asset_balances["free"])
                        borrowed = float(asset_balances["borrowed"])
                    else:
                        balance = float(asset_balances["netAsset"])
                        available = float(asset_balances["netAsset"])
                        borrowed = float(asset_balances["borrowed"])
                    currency = symbol

                data[symbol] = {
                    "symbol": symbol,
                    "balance": balance,
                    "available_balance": available,
                    "borrowed": borrowed,
                    "currency": currency,
                }
        return data

    def margin_get_equity(self):
        btc_price = self.__get_symbol_usd_price("BTC")
        response = self.__api_spot_margin_get_account_information()
        data = {
            "total_net_asset": float(response["totalNetAssetOfBtc"]),
            "equity": float(response["totalNetAssetOfBtc"])*btc_price,
        }
        return data

    def margin_get_mark_price(self, symbol):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_id = self.__margin_products[symbol]["exchange_id"]
        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        response = self.__api_spot_margin_get_price(exchange_symbol)

        data = {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "exchange_symbol": exchange_symbol,
            "price": float(response["price"]),
        }
        return data

    def margin_get_positions(self):
        if not self.__margin_products:
            self.margin_get_products()

        data = {}
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        response = self.__api_spot_margin_get_account_information()
        for asset_balances in response['userAssets']:
            if float(asset_balances["free"]) != 0 or float(asset_balances["borrowed"]) != 0:
                symbol = asset_balances["asset"]
                usd_price = self.__get_symbol_usd_price(symbol)
                if float(asset_balances["borrowed"]) == 0:
                    size = float(asset_balances["free"]) + float(asset_balances["locked"])
                else:
                    size = float(asset_balances["netAsset"])

                data[symbol] = {
                    "symbol": symbol,
                    "exchange_id": symbol,
                    "exchange_symbol": symbol,
                    "size": size,
                    "entry_price": 0,
                    "maintenance_margin": 0,
                    "contract_type": 'spot',
                    "base_asset_id": symbol,
                    "base_asset_symbol": symbol,
                    "quote_asset_id": self.base_currency,
                    "quote_asset_symbol": self.base_currency,
                    "base_currency_amount": size*usd_price*rate,
                    "base_currency_symbol": self.base_currency,
                }
        return data

    def margin_borrow(self, symbol, amount):
        if not self.__margin_assets:
            self.margin_get_assets()
        if symbol not in self.__margin_assets:
            raise UnknownAssetSymbolError(f"Unknown product symbol '{symbol}'")

        response = self.__api_spot_margin_borrow_repay("BORROW", symbol, amount)
        return response

    def margin_hourly_interest_rate(self, symbol, is_isolated=False):
        response = self.__api_spot_margin_hourly_interest_rate(symbol, is_isolated)

        data = {}
        for asset in response:
            data[asset["asset"]] = float(0.00000144)

        return data

    def get_margin_position(self, symbol, is_isolated=False):

        balance = self.margin_get_balance(False)

        base_asset = symbol[:3]

        position = balance[base_asset]["balance"] if base_asset in balance else 0

        return position

    def get_margin_borrow_position(self, symbol, is_isolated=False):

        balance = self.margin_get_balance(False)

        base_asset = symbol[:3]

        borrowed_asset = balance[base_asset]['borrowed'] if base_asset in balance else 0

        return borrowed_asset


    def margin_repay(self, symbol, amount):
        if not self.__margin_assets:
            self.margin_get_assets()
        if symbol not in self.__margin_assets:
            raise UnknownAssetSymbolError(f"Unknown product symbol '{symbol}'")

        response = self.__api_spot_margin_borrow_repay("REPAY", symbol, amount)
        return response

    def margin_place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)

        response = self.__api_spot_margin_place_order(exchange_symbol, size, side.upper(), order_type="market_order", time_in_force=time_in_force)

        data = {
            "id": str(response["orderId"]),
            "type": "market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
        }

        total_price = 0
        total_size = 0
        total_commission = 0
        for fill in response["fills"]:
            total_price += float(fill["price"])*float(fill["qty"])
            total_size += float(fill["qty"])
            total_commission += float(fill["commission"])
        average_price = total_price / total_size

        data["average_price"] = average_price
        data["limit_price"] = None
        data["stop_price"] = None
        data["time_in_force"] = response["timeInForce"].lower()
        data["commission"] = total_commission
        data["created_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)
        data["updated_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)

        self.__log_order(data["id"], symbol, data['size'], side, "market_order", data["status"], average_price)

        return data

    def margin_place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        limit_price = self.__round_price(symbol, limit_price)

        response = self.__api_spot_margin_place_order(exchange_symbol, size, side.upper(), order_type="limit_order", limit_price=limit_price, time_in_force=time_in_force)

        started_at = time.time()

        data = {
            "id": str(response["orderId"]),
            "type": "limit",
        }

        # Wait for order to be placed
        if timeout > 0:
            order_id = str(response["orderId"])
            order_placed = False
            elapsed_time = time.time() - started_at

            while elapsed_time < timeout:
                active_orders = self.margin_get_active_orders(symbol)

                # Order placed
                if not any(order["id"] == order_id for order in active_orders):
                    order_placed = True
                    break

                elapsed_time = time.time() - started_at
                time.sleep(0.1)

            if not order_placed:
                data = self.margin_cancel_order(symbol, order_id)
            else:
                data = self.margin_get_order_state(symbol, order_id)

        # Doesn't wait for order to be placed
        else:
            data["status"] = self.__compose_order_status(response)
            data["symbol"] = symbol
            data["exchange_id"] = response["symbol"]
            data["exchange_symbol"] = response["symbol"]
            data["side"] = response["side"].lower()
            data["size"] = float(response["origQty"])
            data["filled_size"] = float(response["executedQty"])
            data["unfilled_size"] = float(response["origQty"]) - float(response["executedQty"])
            data["average_price"] = None
            data["limit_price"] = response["price"]
            data["stop_price"] = None
            data["time_in_force"] = response["timeInForce"].lower()
            data["commission"] = 0
            data["created_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)
            data["updated_at"] = datetime.datetime.fromtimestamp(response["transactTime"]/1000)

        self.__log_order(data["id"], symbol, data['size'], side, "limit_order", data["status"], response["price"])

        return data

    def margin_place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc", reduce_only=False):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        stop_price = self.__round_price(symbol, stop_price)

        response = self.__api_spot_margin_place_order(exchange_symbol, size, side.upper(), order_type="stop_market_order", stop_price=stop_price, time_in_force=time_in_force)

        data = {
            "id": str(response["orderId"]),
            "type": "stop_market",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": None,
            "limit_price": None,
            "stop_price": response["stopPrice"],
            "time_in_force": response["timeInForce"].lower(),
            "commission": 0,
            "created_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_market_order", data["status"], response["stopPrice"])

        return data

    def margin_place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc", reduce_only=False):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        size = self.__round_size(symbol, size)
        limit_price = self.__round_price(symbol, limit_price)
        stop_price = self.__round_price(symbol, stop_price)

        response = self.__api_spot_margin_place_order(exchange_symbol, size, side.upper(), order_type="stop_limit_order", stop_price=stop_price, limit_price=limit_price, time_in_force=time_in_force)

        data = {
            "id": str(response["orderId"]),
            "type": "stop_limit",
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": None,
            "limit_price": None,
            "stop_price": response["stopPrice"],
            "time_in_force": response["timeInForce"].lower(),
            "commission": 0,
            "created_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["transactTime"]/1000),
        }

        self.__log_order(data["id"], symbol, data['size'], side, "stop_limit_order", data["status"], response["price"])

        return data

    def margin_place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.0075):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
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
            response = self.margin_place_limit_order(symbol, side=side, size=size, limit_price=limit_price)
            log(f"[{self}] {response}")
            limit_order_id = response["id"]
        except Exception as e:
            log(f"[{self}][ERROR] Can't place limit order : {type(e).__name__} - {e}. Place market order instead")
            self.unsubscribe_orders(symbol)
            response = self.margin_place_market_order(symbol, side=side, size=size)
            log(f"{response}")
            return response

        # Place stop market order
        try:
            log(f"[{self}] Place {side} stop market order of {size} on {symbol} at {stop_price}")
            response = self.margin_place_stop_market_order(symbol, side=side, size=size, stop_price=stop_price)
            log(f"[{self}] {response}")
            stop_market_order_id = response["id"]
        except ImmediateExecutionStopOrderError:
            log(f"[{self}][ERROR] Immediate execution of stop order. Cancelling limit order and placing market order instead.")
            self.margin_cancel_order(symbol, limit_order_id)
            self.unsubscribe_orders(symbol)
            response = self.margin_place_market_order(symbol, side=side, size=size)
            log(f"[{self}] {response}")
            return response

        # Check for immediate order filling
        active_orders = self.margin_get_active_orders(symbol)
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
                            self.margin_cancel_order(symbol, stop_market_order_id)
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
                            self.margin_cancel_order(symbol, limit_order_id)
                            log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                        except Exception as e:
                            log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")
                        break

        # An order has been immediatelly placed
        else:
            if limit_order_placed:
                order = self.margin_get_order_state(symbol, limit_order_id)
                try:
                    log(f"[{self}] Cancel stop market order '{stop_market_order_id}'")
                    self.margin_cancel_order(symbol, stop_market_order_id)
                    log(f"[{self}] Stop market order '{stop_market_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel stop market order '{stop_market_order_id}' : {type(e).__name__} - {e}")
            elif stop_order_placed:
                order = self.margin_get_order_state(symbol, stop_market_order_id)
                try:
                    log(f"[{self}] Cancel limit order '{limit_order_id}'")
                    self.margin_cancel_order(symbol, limit_order_id)
                    log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")

        # Unsubscribe orders
        self.unsubscribe_orders(symbol, orders_callback)

        # Timeout
        if not limit_order_placed and not stop_order_placed:
            limit_order_active = False
            stop_market_order_active = False
            active_orders = self.margin_get_active_orders(symbol)
            for order in active_orders:
                if limit_order_id == order["id"]:
                    limit_order_active = True
                if stop_market_order_id == order["id"]:
                    stop_market_order_active = True

            if limit_order_active:
                try:
                    log(f"[{self}] Cancel limit order '{limit_order_id}'")
                    self.margin_cancel_order(symbol, limit_order_id)
                    log(f"[{self}] Limit order '{limit_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel limit order '{limit_order_id}' : {type(e).__name__} - {e}")
            if stop_market_order_active:
                try:
                    log(f"[{self}] Cancel stop market order '{stop_market_order_id}'")
                    self.margin_cancel_order(symbol, stop_market_order_id)
                    log(f"[{self}] Stop market order '{stop_market_order_id}' cancelled")
                except Exception as e:
                    log(f"[{self}][ERROR] Failed to cancel stop market order '{stop_market_order_id}' : {type(e).__name__} - {e}")

            if limit_order_active and stop_market_order_active:
                log(f"[{self}] No order has been placed")
                log(f"[{self}] Place {side} market order of {size} on {symbol}")
                order = self.margin_place_market_order(symbol, side=side, size=size)
                log(f"[{self}] {order}")
            elif not limit_order_active:
                log(f"[{self}] Limit order placed")
                order = self.margin_get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["price"])
            elif not stop_market_order_active:
                log(f"[{self}] Stop market order placed")
                order = self.margin_get_order_state(symbol, limit_order_id)
                log(f"[{self}] {order}")
                if order["status"] == "filled":
                    self.__log_order(order["id"], symbol, order["filled_size"], order["side"], f"{order['type']}_order", order["status"], order["price"])
        else:
            log(f"[{self}] An order has been executed : {order}")

            # Complete order if partially filled
            if order["filled_size"] < size:
                log(f"[{self}] Complete partially filled order with {size-order['filled_size']} {symbol}")
                response = self.margin_place_market_order(symbol, side=side, size=size-order['filled_size'])
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

    def margin_get_active_orders(self, symbol=None):
        if not self.__margin_products:
            self.margin_get_products()

        if symbol:
            if symbol not in self.__margin_products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

            exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]
            response = self.__api_spot_margin_get_active_orders(exchange_symbol)
        else:
            response = self.__api_spot_margin_get_active_orders()

        data = []
        for order in response:
            active_order = {
                "id": str(order["orderId"]),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol if symbol else next((product["symbol"] for product in self.__margin_products.values() if product["exchange_id"] == order["symbol"]), None),
                "exchange_id": order["symbol"],
                "exchange_symbol": order["symbol"],
                "side": order["side"].lower(),
                "size": float(order["origQty"]),
                "filled_size": float(order["executedQty"]),
                "unfilled_size": float(order["origQty"]) - float(order["executedQty"]),
                "average_price": order["avgPrice"] if "avgPrice" in order else None,
                "limit_price": order["price"],
                "stop_price": order["stopPrice"],
                "time_in_force": order["timeInForce"].lower(),
                "created_at" : datetime.datetime.fromtimestamp(order["time"]/1000),
                "updated_at" : datetime.datetime.fromtimestamp(order["updateTime"]/1000),
            }
            data.append(active_order)
        return data

    def margin_get_order_state(self, symbol, order_id):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        response = self.__api_spot_margin_get_order_state(exchange_symbol, order_id)

        data = {
            "id": str(response["orderId"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": response["avgPrice"] if "avgPrice" in response else None,
            "limit_price": response["price"],
            "stop_price": response["stopPrice"] if "stopPrice" in response else None,
            "time_in_force": response["timeInForce"].lower(),
            "created_at" : datetime.datetime.fromtimestamp(response["time"]/1000),
            "updated_at" : datetime.datetime.fromtimestamp(response["updateTime"]/1000),
        }

        return data

    def margin_cancel_order(self, symbol, order_id):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        response = self.__api_spot_margin_cancel_order(exchange_symbol, order_id)

        data = {
            "id": str(response["orderId"]),
            "type": self.__compose_order_type(response),
            "status": self.__compose_order_status(response),
            "symbol": symbol,
            "exchange_id": response["symbol"],
            "exchange_symbol": response["symbol"],
            "side": response["side"].lower(),
            "size": float(response["origQty"]),
            "filled_size": float(response["executedQty"]),
            "unfilled_size": float(response["origQty"]) - float(response["executedQty"]),
            "average_price": response["avgPrice"] if "avgPrice" in response else None,
            "limit_price": response["price"],
            "stop_price": response["stopPrice"] if "stopPrice" in response else None,
            "time_in_force": response["timeInForce"].lower(),
        }

        return data

    def margin_get_order_history(self, symbol):
        if not self.__margin_products:
            self.margin_get_products()
        if symbol not in self.__margin_products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        exchange_symbol = self.__margin_products[symbol]["exchange_symbol"]

        response = self.__api_spot_margin_order_history(exchange_symbol)

        data = []
        for order in response:
            order_refactored = {
                "id": str(order["orderId"]),
                "type": self.__compose_order_type(order),
                "status": self.__compose_order_status(order),
                "symbol": symbol,
                "exchange_id": order["symbol"],
                "exchange_symbol": order["symbol"],
                "side": order["side"].lower(),
                "size": float(order["origQty"]),
                "filled_size": float(order["executedQty"]),
                "unfilled_size": float(order["origQty"]) - float(order["executedQty"]),
                "average_price": order["avgPrice"] if "avgPrice" in order else None,
                "limit_price": order["price"],
                "stop_price": order["stopPrice"] if "stopPrice" in order else None,
                "time_in_force": order["timeInForce"].lower(),
                "created_at" : datetime.datetime.fromtimestamp(order["time"]/1000),
                "updated_at" : datetime.datetime.fromtimestamp(order["updateTime"]/1000),
            }
            data.append(order_refactored)

        return data

    def get_flexible_earn_product_list(self, asset):
        response = self.__api_get_flexible_earn_product_list(asset)
        product_id = response['rows'][0]['productId']

        return product_id

    def get_flexible_earn_position(self, product_id):
        response = self.__api_get_flexible_earn_position(product_id)

        return response

    def subscribe_flexible_earn(self, product_id, amount):
        response = self.__api_subscribe_flexible_earn(product_id, amount)

        return response

    def redeem_flexible_earn(self, product_id, amount):
        response = self.__api_redeem_flexible_earn(product_id, amount)
        return response

    def get_earn_rewards_history(self):

        response = self.__api_get_flexible_reward_history()

        data = []
        for order in response:
            print(order)
            order_refactored = {
                "symbol": order["asset"],
                "amount": float(order["rewards"]),
                "product_id": str(order["productId"]),
                "tx_id": float(order["time"]),
            }
            data.append(order_refactored)


        return data

#################
### Websocket ###
#################

    def connect(self):
        self.__connection_mutex.acquire()
        try:
            if self.__is_connected:
                return

            self.__ws_log("Connecting", "CONNECT")
            # Connect to websocket server
            self.__spot_public_websocket.connect(self.urls["websocket"][BinanceEndpoints.SPOT], timeout=5)
            self.__future_public_websocket.connect(self.urls["websocket"][BinanceEndpoints.FUTURE], timeout=5)

            # Launch reception threads
            self.__spot_public_thread = StoppableThread(name="spot_public_websocket_callback", target=self.__spot_public_websocket_callback)
            self.__spot_public_thread.start()
            self.__future_public_thread = StoppableThread(name="future_public_websocket_callback", target=self.__future_public_websocket_callback)
            self.__future_public_thread.start()

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

            if not self.__connection_lost:
                # Stop reception threads
                if self.__spot_public_thread:
                    self.__spot_public_thread.stop()
                    self.__spot_public_thread.join()
                    self.__spot_public_thread = None
                if self.__future_public_thread:
                    self.__future_public_thread.stop()
                    self.__future_public_thread.join()
                    self.__future_public_thread = None

                # Disconnect websocket
                if self.__spot_public_websocket:
                    self.__spot_public_websocket.close()
                if self.__future_public_websocket:
                    self.__future_public_websocket.close()

                # Stop user data thread
                if self.__spot_private_thread:
                    self.__spot_private_thread.stop()
                    self.__spot_private_thread.join()
                    self.__spot_private_thread = None
                if self.__future_private_thread:
                    self.__future_private_thread.stop()
                    self.__future_private_thread.join()
                    self.__future_private_thread = None

                # Disconnect user data websocket
                if self.__spot_private_websocket:
                    self.__spot_private_websocket.close()
                    self.__spot_private_websocket = None
                if self.__future_private_websocket:
                    self.__future_private_websocket.close()
                    self.__future_private_websocket = None

                # Stop auth timer threads
                if self.__is_spot_auth:
                    self.__spot_unauth()
                if self.__is_future_auth:
                    self.__future_unauth()

                # Close websocket connection
                self.__spot_public_websocket.close()
                self.__future_public_websocket.close()
            else:
                if self.__spot_public_thread:
                    self.__spot_public_thread.stop()
                if self.__future_public_thread:
                    self.__future_public_thread.stop()
                if self.__spot_private_thread:
                    self.__spot_private_thread.stop()
                if self.__future_private_thread:
                    self.__future_private_thread.stop()

            self.__is_connected = False
            self.__is_spot_auth = False
            self.__is_future_auth = False
            self.__ws_log("Disconnected", "DISCONNECT")
        finally:
            self.__connection_mutex.release()

    def cleanup(self, force=False):
        self.disconnect()

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

#######################
### Public channels ###
#######################

    def subscribe_orderbook(self, symbols, callback=None, depth=20):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                """
                interval = "1000ms"
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@depth{depth}@{interval}",
                    "symbol": symbol,
                })
                """
                interval = "100ms"
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@depth@{interval}",
                    "symbol": symbol,
                })

                self.__spot_orderbooks[symbol] = {
                    "symbol": symbol,
                    "buy": {},
                    "sell": {},
                }

                orderbook = self.get_orderbook(symbol)
                for order in orderbook["buy"]:
                    self.__spot_orderbooks[symbol]["buy"][order["price"]] = order["size"]
                for order in orderbook["sell"]:
                    self.__spot_orderbooks[symbol]["sell"][order["price"]] = order["size"]

            elif self.__is_product_future(symbol):
                depth = 20
                interval = "100ms"
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@depth{depth}@{interval}",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_subscribe(spot_channels, "depthUpdate", callback=callback)
        if future_channels:
            self.__future_public_subscribe(future_channels, "depthUpdate", callback=callback)

    def unsubscribe_orderbook(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                interval = "1000ms"
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@depth@{interval}",
                    "symbol": symbol,
                })
            elif self.__is_product_future(symbol):
                depth = 20
                interval = "100ms"
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@depth{depth}@{interval}",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_unsubscribe(spot_channels, "depthUpdate")
        if future_channels:
            self.__future_public_unsubscribe(future_channels, "depthUpdate")

    def subscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@markPrice@1s",
                    "symbol": symbol,
                })
            elif self.__is_product_future(symbol):
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@markPrice@1s",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_subscribe(spot_channels, "markPriceUpdate", callback=callback)
        if future_channels:
            self.__future_public_subscribe(future_channels, "markPriceUpdate", callback=callback)

    def unsubscribe_mark_price(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@markPrice@1s",
                    "symbol": symbol,
                })
            elif self.__is_product_future(symbol):
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@markPrice@1s",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_unsubscribe(spot_channels, "markPriceUpdate")
        if future_channels:
            self.__future_public_unsubscribe(future_channels, "markPriceUpdate")

    def subscribe_trades(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@trade",
                    "symbol": symbol,
                })
            elif self.__is_product_future(symbol):
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@trade",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_subscribe(spot_channels, "trade", callback=callback)
        if future_channels:
            self.__future_public_subscribe(future_channels, "trade", callback=callback)

    def unsubscribe_trades(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_channels = []
        future_channels = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
            else:
                exchange_symbol = self.__products[symbol]["exchange_symbol"]

            if self.__is_product_spot(symbol):
                spot_channels.append({
                    "channel": f"{exchange_symbol.lower()}@trade",
                    "symbol": symbol,
                })
            elif self.__is_product_future(symbol):
                future_channels.append({
                    "channel": f"{exchange_symbol.lower()}@trade",
                    "symbol": symbol,
                })

        if spot_channels:
            self.__spot_public_unsubscribe(spot_channels, "trade")
        if future_channels:
            self.__future_public_unsubscribe(future_channels, "trade")

#########################
### Privates channels ###
#########################

    def subscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_symbols = []
        future_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

            if self.__is_product_spot(symbol):
                spot_symbols.append(symbol)
            elif self.__is_product_future(symbol):
                future_symbols.append(symbol)

        if spot_symbols:
            self.__spot_private_subscribe("ORDER_UPDATE", spot_symbols, callback=callback)
        if future_symbols:
            self.__future_private_subscribe("ORDER_UPDATE", future_symbols, callback=callback)

    def unsubscribe_orders(self, symbols, callback=None):
        if not self.__products:
            self.get_products()

        if isinstance(symbols, str):
            symbols = [symbols]

        spot_symbols = []
        future_symbols = []
        for symbol in symbols:
            if symbol not in self.__products:
                raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

            if self.__is_product_spot(symbol):
                spot_symbols.append(symbol)
            elif self.__is_product_future(symbol):
                future_symbols.append(symbol)

        if spot_symbols:
            self.__spot_private_unsubscribe("ORDER_UPDATE", spot_symbols)
        if future_symbols:
            self.__future_private_unsubscribe("ORDER_UPDATE", future_symbols)

########################
### Privates methods ###
########################

################
### REST API ###
################

    def get_networks(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/capital/config/getall", auth=True)
        return response

    def asset_recovery_pre_check(self, tx_id, network, coin, to_address, amount):
        headers = {}
        payload = {
            "txId": tx_id,
            "network": network,
            "coin": coin,
            "toAddress": to_address,
            "amount": amount,
        }

        headers["X-MBX-APIKEY"] = self.api_key
        recv_window = 5000
        timestamp = self.__get_timestamp()

        query = {
            "recvwindow": recv_window,
            "timestamp": timestamp,
        }

        query_string = self.__query_string(query)
        payload_str=json.dumps(payload, separators=(',', ':'))

        def hashing(query_string):
            return hmac.new(
                self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
            ).hexdigest()

        signature = hashing(query_string)

        url = f"https://api.binance.com/sapi/v1/capital/deposit-appeal/appeal-pre-check?{query_string}&signature={signature}"

        response = self.session.request("POST",
                            url,
                            data=payload_str,
                            headers=headers)

        self.__request_log(response)

        return response.json()

    def asset_recovery_confirm(self, request_id, target_address=None, target_address_tag=None, expected_amount=None):
        headers = {}
        payload = {
            "requestId": request_id,
        }
        if target_address:
            payload["targetAddress"] = target_address
        if target_address_tag:
            payload["targetAddressTag"] = target_address_tag
        if expected_amount:
            payload["expectedAmount"] = expected_amount

        headers["X-MBX-APIKEY"] = self.api_key
        recv_window = 5000
        timestamp = self.__get_timestamp()

        query = {
            "recvwindow": recv_window,
            "timestamp": timestamp,
        }

        query_string = self.__query_string(query)
        payload_str=json.dumps(payload, separators=(',', ':'))

        def hashing(query_string):
            return hmac.new(
                self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
            ).hexdigest()

        signature = hashing(query_string)

        url = f"https://api.binance.com/sapi/v1/capital/deposit-appeal/appeal-confirm?{query_string}&signature={signature}"

        response = self.session.request("POST",
                            url,
                            data=payload_str,
                            headers=headers)

        self.__request_log(response)

        return response.json()

    def asset_recovery_history(self, request_id):
        headers = {}

        headers["X-MBX-APIKEY"] = self.api_key
        recv_window = 5000
        timestamp = self.__get_timestamp()

        query = {
            "requestId": request_id,
            "requestType": 1,
            "recvwindow": recv_window,
            "timestamp": timestamp,
        }

        query_string = self.__query_string(query)

        def hashing(query_string):
            return hmac.new(
                self.api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
            ).hexdigest()

        signature = hashing(query_string)

        url = f"https://api.binance.com/sapi/v1/capital/deposit-appeal/appeal-history?{query_string}&signature={signature}"

        response = self.session.request("POST",
                            url,
                            headers=headers)

        self.__request_log(response)

        return response.json()

    def __api_spot_get_trade_fee(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/asset/tradeFee", auth=True)
        return response

    def __api_spot_get_account_information(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/account", auth=True)
        return response

    def __api_spot_get_key_restriction(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/account/apiRestrictions", auth=True)
        return response

    def __api_spot_get_exchange_info(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/exchangeInfo")
        return response

    def __api_future_get_exchange_info(self):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/exchangeInfo")
        return response

    def __api_spot_get_orderbook(self, symbol, depth=None):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload={
            "symbol": str(symbol),
        }
        if depth:
            payload["limit"] = depth
        response = self.__request("GET", url, "/api/v3/depth", payload)
        return response

    def __api_future_get_orderbook(self, symbol):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/depth", payload={
            "symbol": str(symbol)
        })
        return response

    def __api_spot_get_spot_price(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/ticker/price", payload={
            "symbol": symbol,
        })
        return response

    def __api_spot_get_mark_price(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/avgPrice", payload={
            "symbol": str(symbol)
        })
        return response

    def __api_future_get_mark_price(self, symbol):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/premiumIndex", payload={
            "symbol": str(symbol)
        })
        return response

    def __api_future_get_account_information(self):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v2/account", auth=True)
        return response

    def __api_future_get_position_information(self, symbol):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v2/positionRisk", payload={
            "symbol": symbol
        }, auth=True)
        return response

    def __api_future_set_position_margin(self, symbol, delta_margin):
        url = self.urls["api"][BinanceEndpoints.FUTURE]

        amount = abs(float(delta_margin))
        margin_type = 1 if float(delta_margin) > 0 else 2

        response = self.__request("POST", url, "/fapi/v1/positionMargin", payload={
            "symbol": str(symbol),
            "amount": str(amount),
            "type": str(margin_type),
        }, auth=True)
        return response

    def __api_spot_place_order(self, product_symbol, size, side, order_type, limit_price=None, stop_price=None, time_in_force="gtc"):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "symbol": str(product_symbol),
            "side": str(side),
            "quantity": np.format_float_positional(size, trim="-"),
        }

        if order_type == "market_order":
            payload["type"] = "MARKET"
            payload["newOrderRespType"] = "FULL"
        elif order_type == "limit_order":
            payload["type"] = "LIMIT"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            mark_price = float(self.__api_spot_get_mark_price(product_symbol)["price"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP_LOSS"
                else:
                    payload["type"] = "TAKE_PROFIT"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT"
                else:
                    payload["type"] = "STOP_LOSS"
            payload["stopPrice"] = stop_price
        elif order_type == "stop_limit_order":
            mark_price = float(self.__api_spot_get_mark_price(product_symbol)["price"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP_LIMIT"
                else:
                    payload["type"] = "TAKE_PROFIT_LIMIT"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT_LIMIT"
                else:
                    payload["type"] = "STOP_LIMIT"
            payload["price"] = limit_price
            payload["stopPrice"] = stop_price

        if order_type != "market_order" and order_type != "stop_market_order":
            if time_in_force == "gtc":
                payload["timeInForce"] = "GTC"
            elif time_in_force == "fok":
                payload["timeInForce"] = "FOK"
            elif time_in_force == "ioc":
                payload["timeInForce"] = "IOC"

        response = self.__request("POST", url, "/api/v3/order", payload=payload, auth=True)
        return response

    def __api_future_place_order(self, product_symbol, size, side, order_type, limit_price=None, stop_price=None, time_in_force="gtc"):
        url = self.urls["api"][BinanceEndpoints.FUTURE]

        payload = {
            "symbol": str(product_symbol),
            "side": str(side),
            "quantity": size,
        }

        if order_type == "market_order":
            payload["type"] = "MARKET"
            payload["newOrderRespType"] = "RESULT"
        elif order_type == "limit_order":
            payload["type"] = "LIMIT"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            mark_price = float(self.__api_future_get_mark_price(product_symbol)["markPrice"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP_MARKET"
                else:
                    payload["type"] = "TAKE_PROFIT_MARKET"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT_MARKET"
                else:
                    payload["type"] = "STOP_MARKET"
            payload["stopPrice"] = stop_price
        elif order_type == "stop_limit_order":
            mark_price = float(self.__api_future_get_mark_price(product_symbol)["markPrice"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP"
                else:
                    payload["type"] = "TAKE_PROFIT"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT"
                else:
                    payload["type"] = "STOP"
            payload["price"] = limit_price
            payload["stopPrice"] = stop_price

        if order_type != "market_order":
            if time_in_force == "gtc":
                payload["timeInForce"] = "GTC"
            elif time_in_force == "fok":
                payload["timeInForce"] = "FOK"
            elif time_in_force == "ioc":
                payload["timeInForce"] = "IOC"

        response = self.__request("POST", url, "/fapi/v1/order", payload=payload, auth=True)
        return response

    def __api_spot_get_active_orders(self, symbol=None):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {}
        if symbol:
            payload["symbol"] = str(symbol)
        response = self.__request("GET", url, "/api/v3/openOrders", payload=payload, auth=True)
        return response

    def __api_spot_get_trade_history(self, symbol, trade_id, limit=1000):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/historicalTrades", payload={
            "symbol": str(symbol),
            "fromId": trade_id,
            "limit": limit,
        })
        return response

    def __api_get_trade_history(self, symbol, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/myTrades", payload={
            "symbol": symbol,
            "startTime": start_date,
            "endTime": end_date,
        }, auth=True)
        return response

    def __api_spot_get_historical_trades(self, symbol, from_id=None, limit=1000):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {
            "symbol": symbol,
            "limit": limit,
        }
        if from_id:
            payload["fromId"] = from_id

        response = self.__request("GET", url, "/api/v3/historicalTrades", payload=payload)
        return response

    def __api_future_get_active_orders(self, symbol=None):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        payload = {}
        if symbol:
            payload["symbol"] = str(symbol)
        response = self.__request("GET", url, "/fapi/v1/openOrders", payload=payload, auth=True)
        return response

    def __get_order_state(self, symbol, order_id):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        else:
            exchange_id = self.__products[symbol]["exchange_id"]
            exchange_symbol = self.__products[symbol]["exchange_symbol"]

        if self.__is_product_spot(symbol):
            response = self.__api_spot_get_order_state(exchange_symbol, order_id)
        elif self.__is_product_future(symbol):
            response = self.__api_future_get_order_state(exchange_symbol, order_id)
        else:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")

        return response

    def __api_spot_get_order_state(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/order", payload={
            "symbol": str(symbol),
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_future_get_order_state(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/order", payload={
            "symbol": str(symbol),
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_spot_cancel_order(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("DELETE", url, "/api/v3/order", payload={
            "symbol": str(symbol),
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_future_cancel_order(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("DELETE", url, "/fapi/v1/order", payload={
            "symbol": str(symbol),
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_future_get_position_margin_history(self, symbol):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/positionMargin/history", payload={
            "symbol": symbol,
        }, auth=True)
        return response

    def __api_spot_get_order_history(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/allOrders", payload={
            "symbol": str(symbol)
        }, auth=True)
        return response

    def __api_future_get_order_history(self, symbol):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("GET", url, "/fapi/v1/allOrders", payload={
            "symbol": str(symbol)
        }, auth=True)
        return response

    def __api_future_set_leverage(self, symbol, leverage):
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        response = self.__request("POST", url, "/fapi/v1/leverage", payload={
            "symbol": str(symbol),
            "leverage": int(leverage),
        }, auth=True)
        return response

    def __api_get_staking_product_list(self, product="STAKING"):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/staking/productList",payload={
            "product": product,
            "size": 100,
        }, auth=True)
        return response

    def __api_redeem_staking_product(self, product, product_id, position_id=0, amount=0):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "product": product,
            "productId": product_id,
            "amount": amount,
        }

        if product == "STAKING":
            payload["positionId"] = position_id
        elif product == "L_DEFI":
            payload["positionId"] = position_id
        elif product == "F_DEFI":
            payload["amount"] = amount

        response = self.__request("POST", url, "/sapi/v1/staking/redeem", payload=payload, auth=True)
        return response

    def __api_purchase_staking_product(self, product, product_id, amount=0):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "product": product,
            "productId": product_id,
            "amount": amount,
        }

        response = self.__request("POST", url, "/sapi/v1/staking/purchase", payload=payload, auth=True)

        return response

    def __api_get_flexible_earn_product_list(self, asset):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {
            "asset": asset,
        }
        response = self.__request("GET", url, "/sapi/v1/simple-earn/flexible/list", payload=payload, auth=True)
        print(response)
        return response

    def __api_subscribe_flexible_earn(self, product_id, amount=0):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "productId": product_id,
            "amount": amount,
            "sourceAccount" : "ALL"
        }

        response = self.__request("POST", url, "/sapi/v1/simple-earn/flexible/subscribe", payload=payload, auth=True)

        return response

    def __api_redeem_flexible_earn(self, product_id, amount=0):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "productId": product_id,
            "amount": amount,
            "destAccount" : "SPOT"
        }

        response = self.__request("POST", url, "/sapi/v1/simple-earn/flexible/redeem", payload=payload, auth=True)

        return response

    def __api_get_flexible_earn_position(self, asset):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "asset": asset,
        }

        response = self.get_earn_positions()
        if 'LDUSDC' not in response:
            response = {
                'size': 0,
                'latestAnnualPercentageRate': 0,
                'cumulativeRealTimeRewards': 0,
                'cumulativeTotalRewards': 0
            }
        else:
            response = {
                'size': float(response['LDUSDC']['size']),
                'latestAnnualPercentageRate': 0,
                'cumulativeRealTimeRewards': 0,
                'cumulativeTotalRewards': 0,
            }
        return response

    def __api_get_personnal_left_quota(self, product_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "productId": product_id,
            "amount": 5000,
        }

        print('product_id',product_id)

        #response = self.__request("POST", url, "/sapi/v1/simple-earn/flexible/personalLeftQuota", payload=payload, auth=True)
        response = self.__request("POST", url, "/sapi/v1/simple-earn/flexible/subscriptionPreview", payload=payload, auth=True)

        return response

    def __api_get_flexible_reward_history(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "type": 'REALTIME',
        }

        response = self.__request("GET", url, "/sapi/v1/simple-earn/flexible/history/rewardsRecord", payload=payload, auth=True)
        if float(response['total']) == 0:
            response = {

            }
        else:
            response = response['rows']
        print('response',response)
        return response

    def __api_get_flexible_earn_balance(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {}

        response = self.__request("GET", url, "/sapi/v1/simple-earn/account", payload=payload, auth=True)

        return response

    def __api_staking_quota_left(self, product, product_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "product": product,
            "productId": product_id,
        }

        response = self.__request("GET", url, "/sapi/v1/staking/personalLeftQuota",payload=payload, auth=True)
        return response

    def __api_get_staking_product_position(self, product="STAKING"):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/staking/position",payload={
            "product": product
        }, auth=True)
        return response

    def __api_get_left_quota(self, product_id, product="STAKING"):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "product": product,
            "productId": product_id,
        }

        response = self.__request("GET", url, "/sapi/v1/staking/position",payload=payload, auth=True)
        return response

    def __api_transfer_account(self, asset, amount, type):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        if type == 'from_spot_to_future':
            type = 1
        if type == 'from_future_to_spot':
            type = 2

        payload = {
            "asset": asset,
            "amount": amount,
            "type" : type
        }

        response = self.__request("POST", url, "/sapi/v1/futures/transfer",payload=payload, auth=True)
        return response

    def __api_spot_get_deposit_address(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/capital/deposit/address", payload={
            "coin": symbol,
        }, auth=True)
        return response

    def __api_spot_withdraw(self, symbol, amount, address, network_name=None):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload={
            "coin": symbol,
            "amount": amount,
            "address": address,
        }
        if network_name:
            payload["network"] = network_name

        response = self.__request("POST", url, "/sapi/v1/capital/withdraw/apply", payload, auth=True)
        return response

    def __api_spot_get_fiat_deposit_withdraw_history(self, transaction_type, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        transaction_type = 1 if transaction_type == "withdraw" else 0

        response = self.__request("GET", url, "/sapi/v1/fiat/orders", payload={
            "transactionType": transaction_type,
            "beginTime": round(start_date.timestamp())*1000,
            "endTime": round(end_date.timestamp())*1000,
            "rows": 500,
        }, auth=True)
        return response

    def __api_spot_get_fiat_payments(self, transaction_type, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        transaction_type = 1 if transaction_type == "withdraw" else 0

        response = self.__request("GET", url, "/sapi/v1/fiat/payments", payload={
            "transactionType": transaction_type,
            "beginTime": round(start_date.timestamp())*1000,
            "endTime": round(end_date.timestamp())*1000,
            "rows": 500,
        },auth=True)
        return response

    def __api_spot_get_deposit_history(self, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/capital/deposit/hisrec", payload={
            "startTime": round(start_date.timestamp())*1000,
            "endTime": round(end_date.timestamp())*1000,
        }, auth=True)
        return response

    def __api_spot_get_withdraw_history(self, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/capital/withdraw/history", payload={
            "startTime": round(start_date.timestamp())*1000,
            "endTime": round(end_date.timestamp())*1000,
        }, auth=True)
        return response

    def __api_spot_get_pay_history(self, start_date, end_date):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/pay/transactions", payload={
            "startTime": round(start_date.timestamp())*1000,
            "endTime": round(end_date.timestamp())*1000,
        }, auth=True)
        return response

    def __api_get_masteraccount_transfers(self, start_date, end_date):
        response = response = self.__request("GET",
            self.urls["api"][BinanceEndpoints.SPOT],
            "/sapi/v1/sub-account/sub/transfer/history",
            payload = {
                "startTime": round(start_date.timestamp())*1000,
                "endTime": round(end_date.timestamp())*1000,
            }, auth=True)
        return response

    def __api_get_subaccount_transfers(self, start_date, end_date, type):
        response = self.__request("GET",
            self.urls["api"][BinanceEndpoints.SPOT],
            "/sapi/v1/sub-account/transfer/subUserHistory",
            payload={
                "startTime": round(start_date.timestamp())*1000,
                "endTime": round(end_date.timestamp())*1000,
                "type": type,
            }, auth=True)

        return response

    def __api_spot_get_coin_information(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/capital/config/getall", auth=True)
        return response

    def __api_spot_get_candle(self, symbol, start_time, end_time, resolution):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/api/v3/klines",{
            "symbol": symbol,
            "interval": resolution,
            "startTime": start_time,
            "endTime": end_time,
        })
        return response

    def __api_spot_make_universal_transfer(self, type, symbol, amount):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("POST", url, "/sapi/v1/asset/transfer", {
            "type": type,
            "asset": symbol,
            "amount": amount,
        }, auth=True)
        return response

    def __api_spot_margin_get_account_information(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/account", auth=True)
        return response

    def __api_spot_margin_get_assets(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/allAssets", auth=True)
        return response

    def __api_spot_margin_get_asset(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/asset", {
            "asset": symbol,
        }, auth=True)
        return response

    def __api_spot_margin_get_products(self):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/allPairs", auth=True)
        return response

    def __api_spot_margin_get_product(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/pair", {
            "symbol": symbol,
        }, auth=True)
        return response

    def __api_spot_margin_get_price(self, symbol):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/priceIndex", {
            "symbol": symbol,
        }, auth=True)
        return response

    def __api_spot_margin_borrow_repay(self, type, asset, amount, is_isolated=False, symbol=None):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {
            "type": type,
            "asset": asset,
            "amount": amount,
            "isIsolated": "TRUE" if is_isolated else "FALSE",
        }
        if is_isolated:
            payload["symbol"] = symbol
        response = self.__request("POST", url, "/sapi/v1/margin/borrow-repay", payload=payload, auth=True)
        return response

    def __api_spot_margin_hourly_interest_rate(self, symbol, is_isolated=False):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {
            "assets": symbol,
            "isIsolated": "TRUE" if is_isolated else "FALSE",
        }
        if is_isolated:
            payload["symbol"] = symbol
        response = self.__request("GET", url, "/sapi/v1/margin/next-hourly-interest-rate", payload=payload, auth=True)
        return response

    def __api_spot_margin_place_order(self, symbol, size, side, order_type, limit_price=None, stop_price=None, time_in_force="gtc"):
        url = self.urls["api"][BinanceEndpoints.SPOT]

        payload = {
            "symbol": symbol,
            "side": side,
            "quantity": str(size),
        }

        if order_type == "market_order":
            payload["type"] = "MARKET"
            payload["newOrderRespType"] = "FULL"
        elif order_type == "limit_order":
            payload["type"] = "LIMIT"
            payload["price"] = limit_price
        elif order_type == "stop_market_order":
            mark_price = float(self.__api_spot_get_mark_price(symbol)["price"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP_LOSS"
                else:
                    payload["type"] = "TAKE_PROFIT"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT"
                else:
                    payload["type"] = "STOP_LOSS"
            payload["stopPrice"] = stop_price
        elif order_type == "stop_limit_order":
            mark_price = float(self.__api_spot_get_mark_price(symbol)["price"])
            if stop_price > mark_price:
                if side == "BUY":
                    payload["type"] = "STOP_LIMIT"
                else:
                    payload["type"] = "TAKE_PROFIT_LIMIT"
            else:
                if side == "BUY":
                    payload["type"] = "TAKE_PROFIT_LIMIT"
                else:
                    payload["type"] = "STOP_LIMIT"
            payload["price"] = limit_price
            payload["stopPrice"] = stop_price

        if order_type != "market_order" and order_type != "stop_market_order":
            if time_in_force == "gtc":
                payload["timeInForce"] = "GTC"
            elif time_in_force == "fok":
                payload["timeInForce"] = "FOK"
            elif time_in_force == "ioc":
                payload["timeInForce"] = "IOC"

        response = self.__request("POST", url, "/sapi/v1/margin/order", payload=payload, auth=True)
        return response

    def __api_spot_margin_get_active_orders(self, symbol=None):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {}
        if symbol:
            payload["symbol"] = str(symbol)
        response = self.__request("GET", url, "/sapi/v1/margin/openOrders", payload=payload, auth=True)
        return response

    def __api_spot_margin_get_order_state(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/order", payload={
            "symbol": str(symbol),
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_spot_margin_cancel_order(self, symbol, order_id):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("DELETE", url, "/sapi/v1/margin/order", payload={
            "symbol": symbol,
            "orderId": order_id,
        }, auth=True)
        return response

    def __api_spot_margin_order_history(self, symbol, is_isolated=False):
        url = self.urls["api"][BinanceEndpoints.SPOT]
        response = self.__request("GET", url, "/sapi/v1/margin/allOrders", payload={
            "symbol": symbol,
            "isIsolated": "TRUE" if is_isolated else "FALSE",
        }, auth=True)
        return response

    def __get_symbol_usd_price(self, symbol, date=None):
        if not self.__products:
            self.get_products()
        price = 0
        convs = ["USDT", "BUSD", "USDC", "USD"]

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

    def __round_price(self, symbol, price):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif self.__products[symbol]["limits"]["price"]["min"]:
            rounded_price = float(Decimal(str(price)) - Decimal(str(price)) % Decimal(str(self.__products[symbol]["limits"]["price"]["min"])))
            return rounded_price
        else:
            return price

    def __round_size(self, symbol, size):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            raise UnknownProductSymbolError(f"Unknown product symbol '{symbol}'")
        elif self.__products[symbol]["limits"]["amount"]["min"]:
            rounded_size = float(Decimal(str(size)) - Decimal(str(size)) % Decimal(str(self.__products[symbol]["limits"]["amount"]["min"])))
            return rounded_size
        else:
            return size

    def __request(self, method, base_url, path, payload=None, query=None, auth=False, headers=None):
        if payload is None:
            payload = {}
        if headers is None:
            headers = {}

        # Check weights before request
        if path in self.__weights:
            header = self.__weights[path]["header"]
            path_weight = self.__weights[path]["weight"]
            current_weight = self.__current_weights[header]
            if current_weight + path_weight > self.__weight_limits[header]["limit"]:
                timeout = self.__weight_limits[header]["timeout"]
                log(f"[{self}][ERROR] Issuing request on '{path}' would overflow weight limit ({current_weight + path_weight} / {self.__weight_limits[header]['limit']}). Waiting {timeout} seconds...")
                time.sleep(timeout)
                self.__current_weights[header] = 0

        url = '%s%s' % (base_url, path)

        if auth:
            payload["timestamp"] = self.__get_timestamp()
            query_string = self.__query_string(payload)
            if self.api_key is None or self.api_secret is None:
                raise Exception('Api_key or Api_secret missing')
            signature = self.__generate_signature(query_string)
            payload['signature'] = signature

        params = {"url": url}
        if payload:
            params["params"] = self.__query_string(payload)

        response = self._dispatch_request(method)(**params)

        self.__request_log(response)

        parsed_response = self.__parse_response(response)

        return parsed_response

    def _dispatch_request(self, http_method):
        return {
            "GET": self.session.get,
            "DELETE": self.session.delete,
            "PUT": self.session.put,
            "POST": self.session.post,
        }.get(http_method, "GET")

    def __parse_response(self, response):
        status_code = response.status_code
        headers = response.headers

        # Update weights
        for header in self.__weight_limits.keys():
            if header in headers:
                self.__current_weights[header] = int(headers[header])

        if status_code != 200:
            if status_code == 429:
                raise requests.exceptions.HTTPError("{} : Too Many Requests, need to wait {} s ==> headers : '{}'".format(status_code, response.headers["Retry-After"], response.headers))
            if status_code == 404:
                raise requests.exceptions.HTTPError("{} : Not Found".format(status_code))
            else:
                try:
                    response_json = response.json()
                except:
                    raise requests.exceptions.HTTPError(status_code)
                if "Invalid API-key, IP, or permissions for action" in response_json["msg"]:
                    raise UnauthorizedError
                elif "Invalid symbol." in response_json["msg"]:
                    raise InvalidSymbolError
                elif "This endpoint is allowed to request from sub account only" in response_json["msg"]:
                    raise SubAccountOnlyEndpointError
                elif "Sub-account function is not enabled" in response_json["msg"]:
                    raise MasterAccountOnlyEndpointError
                elif "Stop loss orders are not supported for this symbol" in response_json["msg"]:
                    raise StopLossOrderNotSupportedError
                elif "Take profit orders are not supported for this symbol" in response_json["msg"]:
                    raise TakeProfitOrderNotSupportedError
                elif "Unsupported operation" in response_json["msg"]:
                    raise UnsupportedOperationError
                elif "Account has insufficient balance for requested action" in response_json["msg"]:
                    raise NotEnoughFundsError
                elif "Filter failure: MARKET_LOT_SIZE" in response_json["msg"]:
                    raise InvalidSizeError
                raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["msg"]))
        else:
            if self.urls["api"][BinanceEndpoints.VANILLA] in response.url:
                data = response.json()
                if data["code"] != 0:
                    raise requests.exceptions.HTTPError(response["msg"])
                elif "data" in data:
                    return data["data"]
                else:
                    return {}
            else:
                return response.json()

    def __get_timestamp(self):
        return int(time.time() * 1000)

    def __generate_signature(self, data):
        hash = hmac.new(self.api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()

    def __query_string(self, query):
        if query == None:
            return ''
        else:
            filtered_query = {}
            for key in query.keys():
                if query[key] is not None:
                    filtered_query[key] = query[key]
            return urlencode(filtered_query, True).replace("%40", "@")

    def __is_product_spot(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            is_spot = False
        elif self.__products[symbol]["contract_type"] == "spot":
            is_spot = True
        else:
            is_spot = False
        return is_spot

    def __is_product_future(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            is_future = False
        elif (self.__products[symbol]["contract_type"] == "future" or
            self.__products[symbol]["contract_type"] == "perpetual_future"):
            is_future = True
        else:
            is_future = False
        return is_future

    def __is_product_staking(self, symbol):
        if not self.__products:
            self.get_products()
        if symbol not in self.__products:
            is_staking = False
        elif self.__products[symbol]["contract_type"] == "staking":
            is_staking = True
        else:
            is_staking = False
        return is_staking

    def __compose_order_type(self, response):
        order_type = "unknown"
        if response["type"] == "MARKET":
            order_type = "market"
        elif response["type"] == "LIMIT" or response["type"] == "LIMIT_MAKER":
            order_type = "limit"
        elif response["type"] == "STOP_LOSS":
            order_type = "stop_market"
        elif response["type"] == "STOP_LOSS_LIMIT":
            order_type = "stop_limit"
        elif response["type"] == "STOP_MARKET":
            order_type = "stop_market"
        elif response["type"] == "STOP":
            order_type = "stop_limit"
        return order_type

    def __compose_order_status(self, response):
        order_status = "unknown"
        if response["status"] == "NEW":
            order_status = "open"
        elif response["status"] == "FILLED":
            order_status = "filled"
        elif response["status"] == "PARTIALLY_FILLED":
            order_status = "partially_filled"
        elif response["status"] == "CANCELED":
            order_status = "cancelled"
        return order_status

    def __compose_spot_uniformized_symbol(self, symbol):
        #TODO: Find a way to differenciate spots and futures having the same exchange symbol
        uniformized_products = [product for key, product in self.__products.items() if product["exchange_symbol"] == symbol]

        if len(uniformized_products) > 1:
            uniformized_symbol = [product["symbol"] for product in uniformized_products if product["contract_type"] == "spot"][0]
        else:
            uniformized_symbol = uniformized_products[0]["symbol"]
        return uniformized_symbol

    def __compose_future_uniformized_symbol(self, symbol):
        #TODO: Find a way to differenciate spots and futures having the same exchange symbol
        uniformized_products = [product for key, product in self.__products.items() if product["exchange_symbol"] == symbol]

        if len(uniformized_products) > 1:
            uniformized_symbol = [product["symbol"] for product in uniformized_products if product["contract_type"] == "perpetual_future"][0]
        else:
            uniformized_symbol = uniformized_products[0]["symbol"]
        return uniformized_symbol

    def __log_order(self, order_id, symbol, size, side, order_type, status, execution_price):
        try:
            self.strategy.log_trade(order_id, symbol, size, side, order_type, status, execution_price)
        except Exception as e:
            log(f"[{self}][ERROR] Unable to log order : {type(e).__name__} - {e}\n{format_traceback(e.__traceback__)}")

#################
### Websocket ###
#################

    def __spot_public_subscribe(self, channels, event, callback=None):
        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Subscribing to '{channels}'", "SUBSCRIBE")

        data = {
            "method": "SUBSCRIBE",
            "params": [],
        }

        for channel in channels:
            data["params"].append(channel["channel"])

        self.__ws_send(self.__spot_public_websocket, data)

        self.__public_callbacks_mutex.acquire()
        try:
            if event not in self.__public_callbacks:
                self.__public_callbacks[event] = {}

            for channel in channels:
                self.__public_callbacks[event][channel["symbol"]] = {
                    "channel": channel["channel"],
                    "callback": callback,
                }
        finally:
            self.__public_callbacks_mutex.release()

        self.__ws_log(f"Subscribed to '{channels}'", "SUBSCRIBE")

    def __spot_public_unsubscribe(self, channels, event):
        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Unsubscribing from '{channels}'", "UNSUBSCRIBE")

        data = {
            "method": "UNSUBSCRIBE",
            "params": [],
        }

        for channel in channels:
            data["params"].append(channel["channel"])

        self.__ws_send(self.__spot_public_websocket, data)

        self.__public_callbacks_mutex.acquire()
        try:
            for channel in channels:
                if (event in self.__public_callbacks and
                    channel["symbol"] in self.__public_callbacks[event]):
                    del self.__public_callbacks[event][channel["symbol"]]
                    if not self.__public_callbacks[event]:
                        del self.__public_callbacks[event]
        finally:
            self.__public_callbacks_mutex.release()

        self.__ws_log(f"Unsubscribed from '{channels}'", "UNSUBSCRIBE")

    def __future_public_subscribe(self, channels, event, callback=None):
        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Subscribing to '{channels}'", "SUBSCRIBE")

        data = {
            "method": "SUBSCRIBE",
            "params": [],
        }

        for channel in channels:
            data["params"].append(channel["channel"])

        self.__ws_send(self.__future_public_websocket, data)

        self.__public_callbacks_mutex.acquire()
        try:
            if event not in self.__public_callbacks:
                self.__public_callbacks[event] = {}

            for channel in channels:
                self.__public_callbacks[event][channel["symbol"]] = {
                    "channel": channel["channel"],
                    "callback": callback,
                }
        finally:
            self.__public_callbacks_mutex.release()

        self.__ws_log(f"Subscribed to '{channels}'", "SUBSCRIBE")

    def __future_public_unsubscribe(self, channels, event):
        if not self.__is_connected:
            self.connect()
        self.__ws_log(f"Unsubscribing from '{channels}'", "UNSUBSCRIBE")

        data = {
            "method": "UNSUBSCRIBE",
            "params": [],
        }

        for channel in channels:
            data["params"].append(channel["channel"])

        self.__ws_send(self.__future_public_websocket, data)

        self.__public_callbacks_mutex.acquire()
        try:
            for channel in channels:
                if (event in self.__public_callbacks and
                    channel["symbol"] in self.__public_callbacks[event]):
                    del self.__public_callbacks[event][channel["symbol"]]
                    if not self.__public_callbacks[event]:
                        del self.__public_callbacks[event]
        finally:
            self.__public_callbacks_mutex.release()

        self.__ws_log(f"Unsubscribed from '{channels}'", "UNSUBSCRIBE")

    def __spot_private_subscribe(self, channel, symbols, callback=None):
        self.__ws_log(f"Subscribing to '{channel}'", "SUBSCRIBE")

        self.__spot_auth_mutex.acquire()
        try:
            if not self.__is_spot_auth:
                self.__spot_auth()
                self.__spot_auth_channel_number += 1

            if not self.__spot_private_websocket:
                self.__spot_private_websocket = websocket.WebSocket()
                self.__spot_private_websocket.connect(self.urls["websocket"][BinanceEndpoints.SPOT]+f"/{self.__spot_listen_key}")
                self.__spot_private_websocket.settimeout(1)

                self.__spot_private_thread = StoppableThread(name="websocket_user_callback", target=self.__spot_private_websocket_callback)
                self.__spot_private_thread.start()
        finally:
            self.__spot_auth_mutex.release()

        self.__private_callbacks_mutex.acquire()
        try:
            if channel not in self.__private_callbacks:
                self.__private_callbacks[channel] = {}
            for symbol in symbols:
                self.__private_callbacks[channel][symbol] = callback
        finally:
            self.__private_callbacks_mutex.release()

        self.__ws_log(f"Subscribed to '{channel}'", "SUBSCRIBE")

    def __spot_private_unsubscribe(self, channel, symbols):
        self.__ws_log(f"Unsubscribing from '{channel}'", "UNSUBSCRIBE")

        self.__private_callbacks_mutex.acquire()
        try:
            for symbol in symbols:
                if (channel in self.__private_callbacks and
                    symbol in self.__private_callbacks[channel]):
                    del self.__private_callbacks[channel][symbol]
                    if self.__private_callbacks[channel]:
                        del self.__private_callbacks[channel]

                self.__spot_auth_channel_number -= 1
        finally:
            self.__private_callbacks_mutex.release()

        self.__spot_auth_mutex.acquire()
        try:
            if self.__spot_auth_channel_number == 0:
                self.__spot_unauth()

                if self.__spot_private_thread:
                    self.__spot_private_thread.stop()
                    self.__spot_private_thread.join()
                if self.__spot_private_websocket:
                    self.__spot_private_websocket.close()
        finally:
            self.__spot_auth_mutex.release()

        self.__ws_log(f"Unsubscribed from '{channel}'", "UNSUBSCRIBE")

    def __future_private_subscribe(self, channel, symbols, callback=None):
        self.__ws_log(f"Subscribing to '{channel}'", "SUBSCRIBE")

        self.__future_auth_mutex.acquire()
        try:
            if not self.__is_future_auth:
                self.__future_auth()
                self.__future_auth_channel_number += 1

            if not self.__future_private_websocket:
                self.__future_private_websocket = websocket.WebSocket()
                self.__future_private_websocket.connect(self.urls["websocket"][BinanceEndpoints.FUTURE]+f"/{self.__future_listen_key}")
                self.__future_private_websocket.settimeout(1)

                self.__future_private_thread = StoppableThread(name="websocket_user_callback", target=self.__future_private_websocket_callback)
                self.__future_private_thread.start()
        finally:
            self.__future_auth_mutex.release()

        self.__private_callbacks_mutex.acquire()
        try:
            if channel not in self.__private_callbacks:
                self.__private_callbacks[channel] = {}
            for symbol in symbols:
                self.__private_callbacks[channel][symbol] = callback
        finally:
            self.__private_callbacks_mutex.release()

        self.__ws_log(f"Subscribed to '{channel}'", "SUBSCRIBE")

    def __future_private_unsubscribe(self, channel, symbols):
        self.__ws_log(f"Unsubscribing from '{channel}'", "UNSUBSCRIBE")

        self.__private_callbacks_mutex.acquire()
        try:
            for symbol in symbols:
                if (channel in self.__private_callbacks and
                    symbol in self.__private_callbacks[channel]):
                    del self.__private_callbacks[channel][symbol]
                    if self.__private_callbacks[channel]:
                        del self.__private_callbacks[channel]

                self.__future_auth_channel_number -= 1
        finally:
            self.__private_callbacks_mutex.release()

        self.__future_auth_mutex.acquire()
        try:
            if self.__future_auth_channel_number == 0:
                self.__future_unauth()

                if self.__future_private_thread:
                    self.__future_private_thread.stop()
                    self.__future_private_thread.join()
                if self.__future_private_websocket:
                    self.__future_private_websocket.close()
        finally:
            self.__future_auth_mutex.release()

        self.__ws_log(f"Unsubscribed from '{channel}'", "UNSUBSCRIBE")

    def __reconnect(self):
        self.__ws_log("Reconnecting", "RECONNECT")
        self.disconnect()
        self.connect()

        # Resubscribe public channels
        for event in self.__public_callbacks.keys():
            for symbol in self.__public_callbacks[event].keys():
                channel = self.__public_callbacks[event][symbol]["channel"]
                callback = self.__public_callbacks[event][symbol]["callback"]

                if self.__is_product_spot(symbol):
                    self.__spot_public_subscribe(channel, event, symbol, callback=callback)
                elif self.__is_product_future(symbol):
                    self.__future_public_subscribe(channel, event, symbol, callback=callback)

        # Resubscribe private channels
        for channel in self.__private_callbacks.keys():
            for symbol in self.__private_callbacks[channel].keys():
                callback = self.__private_callbacks[channel][symbol]
                if self.__is_product_spot(symbol):
                    self.__spot_private_subscribe(channel, symbol, callback=callback)
                elif self.__is_product_future(symbol):
                    self.__future_private_subscribe(channel, symbol, callback=callback)

        self.__ws_log("Reconnected", "RECONNECT")

    def __spot_public_websocket_callback(self):
        while not self.__spot_public_thread.is_stopped():
            has_data = select.select([self.__spot_public_websocket], [], [], 1)
            if has_data[0]:
                try:
                    op_code, frame = self.__spot_public_websocket.recv_data_frame()
                except websocket._exceptions.WebSocketConnectionClosedException:
                    log(f"[{self}][RECEIVE] Connection closed by remote host")
                    self.__on_close()
                    return
                except websocket._exceptions.WebSocketTimeoutException:
                    log(f"[{self}][RECEIVE] Read timeout")
                    continue

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
                    self.__on_spot_public_message(frame.data.decode("utf-8"))

    def __future_public_websocket_callback(self):
        while not self.__future_public_thread.is_stopped():
            has_data = select.select([self.__future_public_websocket], [], [], 1)
            if has_data[0]:

                try:
                    op_code, frame = self.__future_public_websocket.recv_data_frame()
                except websocket._exceptions.WebSocketConnectionClosedException:
                    log(f"[{self}][RECEIVE] Connection closed by remote host")
                    self.__on_close()
                    return
                except websocket._exceptions.WebSocketTimeoutException:
                    log(f"[{self}][RECEIVE] Read timeout")
                    continue

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
                    self.__on_future_public_message(frame.data.decode("utf-8"))

    def __spot_private_websocket_callback(self):
        while not self.__spot_private_thread.is_stopped():

            try:
                op_code, frame = self.__spot_private_websocket.recv_data_frame()
            except websocket._exceptions.WebSocketTimeoutException:
                continue
            except websocket._exceptions.WebSocketConnectionClosedException:
                log(f"[{self}][RECEIVE] Connection closed by remote host")
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
                self.__on_spot_private_message(frame.data.decode("utf-8"))

    def __future_private_websocket_callback(self):
        while not self.__future_private_thread.is_stopped():

            try:
                op_code, frame = self.__future_private_websocket.recv_data_frame()
            except websocket._exceptions.WebSocketTimeoutException:
                continue
            except websocket._exceptions.WebSocketConnectionClosedException:
                log(f"[{self}][RECEIVE] Connection closed by remote host")
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
                self.__on_future_private_message(frame.data.decode("utf-8"))

    def __on_close(self):
        self.__connection_lost = True
        reconnect_tries = 1

        while True:
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

    def __on_spot_public_message(self, message):
        self.__ws_log(message, "RECEIVE")
        event = ""
        data_refactored = {}
        symbol = ""

        data = json.loads(message)

        if "e" in data:
            event = data["e"]

            # Mark price update
            if event == "markPriceUpdate":
                symbol = self.__compose_spot_uniformized_symbol(data["s"])
                data_refactored = {
                    "symbol": symbol,
                    "mark_price": data["p"],
                }
            elif event == "trade":
                symbol = self.__compose_spot_uniformized_symbol(data["s"])
                data_refactored = {
                    "symbol": symbol,
                    "amount": float(data["q"]),
                    "price": float(data["p"]),
                    "timestamp": data["T"],
                }
            elif event == "depthUpdate":
                symbol = self.__compose_spot_uniformized_symbol(data["s"])

                if symbol not in self.__spot_orderbooks:
                    self.__spot_orderbooks[symbol] = {
                        "symbol": symbol,
                        "buy": {},
                        "sell": {},
                    }

                for order in data["b"]:
                    price = float(order[0])
                    size = float(order[1])

                    # Delete if size is 0
                    if size == 0:
                        if price in self.__spot_orderbooks[symbol]["buy"]:
                            #print(f"-- BUY {order} DELETE")
                            del self.__spot_orderbooks[symbol]["buy"][price]
                    else:
                        #print(f"-- BUY {order} UPDATE")
                        self.__spot_orderbooks[symbol]["buy"][price] = size

                for order in data["a"]:
                    price = float(order[0])
                    size = float(order[1])

                    # Delete if size is 0
                    if size == 0:
                        if price in self.__spot_orderbooks[symbol]["sell"]:
                            #print(f"-- SELL {order} DELETE")
                            del self.__spot_orderbooks[symbol]["sell"][price]
                    else:
                        #print(f"-- SELL {order} UPDATE")
                        self.__spot_orderbooks[symbol]["sell"][price] = size
                """
                for order in data["b"]:
                    found = False
                    index = None
                    price = float(order[0])
                    size = float(order[1])
                    # Update size if price exists
                    for idx, spot_order in enumerate(self.__spot_orderbooks[symbol]["buy"]):
                        if spot_order["price"] == price:
                            print(f"-- BUY {order} UPDATE")
                            self.__spot_orderbooks[symbol]["buy"][idx]["size"] = size
                            index = idx
                            found = True
                            break

                    # Add if price doesn't exist yet
                    if not found:
                        print(f"-- BUY {order} APPEND")
                        order_refactored = {}
                        order_refactored["price"] = price
                        order_refactored["size"] = size
                        self.__spot_orderbooks[symbol]["buy"].append(order_refactored)
                    # Remove if size is 0
                    elif size == 0 and index:
                        print(f"-- BUY {order} DELETE")
                        del self.__spot_orderbooks[symbol]["buy"][index]

                for order in data["a"]:
                    found = False
                    index = None
                    price = float(order[0])
                    size = float(order[1])
                    # Update size if price exists
                    for idx, spot_order in enumerate(self.__spot_orderbooks[symbol]["sell"]):
                        if spot_order["price"] == price:
                            print(f"-- SELL {order} UPDATE")
                            self.__spot_orderbooks[symbol]["sell"][idx]["size"] = size
                            index = idx
                            found = True
                            break

                    # Add if price doesn't exist yet
                    if not found:
                        print(f"-- SELL {order} APPEND")
                        order_refactored = {}
                        order_refactored["price"] = price
                        order_refactored["size"] = size
                        self.__spot_orderbooks[symbol]["sell"].append(order_refactored)
                    # Remove if size is 0
                    elif size == 0 and index:
                        print(f"-- SELL {order} DELETE")
                        del self.__spot_orderbooks[symbol]["sell"][index]
                """

                data_refactored = {
                    "symbol": symbol,
                    "buy": dict(sorted(self.__spot_orderbooks[symbol]["buy"].items(), reverse=True)),
                    "sell": dict(sorted(self.__spot_orderbooks[symbol]["sell"].items(), reverse=False)),
                }
            else:
                data_refactored = data

        if event in self.__public_callbacks:
            if symbol in self.__public_callbacks[event] and self.__public_callbacks[event][symbol]["callback"]:
                try:
                    self.__public_callbacks[event][symbol]["callback"](self, copy.deepcopy(data_refactored))
                except Exception as e:
                    log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __on_future_public_message(self, message):
        self.__ws_log(message, "RECEIVE")

        data = json.loads(message)

        if "e" in data:
            event = data["e"]

            # Orderbook update
            if event == "depthUpdate" or ("bids" in data and "asks" in data):
                symbol = self.__compose_future_uniformized_symbol(data["s"])

                data_refactored = {
                    "symbol": symbol,
                    "buy": [],
                    "sell": [],
                }

                for order in data["b"]:
                    order_refactored = {}
                    order_refactored["price"] = float(order[0])
                    order_refactored["size"] = float(order[1])
                    data_refactored["buy"].append(order_refactored)
                for order in data["a"]:
                    order_refactored = {}
                    order_refactored["price"] = float(order[0])
                    order_refactored["size"] = float(order[1])
                    data_refactored["sell"].append(order_refactored)

            # Mark price update
            elif event == "markPriceUpdate":
                symbol = self.__compose_future_uniformized_symbol(data["s"])
                data_refactored = {
                    "symbol": symbol,
                    "mark_price": data["p"],
                }
            else:
                data_refactored = data

            if event in self.__public_callbacks:
                if symbol in self.__public_callbacks[event] and self.__public_callbacks[event][symbol]["callback"]:
                    try:
                        self.__public_callbacks[event][symbol]["callback"](self, copy.deepcopy(data_refactored))
                    except Exception as e:
                        log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __on_spot_private_message(self, message):
        self.__ws_log(message, "RECEIVE")

        data = json.loads(message)

        if "e" in data:
            events = []
            symbols = []
            event = data["e"]
            data_refactored = {}

            # Order update
            if event == "executionReport":
                # Overwrite events and symbols to split callbacks
                events = ["ORDER_UPDATE"]

                exchange_symbol = data["s"]
                symbol = self.__compose_spot_uniformized_symbol(exchange_symbol)
                symbols = [symbol]

                # Refactor data
                data_refactored = {
                    "id": str(data["i"]),
                    "type": self.__compose_order_type({"type": data["o"]}),
                    "status": "cancelled" if data["X"] == "CANCELED" else data["X"].lower(),
                    "symbol": symbol,
                    "exchange_id": exchange_symbol,
                    "exchange_symbol": exchange_symbol,
                    "side": data["S"].lower(),
                    "size": float(data["q"]),
                    "filled_size": float(data["l"]),
                    "unfilled_size": float(data["q"]) - float(data["l"]),
                    "average_price": float(data["L"]),
                    "limit_price": float(data["p"]),
                    "stop_price": float(data["P"]) if "P" in data else None,
                    "time_in_force": data["f"].lower(),
                }

            for event in events:
                if event in self.__private_callbacks:
                    for symbol in symbols:
                        if (symbol in self.__private_callbacks[event] and
                            self.__private_callbacks[event][symbol]):
                            try:
                                self.__private_callbacks[event][symbol](self, copy.deepcopy(data_refactored))
                            except Exception as e:
                                log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __on_future_private_message(self, message):

        self.__ws_log(message, "RECEIVE")

        data = json.loads(message)

        if "e" in data:
            events = []
            symbols = []
            event = data["e"]
            data_refactored = {}

            if event == "MARGIN_CALL":
                data = data["p"]
                data_refactored = data
            elif event == "ACCOUNT_UPDATE":
                data = data["a"]
                reason_type = data["m"]

                if reason_type == "MARGIN_TRANSFER":
                    # Overwrite events and symbols to split callbacks
                    events = ["MARGIN_UPDATE"]
                    symbols = []

                elif reason_type == "ORDER":
                    # Overwrite events and symbols to split callbacks
                    events = ["POSITIONS_UPDATE", "TRADE_UPDATE"]
                    symbols = [position["s"] for position in data["P"]]

                    data_refactored = []
                    for position in data["P"]:
                        data_refactored.append({
                            "symbol": position["s"],
                            "action": "fill",
                            "side": "buy" if float(position["pa"]) > 0 else "sell",
                            "size": str(float(position["pa"])/get_contract_size(position["s"])),
                            "entry_price": position["ep"],
                        })
                else:
                    # Do not handle this event yet
                    return

            elif event == "ORDER_TRADE_UPDATE":
                # Overwrite events and symbols to split callbacks
                events = ["ORDER_UPDATE"]

                exchange_symbol = data["o"]["s"]
                symbol = self.__compose_future_uniformized_symbol(exchange_symbol)
                symbols = [symbol]

                # Refactor data
                data = data["o"]
                data_refactored = {
                    "id": str(data["i"]),
                    "type": self.__compose_order_type({"type": data["o"]}),
                    "status": "cancelled" if data["X"] == "CANCELED" else data["X"].lower(),
                    "symbol": symbol,
                    "exchange_id": exchange_symbol,
                    "exchange_symbol": exchange_symbol,
                    "side": data["S"].lower(),
                    "size": float(data["q"]),
                    "filled_size": float(data["l"]),
                    "unfilled_size": float(data["q"]) - float(data["l"]),
                    "average_price": float(data["ap"]),
                    "limit_price": float(data["p"]),
                    "stop_price": float(data["sp"]) if "sp" in data else None,
                    "time_in_force": data["f"].lower(),
                }

            for event in events:
                if event in self.__private_callbacks:
                    for symbol in symbols:
                        if (symbol in self.__private_callbacks[event] and
                            self.__private_callbacks[event][symbol]):
                            try:
                                self.__private_callbacks[event][symbol](self, copy.deepcopy(data_refactored))
                            except Exception as e:
                                log(f"[{self}] An exception occured into callback with input data : '{data_refactored}'\n{type(e).__name__} : {e}\n{format_traceback(e.__traceback__)}")

    def __spot_auth(self):
        self.__ws_log("Authenticating", "AUTH")
        url = self.urls["api"][BinanceEndpoints.SPOT]
        data = self.__request("POST", url, "/api/v3/userDataStream")
        self.__spot_listen_key = data["listenKey"]

        self.__spot_auth_timer = threading.Timer(self.__spot_auth_timeout, self.__spot_reauth)
        self.__spot_auth_timer.start()

        log(f"[{self}] Got new listen key : {self.__spot_listen_key}")

        self.__is_spot_auth = True
        self.__ws_log("Authenticated", "AUTH")

    def __future_auth(self):
        self.__ws_log("Authenticating", "AUTH")
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        data = self.__request("POST", url, "/fapi/v1/listenKey", auth=True)
        self.__future_listen_key = data["listenKey"]

        self.__future_auth_timer = threading.Timer(self.__future_auth_timeout, self.__future_reauth)
        self.__future_auth_timer.start()

        log(f"[{self}] Got new listen key : {self.__future_listen_key}")

        self.__is_future_auth = True
        self.__ws_log("Authenticated", "AUTH")

    def __spot_reauth(self):
        self.__ws_log("Reauthenticating", "REAUTH")
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {"listenKey": self.__spot_listen_key}
        self.__request("PUT", url, "/api/v3/userDataStream", payload=payload)

        log(f"[{self}] Renew listen key : {self.__spot_listen_key}")

        self.__spot_auth_timer.cancel()
        self.__spot_auth_timer = threading.Timer(self.__spot_auth_timeout, self.__spot_reauth)
        self.__spot_auth_timer.start()

        self.__is_spot_auth = True
        self.__ws_log("Reauthenticated", "REAUTH")

    def __future_reauth(self):
        self.__ws_log("Reauthenticating", "REAUTH")
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        payload = {"listenKey": self.__future_listen_key}
        self.__request("PUT", url, "/fapi/v1/listenKey", auth=True, payload=payload)

        log(f"[{self}] Renew listen key : {self.__future_listen_key}")

        self.__future_auth_timer.cancel()
        self.__future_auth_timer = threading.Timer(self.__future_auth_timeout, self.__future_reauth)
        self.__future_auth_timer.start()

        self.__is_future_auth = True
        self.__ws_log("Reauthenticated", "REAUTH")

    def __spot_unauth(self):
        self.__ws_log("Unauthenticating", "UNAUTH")
        url = self.urls["api"][BinanceEndpoints.SPOT]
        payload = {"listenKey": self.__spot_listen_key}
        self.__request("DELETE", url, "/api/v3/userDataStream", payload=payload)
        self.__spot_listen_key = ""

        self.__spot_auth_timer.cancel()

        self.__is_spot_auth = False
        self.__ws_log("Unauthenticated", "UNAUTH")

    def __future_unauth(self):
        self.__ws_log("Unauthenticating", "UNAUTH")
        url = self.urls["api"][BinanceEndpoints.FUTURE]
        self.__request("DELETE", url, "/fapi/v1/listenKey", auth=True)
        self.__future_listen_key = ""

        self.__future_auth_timer.cancel()

        self.__is_future_auth = False
        self.__ws_log("Unauthenticated", "UNAUTH")

    def __ws_send(self, websocket, data):
        data["id"] = self.__ws_count
        self.__ws_log(data, "SEND")
        websocket.send(json.dumps(data))
        self.__ws_count += 1

    def __ws_log(self, message, header=""):
        if self.__show_websocket_logs:
            if header:
                message = f"[{header}] {message}"
            log(f"[{self}] {message}")

    def __request_log(self, response):
        try:
            file_path = "binance_requests.log"
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
