
from connectors.crypto.connector.common.platforms import Exchanges, Exchange
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.threading.Threads import format_traceback
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
import datetime
import json

class StrategyConnector:

    def __init__(self, strategy, base_currency="USD", brokerage_strategy=None):
        self.strategy = strategy
        self.database = strategy.database
        self.base_currency = base_currency
        self.brokerage_strategy = brokerage_strategy

        # Initialize strategy information
        self.__strategy_df = self.database.getTable("strategies", arg=f"WHERE id = {self.strategy.id}")

        self.PLATFORM_ID = self.__strategy_df["platform_id"].values[0]
        self.api_key = strategy.api_key
        self.api_secret = strategy.api_secret
        self.passphrase = strategy.passphrase

        if strategy.is_master_strat() and len(strategy.get_substrats()) > 0:
            self.PLATFORM_NAME = "None"
            self.connector = None
        else:
            if self.api_key is None or self.api_secret is None:
                raise InvalidApiKeyError('Api_key or Api_secret missing')

            exchange = Exchange(self.PLATFORM_ID)
            print('strategy',strategy)
            if exchange.connector is None:
                raise UnknownExchangeError(f"Unknown exchange identified by platform_id '{self.PLATFORM_ID}'.")
            self.PLATFORM_NAME = exchange.name

            if exchange == Exchanges.BINANCE:
                self.connector = exchange.connector(self.api_key, self.api_secret, testnet=self.strategy.testnet, strategy=strategy, passphrase=self.passphrase, base_currency=base_currency, brokerage_strategy=self.brokerage_strategy)
            else:
                self.connector = exchange.connector(self.api_key, self.api_secret, testnet=self.strategy.testnet, strategy=strategy, passphrase=self.passphrase, base_currency=base_currency)

    def is_ip_authorized(self):
        return self.connector.is_ip_authorized()

    def has_read_permission(self):
        return self.connector.has_read_permission()

    def has_write_permission(self):
        return self.connector.has_write_permission()

    def has_withdraw_permission(self):
        return self.connector.has_withdraw_permission()

    def has_future_authorized(self):
        return self.connector.has_future_authorized()

    def get_fees(self):
        return self.connector.get_fees()

    def get_symbol_usd_price(self, symbol, date=None):
        return self.connector.get_symbol_usd_price(symbol, date)

    def get_balance(self, as_base_currency=False, as_eur=False):
        balances = {}
        if not self.strategy.is_master_strat():
            balances = self.connector.get_balance(as_base_currency, as_eur)
        else:
            substrats = self.strategy.get_substrats()
            if len(substrats) == 0:
                balances = self.connector.get_balance(as_base_currency, as_eur)
            else:
                substrat_balances = {}
                # Compose substrat balances
                for substrat in substrats:
                    substrat_balances[substrat.id] = substrat.get_api_connector().get_balance(as_base_currency, as_eur)
                # Merge substrat balances into a single balance
                for strat_id, strat_balances in substrat_balances.items():
                    for symbol, balance in strat_balances.items():
                        if symbol not in balances:
                            balances[symbol] = balance
                        else:
                            balances[symbol]["balance"] += balance["balance"]
                            balances[symbol]["available_balance"] += balance["available_balance"]
        return balances

    def get_profit(self, buy_crypto_history=None):
        profit = {
            "total_balance": 0,
            "total_deposit": 0,
            "total_withdraw": 0,
            "total_profit": 0,
        }
        if not self.strategy.is_master_strat():
            if self.PLATFORM_ID == Exchanges.BINANCE:
                buy_crypto_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {self.strategy.id}")
            profit = self.connector.get_profit(buy_crypto_history)
        else:
            substrats = self.strategy.get_substrats()
            if len(substrats) == 0:
                if self.PLATFORM_ID == Exchanges.BINANCE:
                    buy_crypto_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {self.strategy.id}")
                profit = self.connector.get_profit(buy_crypto_history)
            else:
                for substrat in substrats:
                    if substrat.is_deposit_withdraw:
                        if substrat.platform_id == Exchanges.BINANCE:
                            buy_crypto_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {self.strategy.id}")
                        else:
                            buy_crypto_history = None
                        substrat_profit = substrat.get_api_connector().get_profit(buy_crypto_history)
                        profit["total_balance"] += substrat_profit["total_balance"]
                        profit["total_deposit"] += substrat_profit["total_deposit"]
                        profit["total_withdraw"] += substrat_profit["total_withdraw"]

                        print("-------------------------------------------------")
                        print(substrat)
                        print(json.dumps(substrat_profit, indent=4, default=str))
                        print("-------------------------------------------------")

                profit["total_profit"] = profit["total_balance"] - profit["total_deposit"] + profit["total_withdraw"]

        return profit

    def get_products(self, reload=False):
        return self.connector.get_products(reload)

    def get_product(self, symbol):
        return self.connector.get_product(symbol)

    def get_orderbook(self, symbol, pricing_client=None):
        return self.connector.get_orderbook(symbol, pricing_client)

    def get_greeks(self, symbol):
        return self.connector.get_greeks(symbol)

    def send_rfq(self, symbol, side=None):
        return self.connector.send_rfq(symbol, side)

    def get_open_interest(self, symbol):
        return self.connector.get_open_interest(symbol)

    def get_candle(self, symbol, start_time, end_time):
        return self.connector.get_candle(symbol, start_time, end_time)

    def get_staking_product_position(self, product):
        return self.connector.get_staking_product_position(product)

    def get_spot_price(self, symbol):
        return self.connector.get_spot_price(symbol)

    def get_mark_price(self, symbol, pricing_client=None):
        return self.connector.get_mark_price(symbol, pricing_client)

    def get_positions(self):
        return self.connector.get_positions()

    def get_position(self, symbol):
        return self.connector.get_position(symbol)

    def get_position_margin(self, symbol):
        return self.connector.get_position_margin(symbol)

    def set_position_margin(self, symbol, delta_margin):
        return self.connector.set_position_margin(symbol, delta_margin)

    def get_symbol_usd_price(self, symbol, date=None):
        print('ICIIIIIIIIIIIIIIIIIIIII')
        return self.connector.get_symbol_usd_price(symbol, date)

    def place_market_order(self, symbol, size, side, time_in_force="gtc", reduce_only=False, wait=False, hide_log=False):
        return self.connector.place_market_order(symbol, size, side, time_in_force, reduce_only=reduce_only, wait=wait, hide_log=hide_log)

    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0, reduce_only=False):
        return self.connector.place_limit_order(symbol, size, side, limit_price, time_in_force, timeout, reduce_only=reduce_only)

    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc", reduce_only=False):
        return self.connector.place_stop_market_order(symbol, size, side, stop_price, time_in_force, reduce_only=reduce_only)

    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc", reduce_only=False):
        return self.connector.place_stop_limit_order(symbol, size, side, stop_price, limit_price, time_in_force, reduce_only=reduce_only)

    def place_maker_order(self, symbol, side, size, timeout=60, limit_price_coef=0.0001, stop_price_coef=0.0075):
        if (self.PLATFORM_ID == Exchanges.DELTA_EXCHANGE or
            self.PLATFORM_ID == Exchanges.DERIBIT or
            self.PLATFORM_ID == Exchanges.BINANCE):
            return self.connector.place_maker_order(symbol, side, size, timeout, limit_price_coef, stop_price_coef)
        else:
            return self.connector.place_market_order(symbol, side=side, size=size)

    def place_limit_custom(self, symbol, side, size, remaining_size, limit_price):
        if self.PLATFORM_ID == Exchanges.COINBASE:
            return self.connector.place_limit_custom(symbol, side, size, remaining_size, limit_price)
        else:
            return self.connector.place_limit_order(symbol, side=side, size=size, limit_price=limit_price)

    def get_active_orders(self, symbol=None):
        return self.connector.get_active_orders(symbol)

    def get_order_state(self, symbol, order_id):
        return self.connector.get_order_state(symbol, order_id)

    def cancel_order(self, symbol, order_id):
        return self.connector.cancel_order(symbol, order_id)

    def edit_order(self, order_id, symbol, limit_price=None, size=None):
        return self.connector.edit_order(order_id, symbol, limit_price, size)

    def get_order_history(self, symbol=None):
        return self.connector.get_order_history(symbol)

    def get_trade_history(self, symbol, start_date, end_date):
        return self.connector.get_trade_history(symbol, start_date, end_date)

    def get_leverage(self, symbol):
        return self.connector.get_leverage(symbol)

    def set_leverage(self, symbol, leverage):
        return self.connector.set_leverage(symbol, leverage)

    def get_transaction_history(self):
        return self.connector.get_transaction_history()

    def get_deposit_withdraw_history(self, start_date=None, end_date=None, reload=False, convert_from_sas=False):
        if not self.strategy.is_master_strat():
            if self.PLATFORM_ID == Exchanges.BINANCE and not reload:
                transfers = self.__get_deposit_withdraw_from_database(self.strategy.id, start_date, end_date)
            else:
                transfers = self.connector.get_deposit_withdraw_history(start_date, end_date)
        else:
            substrats = self.strategy.get_substrats()
            if len(substrats) == 0:
                if self.PLATFORM_ID == Exchanges.BINANCE and not reload:
                    transfers = self.__get_deposit_withdraw_from_database(self.strategy.id, start_date, end_date)
                else:
                    transfers = self.connector.get_deposit_withdraw_history(start_date, end_date)
            else:
                transfers = []
                for substrat in substrats:
                    if substrat.platform_id == Exchanges.BINANCE and not reload:
                        substrat_transfers = self.__get_deposit_withdraw_from_database(substrat.id, start_date, end_date)
                    else:
                        substrat_transfers = substrat.get_api_connector().get_deposit_withdraw_history(start_date, end_date)

                    #print("-------------------------------")
                    #print(f"Strategy '{substrat}' transfers : {len(substrat_transfers)} transfers")
                    #print(json.dumps(substrat_transfers, indent=4, default=str))
                    #print("-------------------------------")

                    transfers.extend(substrat_transfers)

        """
        if convert_from_sas:
            sas_connector = self.strategy.get_sas_connector()
            for transfer in transfers:
                if transfer["destination_symbol"] == self.strategy.base_currency:
                    transfer["base_currency_amount"] = transfer["destination_amount"]
                else:
                    symbol = f"{transfer['destination_symbol']}_{self.strategy.base_currency}"
                    date = transfer["date"]
                    end_date = date
                    start_date = date - datetime.timedelta(hours=1)
                    fills = sas_connector.get_trade_history(symbol, start_date=start_date, end_date=end_date)

                    if len(fills) > 0:
                        order_id = fills[0]["order_id"]
                        order = sas_connector.get_order_state(symbol, order_id)
                        rate = order["average_price"]
                        transfer["base_currency_amount"] = round(transfer["source_amount"]*rate, 0)
        """

        transfers.sort(key=lambda order:order["date"], reverse=True)
        return transfers

    def create_wallet_address(self, symbol):
        return self.connector.create_wallet_address(symbol)

    def get_wallet_address(self, symbol=None):
        return self.connector.get_wallet_address(symbol)

    def withdraw(self, symbol, amount, address, network_name=None):
        return self.connector.withdraw(symbol, amount, address, network_name)

    def get_portfolio_margins(self, currency, add_positions=True, simulated_positions={}):
        return self.connector.get_portfolio_margins(currency, add_positions, simulated_positions)

    #def get_margins(self, symbol, amount, price):
    def get_margins(self):
        return self.connector.get_margins()

    def get_payment_methods(self):
        return self.connector.get_payment_methods(symbol)

    def withdraw_to_iban(self, symbol, amount, iban):
        return self.connector.withdraw_to_iban(symbol, amount, iban)

    def is_connection_lost(self):
        return self.connector.is_connection_lost()

    def is_connection_aborted(self):
        return self.connector.is_connection_aborted()

    def set_reconnect_timeout(self, timeout):
        return self.connector.set_reconnect_timeout(timeout)

    def set_abortion_datetime(self, datetime):
        return self.connector.set_abortion_datetime(datetime)

    def show_websocket_logs(self, show):
        return self.connector.show_websocket_logs(show)

    def cleanup(self, force=False):
        return self.connector.cleanup(force)

    def subscribe_orderbook(self, symbols, callback=None):
        return self.connector.subscribe_orderbook(symbols, callback)

    def unsubscribe_orderbook(self, symbols, callback=None):
        return self.connector.unsubscribe_orderbook(symbols, callback)

    def subscribe_mark_price(self, symbols, callback=None):
        return self.connector.subscribe_mark_price(symbols, callback)

    def unsubscribe_mark_price(self, symbols, callback=None):
        return self.connector.unsubscribe_mark_price(symbols, callback)

    def subscribe_orders(self, symbols, callback=None):
        return self.connector.subscribe_orders(symbols, callback)

    def unsubscribe_orders(self, symbols, callback=None):
        return self.connector.unsubscribe_orders(symbols, callback)

    def subscribe_balances(self, callback=None):
        return self.connector.subscribe_balances(callback=callback)

    def unsubscribe_balances(self, callback=None):
        return self.connector.unsubscribe_balances(callback=callback)

    def subscribe_positions(self, callback=None):
        return self.connector.subscribe_positions(callback=callback)

    def unsubscribe_positions(self, callback=None):
        return self.connector.unsubscribe_positions(callback=callback)

    def __str__(self):
        return f"{self.PLATFORM_NAME} {self.strategy}"

    def __get_deposit_withdraw_from_database(self, strategy_id, start_date=None, end_date=None):
        data = []
        if start_date and end_date:
            date_selector = f" AND date > {start_date} AND date < {end_date}"
        elif start_date:
            date_selector = f" AND date > {start_date}"
        elif end_date:
            date_selector = f" AND date < {end_date}"
        else:
            date_selector = ""
        deposit_withdraw_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {strategy_id} {date_selector}")
        for index, operation in deposit_withdraw_history.iterrows():
            data.append({
                "date": datetime.datetime.strptime(str(operation.date), "%Y-%m-%d %H:%M:%S"),
                "type": operation.transaction_type,
                "source_symbol": operation.source_symbol,
                "source_amount": operation.source_amount,
                "destination_symbol": operation.destination_symbol,
                "destination_amount": operation.destination_amount,
                "fees": operation.fees,
                "operation_id": operation.order_number,
                "wallet_address": operation.wallet_address,
                "base_currency_amount": operation.base_currency_amount,
                "base_currency_symbol": operation.base_currency_symbol,
            })
        return data