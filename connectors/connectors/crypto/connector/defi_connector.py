from connectors.crypto.connector.defi.coin_market_cap import CoinMarketCap
from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.crypto.connector.defi.etherscan import Etherscan
from connectors.crypto.connector.defi.debank import Debank
import connectors.config as connectors_config
from enum import IntEnum
import json

class DefiConnectorType(IntEnum):
    NONE = 0,
    DEBANK = 1,
    ETHERSCAN = 2,

class DefiConnector():
    PLATFORM_ID = None
    PLATFORM_NAME = None

    def __init__(self, strategy, base_currency="USD"):
        self.strategy = strategy
        self.database = strategy.database
        self.public_key = strategy.wallet_address
        self.base_currency = base_currency
        self.debank_api_key = connectors_config.get("DEBANK_API_KEY")
        self.etherscan_api_key = connectors_config.get("ETHERSCAN_API_KEY")
        self.coin_market_cap_api_key = connectors_config.get("COIN_MARKET_CAP_API_KEY")

        if self.debank_api_key:
            self.connector_type = DefiConnectorType.DEBANK
            self.debank_connector = Debank(self.debank_api_key, self.public_key, self.strategy, self.base_currency)
        elif self.etherscan_api_key and self.coin_market_cap_api_key:
            self.connector_type = DefiConnectorType.ETHERSCAN
            self.etherscan_connector = Etherscan(self.etherscan_api_key, self.public_key)
            self.coin_market_cap_connector = CoinMarketCap(self.coin_market_cap_api_key, self.public_key)
        else:
            self.connector_type = DefiConnectorType.NONE

    def is_ip_authorized(self):
        return True

    def has_read_permission(self):
        return True

    def has_write_permission(self):
        return False

    def has_withdraw_permission(self):
        return False

    def has_future_authorized(self):
        return False

    def get_balance(self, as_base_currency=False, as_eur=False):
        if self.connector_type == DefiConnectorType.DEBANK:
            return self._debank_get_balance(as_base_currency)
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            return self._etherscan_get_balance(as_base_currency)
        else:
            return {}

    def get_profit(self, buy_crypto_history=None):
        if self.connector_type == DefiConnectorType.DEBANK:
            profit = self._debank_get_profit()
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            profit =  self._etherscan_get_profit()
        else:
            profit =  {
                    "total_balance": 0,
                    "total_deposit": 0,
                    "total_withdraw": 0,
                    "total_profit": 0
                }

        """
        feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
        if not feeder_cashflows.empty:
            for index, feeder_cashflow in feeder_cashflows.iterrows():
                currency = feeder_cashflow.currency
                amount = feeder_cashflow.amount
                cashflow_amount = amount*CbForex(self.strategy.database).get_rate(currency, self.base_currency)
                profit["total_withdraw"] -= cashflow_amount
                profit["total_profit"] -= cashflow_amount
        """

        return profit

    def get_mark_price(self, symbol, pricing_client=None):
        if self.connector_type == DefiConnectorType.DEBANK:
            return self.debank_connector.get_mark_price(symbol)
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            price = self.coin_market_cap_connector.get_token_price(symbol)[symbol]
            return {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "mark_price": price,
            }
        else:
            return {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "mark_price": 0,
            }

    def get_positions(self):
        if self.connector_type == DefiConnectorType.DEBANK:
            return self._debank_get_positions()
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            return self._etherscan_get_positions()
        else:
            return {}

    def get_active_orders(self, symbol=None):
        return []

    def get_transaction_history(self):
        if self.connector_type == DefiConnectorType.DEBANK:
            return self.debank_connector.get_transaction_history()
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            #return self._etherscan_get_transaction_history()
            return []
        else:
            return []

    def get_deposit_withdraw_history(self, start_date= None, end_date=None, reload=False, convert_from_sas=True):
        if self.connector_type == DefiConnectorType.DEBANK:
            return self._debank_get_transactions()
        elif self.connector_type == DefiConnectorType.ETHERSCAN:
            return self._etherscan_get_transactions()
        else:
            return []

    def _etherscan_get_balance(self, as_base_currency=False):
        balance = self.etherscan_connector.get_balance()
        if as_base_currency:
            for symbol in list(balance.keys()):
                try:
                    factor = self.coin_market_cap_connector.get_token_price(symbol, convert=self.base_currency)[symbol]
                except:
                    del balance[symbol]
                    continue
                balance[symbol]["balance"] = balance[symbol]["balance"]*factor
                balance[symbol]["available_balance"] = balance[symbol]["available_balance"]*factor
                balance[symbol]["currency"] = self.base_currency
        return balance

    def _etherscan_get_positions(self):
        data = {}
        balance = self.etherscan_connector.get_balance()
        for symbol in balance.keys():
            try:
                base_currency_price = self.coin_market_cap_connector.get_token_price(symbol, convert=self.base_currency)[symbol]
            except:
                continue

            data[symbol] = {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "size": balance[symbol]["balance"],
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": symbol,
                "base_asset_symbol": symbol,
                "quote_asset_id": self.base_currency,
                "quote_asset_symbol": self.base_currency,
                "base_currency_amount": balance[symbol]["balance"]*base_currency_price,
                "base_currency_symbol": self.base_currency,
            }

        return data

    def _etherscan_get_transactions(self):
        return self.etherscan_connector.get_transactions()

    def _etherscan_get_profit(self):
        balance = self.get_balance(as_base_currency=True)

        total_balance = sum([token_balance["balance"] for token_balance in balance.values()])
        total_withdraw = 0
        total_deposit = 0

        token_prices = {}

        transactions = self.get_deposit_withdraw_history()
        for transaction in transactions:
            symbol = transaction["source_symbol"]

            if symbol not in token_prices:
                try:
                    token_prices[symbol] = self.coin_market_cap_connector.get_token_price(symbol, convert="USD")[symbol]
                except:
                    continue

            if transaction["type"] == "withdraw":
                total_withdraw += transaction["source_amount"]*token_prices[symbol]
            else:
                total_deposit += transaction["source_amount"]*token_prices[symbol]

        data = {
                "total_deposit": total_deposit,
                "total_withdraw": total_withdraw,
                "trading_credit": 0,
                "total_fees": 0, #TODO
                "total_balance": total_balance,
                "total_profit": total_balance - total_deposit + total_withdraw
            }
        return data

    def _debank_get_balance(self, as_base_currency=False):
        return self.debank_connector.get_balance(as_base_currency)

    def _debank_get_positions(self):
        return self.debank_connector.get_positions()

    def _debank_get_transactions(self):
        return self.debank_connector.get_deposit_withdraw_history()

    def _debank_get_profit(self):
        return self.debank_connector.get_profit()