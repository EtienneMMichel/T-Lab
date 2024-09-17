from connectors.crypto.connector.exchanges.coinbase import CbForex
from urllib.parse import urlencode
import requests
import datetime
import json

class Debank:

    def __init__(self, api_key, public_key, strategy=None, base_currency="USD"):
        self.base_url = "https://pro-openapi.debank.com"
        self.api_key = api_key
        self.public_key = public_key
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.session = requests.Session()

        self.__known_addresses = [
            "0x76b712c313584063a814db4aa4c82587b9aef0ee",
            "0xd079a0817fbb17d4ee15cf7216dc56b910856efd",
            "0xe767ecb1a5d44445e1acbaeb9dd35f4ad30ffd33",
            "0xfa3EF2Ea5F719fdE7A291009212401bFE140a551",
            "0xe9777B2EAFfb206d80eDF8bdD7517ADCeb930fa7",
            "0x465deaba55c4e6c788ce1a4833b62c1951dfebe6",
            "0xfd111fa8600c0f860b65cb765ae6bbc7d503e719",
            "0x7be3032a9fe197b530bec487c0fba104975d78a7",
            "0xfb6ae172a1a0b484e78e65a09aa83aea95fde1b2",
            "0x18c788b680944a0cf6fe965243e4592320dad708",
            "0x32aA234c8DB6723D99E5558Ee8Ce39e9d2D4E836",
            '0x5b38c25410d1eac15d49d46487edaa277ce754fb',
            "0xa3116dc4bb0ac2b2dfc160a102b1193fdf5514cc",
            "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
            '0x7ecb8d6edd198d63f5cf1431755da34e1414e558',
            '0x6CCbdeFD00228204CC5a3eb67A4DB0517b08C894',
            '0x19886C34c1f06D38937f1E63D4b883eB29B21e0c',
            '0x57fE26E72f588607155C53760C282bfC0E7eB54d',
            "0xa402d57b7de8ed051c335fd8038c17f32f2ddb4f"
        ]

        self._verified_list = [
            "OLAS",
            "HILO",
            "wTAO",
            "HERA",
            "RSTK",
            "TAO",
            "UNIBOT",
            "ALPH",
            "SMT",
            "RVST",
            "ROUTE",
            "NXRA",
            "RVF",
            "XRT",
            "KAS",
            "SHRAP",
            "HELLO",
            "PAAL",
            "MUBI",
            "RIO",
            "wLYX",
            "RAIN",
            "CBY",
            "eXRD",
            "basedAI",
            "ARC",
            "SDEX",
            "OxO",
            "NXRA",
            "PEAS",
            "GLQ",
            "DOLA",
            "ZIG",
            "DEAI",
            "BANANA",
            "NPC",
            "mooAeroDOLA-USDC",
            "atlWETH",
            "RLB",
        ]
        self._protocol_verified_list = [
            "pendle2",
            "arb_dhedge"
        ]
        self.__known_addresses = [address.lower() for address in self.__known_addresses]

        self.__token_ids = {
            "ETH": {
                "chain_id": "eth",
                "id": "eth",
            },
            "BTC": {
                "chain_id": "eth",
                "id": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
            },
            "WBTC": {
                "chain_id": "eth",
                "id": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
            },
            "USDT": {
                "chain_id": "eth",
                "id": "0xdac17f958d2ee523a2206206994597c13d831ec7"
            }
        }

    def get_balance(self, as_base_currency=False):
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        data = {}

        response = self.__api_get_all_user_token_list()

        for token in response:
            symbol = token["symbol"]

            if not token["is_verified"]:
                if symbol not in self._verified_list:
                    continue

            if symbol == "atlWETH":
                token["price"] = 1


            if as_base_currency:
                balance = token["amount"]*token["price"]*rate
                currency = self.base_currency
            else:
                balance = token["amount"]
                currency = symbol

            data[symbol] = {
                "symbol": symbol,
                "balance": balance,
                "available_balance": balance,
                "currency": currency,
            }

        protocols = self.__api_get_user_complex_protocol_list()
        for protocol in protocols:
            for portfolio in protocol["portfolio_item_list"]:
                for token in portfolio["asset_token_list"]:
                    symbol = token["symbol"]

                    if not token["is_verified"]:
                        if token['protocol_id'] not in self._protocol_verified_list:
                            continue

                    if as_base_currency:
                        balance = token["amount"]*token["price"]*rate
                        currency = self.base_currency
                    else:
                        balance = token["amount"]
                        currency = symbol

                    if symbol not in data:
                        data[symbol] = {
                            "symbol": symbol,
                            "balance": balance,
                            "available_balance": balance,
                            "currency": currency,
                        }
                    else:
                        data[symbol]["balance"] += balance
                        data[symbol]["available_balance"] += balance
        return data

    def get_mark_price(self, symbol):
        if symbol not in self.__token_ids:
            data = {
                "symbol": symbol,
                "exchange_id": None,
                "exchange_symbol": symbol,
                "mark_price": 0,
            }
        else:
            response = self.__api_get_token(self.__token_ids[symbol]["id"], chain_id=self.__token_ids[symbol]["chain_id"])
            data = {
                "symbol": symbol,
                "exchange_id": self.__token_ids[symbol]["id"],
                "exchange_symbol": symbol,
                "mark_price": response["price"],
            }
        return data

    def get_positions(self):
        rate = CbForex(self.database).get_rate("USD", self.base_currency)
        data = {}

        response = self.__api_get_all_user_token_list()
        for token in response:
            symbol = token["symbol"]
            if not token["is_verified"] and symbol not in self._verified_list:
                continue

            data[symbol] = {
                "symbol": symbol,
                "exchange_id": symbol,
                "exchange_symbol": symbol,
                "size": token["amount"],
                "entry_price": 0,
                "maintenance_margin": 0,
                "contract_type": "spot",
                "base_asset_id": symbol,
                "base_asset_symbol": symbol,
                "quote_asset_id": self.base_currency,
                "quote_asset_symbol": self.base_currency,
                "base_currency_amount": token["amount"]*token["price"]*rate,
                "base_currency_symbol": self.base_currency,
            }

        protocols = self.__api_get_user_complex_protocol_list()
        for protocol in protocols:
            for portfolio in protocol["portfolio_item_list"]:
                for token in portfolio["asset_token_list"]:
                    symbol = token["symbol"]
                    if not token["is_verified"] and symbol not in self._verified_list:
                        continue

                    if symbol not in data:
                        data[symbol] = {
                            "symbol": symbol,
                            "exchange_id": symbol,
                            "exchange_symbol": symbol,
                            "size": token["amount"],
                            "entry_price": 0,
                            "maintenance_margin": 0,
                            "contract_type": "spot",
                            "base_asset_id": symbol,
                            "base_asset_symbol": symbol,
                            "quote_asset_id": self.base_currency,
                            "quote_asset_symbol": self.base_currency,
                            "base_currency_amount": token["amount"]*token["price"]*rate,
                            "base_currency_symbol": self.base_currency,
                        }
                    else:
                        data[symbol]["size"] += token["amount"]
                        data[symbol]["base_currency_amount"] += token["amount"]*token["price"]*rate

        return data

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

    def get_deposit_withdraw_history(self):
        data = []
        tokens = {}

        transactions = []
        response = self.__api_get_transaction_history_on_all_chains()
        transactions.extend(response["history_list"])

        count = 20
        while len(response["history_list"]) == count:
            timestamp = response["history_list"][count-1]["time_at"]
            response = self.__api_get_transaction_history_on_all_chains(count=count, timestamp=timestamp)
            transactions.extend(response["history_list"])

        for transaction in transactions:
            if transaction["is_scam"]:
                continue

            # Filter-out trades and swaps
            if len(transaction["sends"]) > 0 and len(transaction["receives"]) > 0:
                continue
            elif len(transaction["sends"]) > 0:
                type = "withdraw"
                amount = transaction["sends"][0]["amount"]
                token_id = transaction["sends"][0]["token_id"]
                wallet_address = transaction["sends"][0]["to_addr"]
                fees = transaction["tx"]["eth_gas_fee"] if "tx" in transaction and "eth_gas_fee" in transaction["tx"] else 0
            elif len(transaction["receives"]) > 0:
                type = "deposit"
                amount = transaction["receives"][0]["amount"]
                token_id = transaction["receives"][0]["token_id"].upper()
                wallet_address = transaction["receives"][0]["from_addr"]
                fees = 0
            else:
                continue

            if wallet_address not in self.__known_addresses:
                continue

            if not token_id.upper().startswith("0X"):
                symbol = token_id.upper()
            elif token_id in tokens:
                token = tokens[token_id]
                symbol = token["symbol"]
                if not token["is_verified"] and symbol not in self._verified_list:
                    continue
            else:
                token = self.__api_get_token(token_id, transaction["chain"])
                symbol = token["symbol"]
                if not token["is_verified"] and symbol not in self._verified_list:
                    continue
                tokens[token_id] = token

            transaction_id = transaction["id"]
            date = datetime.datetime.fromtimestamp(int(transaction["time_at"]))
            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            try:
                token_price = self.__api_get_token_history_price(token_id, date, transaction["chain"])["price"]
            except Exception as e:
                token_price = 0

            # Filter-out fees and feeder cashflows
            if self.strategy and self.database:
                transaction_cashflow = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND last_tx_id = '{transaction_id}'")
                if not transaction_cashflow.empty:
                    cashflow = transaction_cashflow.iloc[0]
                    if cashflow.transaction_type == 'fees' or cashflow.transaction_type == 'feeder_cashflow':
                        continue

            data.append({
                "date": date,
                "type": type,
                "source_symbol": symbol,
                "source_amount": amount,
                "destination_symbol": symbol,
                "destination_amount": amount,
                "fees": fees,
                "operation_id": transaction_id,
                "wallet_address": wallet_address,
                "base_currency_amount": amount*token_price*rate,
                "base_currency_symbol": self.base_currency,
                "tx_hash": transaction_id,
            })

        return data

    def get_profit(self):
        total_balance = 0
        total_withdraw = 0
        total_deposit = 0

        balances = self.get_balance(as_base_currency=True)
        for balance in balances.values():
            total_balance += balance["balance"]

        print('total_balance',total_balance)

        transactions = []
        response = self.__api_get_transaction_history_on_all_chains()
        transactions.extend(response["history_list"])

        count = 20
        while len(response["history_list"]) == count:
            timestamp = response["history_list"][count-1]["time_at"]
            response = self.__api_get_transaction_history_on_all_chains(count=count, timestamp=timestamp)
            transactions.extend(response["history_list"])

        feeder_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'feeder_cashflow'")
        feeder_cashflows_transaction_ids = feeder_cashflows["last_tx_id"].values
        internal_deposit_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_deposit'")
        internal_deposit_transaction_ids = internal_deposit_cashflows["last_tx_id"].values
        internal_fees_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'fees'")
        internal_fees_transaction_id = internal_fees_cashflows["last_tx_id"].values
        print('internal_fees_transaction_id',internal_fees_transaction_id)
        internal_withdraw_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'internal_withdraw'")
        internal_withdraw_transaction_ids = internal_withdraw_cashflows["last_tx_id"].values

        for transaction in transactions:
            if transaction['id'] in feeder_cashflows_transaction_ids:
                #print(f"### Skip transaction by feeder id")
                continue
            if transaction['id'] in internal_deposit_transaction_ids:
                #print(f"### Skip transaction by internal deposit id")
                continue
            if transaction['id'] in internal_withdraw_transaction_ids:
                #print(f"### Skip transaction by internal withdraw id")
                continue
            if len(internal_fees_transaction_id) > 0:
                if transaction['id'] in internal_fees_transaction_id:
                    #print(f"### Skip transaction by internal withdraw id")
                    continue

            if transaction["is_scam"]:
                #print("### SKIP IS SCAM")
                continue

            if len(transaction["sends"]) > 0:
                type = "withdraw"
                amount = transaction["sends"][0]["amount"]
                token_id = transaction["sends"][0]["token_id"]
                wallet_address = transaction["sends"][0]["to_addr"]
            elif len(transaction["receives"]) > 0:
                type = "deposit"
                amount = transaction["receives"][0]["amount"]
                token_id = transaction["receives"][0]["token_id"].upper()
                wallet_address = transaction["receives"][0]["from_addr"]
            else:
                #print("### SKIP NO RECEIVE NO SEND")
                continue

            if wallet_address not in self.__known_addresses:
                #print("### SKIP UNKNOWN ADDRESS")
                continue

            date = datetime.datetime.fromtimestamp(int(transaction["time_at"]))
            rate = CbForex(self.database).get_rate("USD", self.base_currency, date)
            try:
                token_price = self.__api_get_token_history_price(token_id, date, transaction["chain"])["price"]
            except Exception as e:
                #print("### EXCEPTION ON TOKEN PRICE")
                token_price = 0

            if type == "withdraw":
                total_withdraw += amount*token_price*rate
            else:
                total_deposit += amount*token_price*rate

        """
        if self.strategy and self.database:
            # Remove fees cashflows from withdrawals
            fees_cashflows = self.database.getTable("strategy_cashflows", arg=f"WHERE strategy_id = {self.strategy.id} AND transaction_type = 'fees'")
            withdraw_fees = 0
            for index, fees_cashflow in fees_cashflows.iterrows():
                if 'USD' in fees_cashflow.currency:
                    currency = 'USD'
                if 'EUR' in fees_cashflow.currency:
                    currency = 'EUR'
                rate = CbForex(self.database).get_rate(currency, self.base_currency, datetime.datetime.strptime(str(fees_cashflow.timestamp), "%Y-%m-%d %H:%M:%S"))
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

        data = {
                "total_balance": total_balance,
                "total_deposit": total_deposit,
                "total_withdraw": total_withdraw,
                "total_profit": total_balance - total_deposit + total_withdraw
            }
        return data

    def __compose_portfolios(self):
        portfolios = [
            {
                "id": self.public_key,
                "chain_id": "eth",
            },
        ]

        protocols = self.__api_get_user_complex_protocol_list()
        for protocol in protocols:
            for portfolio in protocol["portfolio_item_list"]:
                for asset in portfolio["asset_token_list"]:
                    portfolios.append({
                        "id": asset["id"],
                        "chain_id": asset["chain"]
                    })
        return portfolios

    def __api_get_token(self, token_id, chain_id="eth"):
        response = self.__request("GET", "/v1/token", payload={
            "id": token_id,
            "chain_id": chain_id,
        })
        return response

    def __api_get_token_history_price(self, token_id, date, chain_id="eth"):
        response = self.__request("GET", "/v1/token/history_price", payload={
            "id": token_id,
            "chain_id": chain_id,
            "date_at": str(date),
        })
        return response

    def __api_get_user_complex_protocol_list(self, chain_id="eth"):
        response = self.__request("GET", "/v1/user/complex_protocol_list", payload={
            "id": self.public_key,
            "chain_id": chain_id,
        })
        return response

    def __api_get_user_token_list(self, chain_id="eth"):
        response = self.__request("GET", "/v1/user/token_list", payload={
            "id": self.public_key,
            "chain_id": chain_id,
            "is_all": True,
        })
        return response

    def __api_get_all_user_token_list(self):
        response = self.__request("GET", "/v1/user/all_token_list", payload={
            "id": self.public_key,
        })
        return response

    def __api_get_transaction_history(self, chain_id="eth"):
        response = self.__request("GET", "/v1/user/history_list", payload={
            "id": self.public_key,
            "chain_id": chain_id,
            "count": 20,
        })
        return response

    def __api_get_transaction_history_on_all_chains(self, count=20, timestamp=None):
        payload = {
            "id": self.public_key,
            "count": count,
        }
        if timestamp:
            payload["start_time"] = int(timestamp)
        response = self.__request("GET", "/v1/user/all_history_list", payload=payload)
        return response

    def __request(self, method, path, payload=None, headers=None):
        if not headers:
            headers = {}
        if not payload:
            payload = {}

        headers["accept"] = "application/json"
        headers["AccessKey"] = self.api_key

        url = f"{self.base_url}{path}"

        if method == "GET":
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", timeout=(3, 27), headers=headers)
        else:
            response = self.session.request(method, url, params=payload, timeout=(3, 27), headers=headers)
        self.__request_log(response)

        return self.__parse_response(response)

    def __parse_response(self, response):
        if response.status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(response.status_code)

            if response_json:
                raise requests.exceptions.HTTPError("{} : {}".format(response.status_code, response_json))
            else:
                raise requests.exceptions.HTTPError(response.status_code)
        else:
            response = response.json()
            return response

    def __request_log(self, response):
        try:
            file_path = "debank_requests.log"
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
            return f"DEBANK {self.strategy}"
        else:
            return "DEBANK"
