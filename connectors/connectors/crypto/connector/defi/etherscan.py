import requests
import datetime
import json

WEI=10**-18
class Etherscan:

    def __init__(self, api_key, public_key, base_currency="USD"):
        self.base_url = "https://api.etherscan.io/api"
        self.api_key = api_key
        self.public_key = public_key
        self.base_currency = base_currency
        self.session = requests.Session()

    def get_gas_prices(self):
        response = self.__request("GET", {
            "module": "gastracker",
            "action": "gasoracle",
            "apikey": self.api_key,
        })
        return response

    def get_balance(self, as_base_currency=False):
        data = {}

        # Get ETH balance
        response = self.__request("GET", {
            "module": "account",
            "action": "balance",
            "address": self.public_key,
            "tag": "latest",
            "apiKey": self.api_key,
        })
        balance = float(response)*WEI
        data["ETH"] = {
            "symbol": "ETH",
            "balance": balance,
            "available_balance": balance,
            "currency": "ETH",
        }

        # Get other tokens balance
        tokens = self.__get_account_tokens()
        for contract_address, token in tokens.items():
            response = self.__request("GET", {
                "module": "account",
                "action": "tokenbalance",
                "address": self.public_key,
                "contractaddress": contract_address,
                "tag": "latest",
                "apiKey": self.api_key,
            })

            symbol = token["symbol"]
            decimals = token["decimals"]

            balance = float(response)*10**-decimals

            data[symbol] = {
                "symbol": symbol,
                "balance": balance,
                "available_balance": balance,
                "currency": symbol,
            }

        return data

    def get_transactions(self):
        data = []

        operations = {}

        transfers = self.__get_token_transfers()
        for transfer in transfers:
            decimals = int(transfer["tokenDecimal"])
            data.append({
                "date": datetime.datetime.fromtimestamp(int(transfer["timeStamp"])),
                "type": "withdraw" if transfer["from"] == self.public_key else "deposit",
                "source_symbol": transfer["tokenSymbol"],
                "source_amount": float(transfer["value"])*10**-decimals,
                "destination_symbol": transfer["tokenSymbol"],
                "destination_amount": float(transfer["value"])*10**-decimals,
                "fees": 0, # To compute
                "operation_id": transfer["hash"],
                "wallet_address": transfer["to"] if transfer["from"] == self.public_key else transfer["from"],
            })
            operations[transfer["hash"]] = transfer

        transactions = self.__get_transactions()
        for transaction in transactions:
            if transaction["hash"] in operations:
                continue

            data.append({
                "date": datetime.datetime.fromtimestamp(int(transaction["timeStamp"])),
                "type": "withdraw" if transaction["from"] == self.public_key else "deposit",
                "source_symbol": "ETH",
                "source_amount": float(transaction["value"])*WEI,
                "destination_symbol": "ETH",
                "destination_amount": float(transaction["value"])*WEI,
                "fees": 0, # To compute
                "operation_id": transaction["hash"],
                "wallet_address": transaction["to"] if transaction["from"] == self.public_key else transaction["from"],
            })

        data.sort(key=lambda item:item["date"], reverse=True)

        return data

    def get_last_block(self):
        response = self.__api_get_last_block()
        return int(response, 16)

    def __get_account_tokens(self):
        data = {}
        response = self.__get_token_transfers()
        for transfer in response:
            token_contract_address = transfer["contractAddress"]
            if token_contract_address not in data:
                data[token_contract_address] = {
                    "name": transfer["tokenName"],
                    "symbol": transfer["tokenSymbol"],
                    "decimals": int(transfer["tokenDecimal"]),
                    "contract_address": token_contract_address,
                }
        return data

    def __get_transactions(self):
        response = self.__request("GET", {
            "module": "account",
            "action": "txlist",
            "address": self.public_key,
            "page": 1,
            "offset": 0,
            "sort": "desc",
            "apiKey": self.api_key,
        })
        return response

    def __get_token_transfers(self):
        response = self.__request("GET", {
            "module": "account",
            "action": "tokentx",
            "address": self.public_key,
            "apiKey": self.api_key,
        })
        return response

    def __api_get_last_block(self):
        response = self.__request("GET", {
            "module": "proxy",
            "action": "eth_blockNumber",
            "apiKey": self.api_key,
        })
        return response

    def __request(self, method, payload=None, headers=None):
        if not headers:
            headers = {}
        if not payload:
            payload = {}

        if method == "GET":
            payload_str = self.__serialize(payload)
            response = self.session.request(method, f"{self.base_url}?{payload_str}", timeout=(3, 27), headers=headers)
        else:
            response = self.session.request(method, self.base_url, params=payload, timeout=(3, 27), headers=headers)
        return self.__parse_response(response)

    def __parse_response(self, response):
        if response.status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(response.status_code)

            if response_json["result"]:
                raise requests.exceptions.HTTPError("{} : {}".format(response.status_code, response_json["result"]))
            else:
                raise requests.exceptions.HTTPError(response.status_code)
        else:
            response = response.json()
            return response["result"]

    def __serialize(self, data):
        body_str = ""
        for key in sorted(data.keys()):
            value = data[key]
            if isinstance(value, bool):
                if data[key]:
                    value = "true"
                else:
                    value = "false"
            body_str += f"{key}={value}&"
        body_str = body_str[:-1]
        return body_str