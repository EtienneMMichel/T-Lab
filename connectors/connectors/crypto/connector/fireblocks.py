from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *
from urllib.parse import urlencode
from hashlib import sha256
import datetime
import requests
import secrets
import json
import time
import jwt
import os

class DictGenerated:
    def __init__(self, dictionary):
        self.__parse_dict(self, dictionary)

    def __parse_dict(self, target, dictionary):
        for k, v in dictionary.items():
            if isinstance(v, dict):
                setattr(self, k, DictGenerated(v))
            else:
                setattr(self, k, v)

    def __getattr__(self, name):
        return None

    def __str__(self):
        return json.dumps(vars(self), default=str)

class FireblocksTransfer(DictGenerated):

    def __init__(self, dictionary, transaction_type):
        super().__init__(dictionary)
        self.transaction_type = transaction_type

    # Add from_strategy_id / to_strategy_id / transaction_type
    def to_database(self, database):
        source_strategy_id = None
        destination_strategy_id = None

        if self.source:
            source_strategy = database.getTable("strategies", arg=f"WHERE fireblocks_account_name = '{self.source.name}'")
            if not source_strategy.empty:
                source_strategy_id = source_strategy["id"].values[0]

        if self.destination:
            destination_strategy = database.getTable("strategies", arg=f"WHERE fireblocks_account_name = '{self.destination.name}'")
            if not destination_strategy.empty:
                destination_strategy_id = destination_strategy["id"].values[0]

        transfer = database.getTable("fireblocks_transfers", arg=f"WHERE fireblocks_id = '{self.id}'")
        if transfer.empty:
            database.execute("""INSERT INTO fireblocks_transfers (from_strategy_id, to_strategy_id, transaction_type, fireblocks_id, source_type, source_name, destination_type, destination_name, asset, amount, fees, status, sub_status, tx_hash, gas_price, created_at, updated_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            params=(source_strategy_id, destination_strategy_id, self.transaction_type, self.id, self.source.type if self.source else None, self.source.name if self.source else None, self.destination.type if self.destination else None, self.destination.name if self.destination else None, self.assetId, self.amount, self.fee, self.status, self.subStatus, self.txHash, self.gasPrice, datetime.datetime.fromtimestamp(self.createdAt/1000) if self.createdAt else None, datetime.datetime.fromtimestamp(self.lastUpdated/1000) if self.lastUpdated else None))
        else:
            database.execute("""UPDATE fireblocks_transfers SET from_strategy_id = %s, to_strategy_id = %s, transaction_type = %s, source_type = %s, source_name = %s, destination_type = %s, destination_name = %s, asset = %s, amount = %s, fees = %s, status = %s, sub_status = %s, tx_hash = %s, gas_price = %s, created_at = %s, updated_at = %s WHERE fireblocks_id = %s
            """,
            params=(source_strategy_id, destination_strategy_id, self.transaction_type, self.source.type if self.source else None, self.source.name if self.source else None, self.destination.type if self.destination else None, self.destination.name if self.destination else None, self.assetId, self.amount, self.fee, self.status, self.subStatus, self.txHash, self.gasPrice, datetime.datetime.fromtimestamp(self.createdAt/1000) if self.createdAt else None, datetime.datetime.fromtimestamp(self.lastUpdated/1000) if self.lastUpdated else None, self.id))

        '''
        #TODO: Find a way to have an unique identifier for the cashflow so it can't be inserted twice
        # Update associated cashflows
        if self.status == "COMPLETED":
            now = datetime.datetime.now()

            # Fees transfer
            if self.transaction_type == "fees" or self.transaction_type == "brokering_fees":

                #TODO: Do we need to handle the different types of fees ?
                if source_strategy_id:
                    database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params=(source_strategy_id, self.transaction_type, self.txHash, datetime.datetime.fromtimestamp(self.lastUpdated/1000), self.amount, self.assetId, 0, 0, 1, True, now, now))

                #TODO: Do we need to add a cashflow for incoming fees ?
                if destination_strategy_id:
                    pass

            # Feeder transfer
            elif self.transaction_type == "feeder_cashflow":
                #TODO: implement feeder cashflows
                pass

            # Internal transfer
            elif self.transaction_type == "internal_transfer":

                # Create cashflow for outgoing withdraw from internal transfer
                if source_strategy_id:

                    number_shares_before = 0
                    number_shares_after = 0
                    nav = 1

                    # Get number shares and nav for strategy
                    balances = database.getTable("users_balances", arg=f"WHERE strategy_id = {source_strategy_id}")
                    if balances.empty:
                        #TODO: Handle case where data is empty since it's supposed to be impossible
                        pass
                    else:
                        # Get data for strategy
                        number_shares_before = balances["number_shares"].values[0]
                        number_shares_after = number_shares_before
                        nav = balances["nav"].values[0]

                    cashflow_type = "internal_withdraw"
                    log(f"Add new cashflow '{cashflow_type}' for strategy {source_strategy_id} : {self.amount} {self.assetId}")
                    database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params=(source_strategy_id, cashflow_type, self.txHash, datetime.datetime.fromtimestamp(self.lastUpdated/1000), self.amount, self.assetId, number_shares_before, number_shares_after, nav, True, now, now))

                # Create cashflow for incoming deposit from internal transfer
                if destination_strategy_id:
                    self.__register_cashflow(destination_strategy_id, 'internal_deposit', database)
        '''

    def __register_cashflow(self, strategy_id, cashflow_type, database):
        log(f"Add new cashflow '{cashflow_type}' for strategy {strategy_id} : {self.amount} {self.assetId}")
        now = datetime.datetime.now()

        base_currency_symbol = database.getTable("strategies", arg=f"WHERE id = {strategy_id}")["base_currency"].values[0]

        # Get number shares and nav for strategy
        balances = database.getTable("users_balances", arg=f"WHERE strategy_id = {strategy_id}")
        if balances.empty:

            # Compute balances and shares
            nav = 1
            number_shares_before = 0
            number_shares_after = self.amountUSD*CbForex(database).get_rate("USD", base_currency_symbol, datetime.datetime.fromtimestamp(self.lastUpdated/1000))
            total_balance = number_shares_after
            total_deposit = number_shares_after
            total_withdrawal = 0
            total_profit = 0
            last_hwm = 0
            nav_benchmark = 0
            last_hwm_benchmark = 0
            last_benchmark_price = 0

            # Create first balances entry for strategy
            database.execute("""INSERT INTO users_balances (strategy_id, total_balance, total_withdrawal, total_deposit, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            params=(strategy_id, total_balance, total_withdrawal, total_deposit, total_profit, number_shares_after, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, now, now))
        else:
            # Get data for strategy
            number_shares_before = balances["number_shares"].values[0]
            number_shares_after = number_shares_before
            nav = balances["nav"].values[0]

        database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        params=(strategy_id, cashflow_type, self.txHash, datetime.datetime.fromtimestamp(self.lastUpdated/1000), self.amount, self.assetId, number_shares_before, number_shares_after, nav, True, now, now))

class Fireblocks:

    def __init__(self, api_key, api_secret_file_path, testnet=False, strategy=None, passphrase="", base_currency="USD"):
        self.api_key = api_key
        self.api_secret = ""
        self.passphrase = passphrase
        self.testing = testnet
        self.strategy = strategy
        self.base_currency = base_currency
        self.database = strategy.database if strategy else None
        self.session = requests.Session()

        if not os.path.isfile(api_secret_file_path):
            raise OSError(f"[ERROR] Fireblocks private key path '{api_secret_file_path}' doesn't exist or is not a file")
        with open(api_secret_file_path, 'r') as fp:
            self.api_secret = fp.read()

        if self.testing:
            self.base_url = "https://sandbox-api.fireblocks.io"
        else:
            self.base_url = "https://api.fireblocks.io"

        self.__assets = {}
        self.__vaults = {}
        self.__external_wallets = {}
        self.__exchange_accounts = {}
        self.__internal_wallets = {}

######################
### Public methods ###
######################

    def get_vault_accounts(self, reload=False):
        if not self.__vaults or reload:
            self.__vaults = {}
            response = self.__api_get_vault_accounts()
            for account in response["accounts"]:
                assets = {}
                for asset in account["assets"]:
                    if float(asset["total"]) == 0:
                        continue
                    assets[asset["id"]] = {
                        "id": asset["id"],
                        "balance": float(asset["total"]),
                    }
                self.__vaults[account["name"]] = {
                    "id": account["id"],
                    "name": account["name"],
                    "assets": assets,
                }
        return self.__vaults

    def get_vault_account(self, account_id):
        response = self.__api_get_vault_account(account_id)
        return response

    def get_exchange_accounts(self, reload=False):
        if not self.__exchange_accounts or reload:
            response = self.__api_get_exchange_accounts()
            for account in response:
                assets = {}
                for asset in account["assets"]:
                    assets[asset["id"]] = {
                        "id": asset["id"],
                        "balance": float(asset["balance"]) if "balance" in asset else 0,
                    }
                self.__exchange_accounts[account["name"]] = {
                    "id": account["id"],
                    "name": account["name"],
                    "assets": assets,
                }
        return self.__exchange_accounts

    def get_exchange_account(self, account_id):
        response = self.__api_get_exchange_account(account_id)
        return response

    def get_internal_wallets(self, reload=False):
        if not self.__internal_wallets or reload:
            self.__internal_wallets = {}
            response = self.__api_get_internal_wallets()
            for internal_wallet in response:
                assets = {}
                for asset in internal_wallet["assets"]:
                    assets[asset["id"]] = {
                        "id": asset["id"],
                        "address": asset["address"],
                        "balance": float(asset["balance"]) if "balance" in asset else 0,
                    }
                self.__internal_wallets[internal_wallet["name"]] = {
                    "id": internal_wallet["id"],
                    "name": internal_wallet["name"],
                    "assets": assets,
                }
        return self.__internal_wallets

    def get_external_wallets(self, reload=False):
        if not self.__external_wallets or reload:
            self.__external_wallets = {}
            response = self.__api_get_external_wallets()
            for external_wallet in response:
                assets = {}
                for asset in external_wallet["assets"]:
                    assets[asset["id"]] = {
                        "id": asset["id"],
                        "address": asset["address"],
                        "balance": float(asset["balance"]) if "balance" in asset else 0,
                    }
                self.__external_wallets[external_wallet["name"]] = {
                    "id": external_wallet["id"],
                    "name": external_wallet["name"],
                    "assets": assets,
                }
        return self.__external_wallets

    def get_external_wallet(self, wallet_id):
        response = self.__api_get_external_wallet(wallet_id)
        return response

    def get_assets(self, reload=False):
        if not self.__assets or reload:
            self.__assets = {}
            response = self.__api_get_supported_assets()
            for asset in response:
                symbol = asset["id"]
                self.__assets[symbol] = {
                    "id": symbol,
                    "type": asset["type"],
                    "name": asset["name"],
                    "contract_address": asset["contractAddress"],
                    "native_asset": asset["nativeAsset"],
                    "decimals": asset["decimals"],
                }
            return self.__assets
        return self.__assets

    def get_transaction(self, transaction_id):
        response = self.__api_get_transaction(transaction_id)
        return response

    def get_transaction_history(self, src_type=None, src_id=None, dest_type=None, dest_id=None, symbols=[], before=None, after=None):
        if before:
            if isinstance(before, datetime.datetime):
                before = int(before.timestamp())
            elif isinstance(before, datetime.date):
                before = datetime.datetime.combine(before, datetime.max.time())
        if after:
            if isinstance(after, datetime.datetime):
                after = int(after.timestamp())
            elif isinstance(after, datetime.date):
                after = datetime.datetime.combine(after, datetime.min.time())
        response = self.__api_list_transaction_history(src_type, src_id, dest_type, dest_id, symbols, before, after)
        return response

    def make_transfer(self, symbol, amount, src_type, src_name, dest_type, dest_name):
        if not self.__assets:
            self.get_assets()
        if src_type not in ["VAULT_ACCOUNT", "EXTERNAL_WALLET", "EXCHANGE_ACCOUNT", "INTERNAL_WALLET"]:
            raise InvalidSourceTypeError(f"Unknown source type for transfer '{src_type} : Must be one of the followings : ['VAULT_ACCOUNT', 'EXTERNAL_WALLET', 'EXCHANGE_ACCOUNT']")
        if dest_type not in ["VAULT_ACCOUNT", "EXTERNAL_WALLET", "EXCHANGE_ACCOUNT", "INTERNAL_WALLET"]:
            raise InvalidSourceTypeError(f"Unknown destination type for transfer '{dest_type} : Must be one of the followings : ['VAULT_ACCOUNT', 'EXTERNAL_WALLET', 'EXCHANGE_ACCOUNT', 'INTERNAL_WALLET']")
        if (src_type == "VAULT_ACCOUNT" or dest_type == "VAULT_ACCOUNT") and not self.__vaults:
            self.get_vault_accounts()
        if (src_type == "EXTERNAL_WALLET" or dest_type == "EXTERNAL_WALLET") and not self.__external_wallets:
            self.get_external_wallets()
        if (src_type == "EXCHANGE_ACCOUNT" or dest_type == "EXCHANGE_ACCOUNT") and not self.__exchange_accounts:
            self.get_exchange_accounts()
        if (src_type == "INTERNAL_WALLET" or dest_type == "INTERNAL_WALLET") and not self.__internal_wallets:
            self.get_internal_wallets()
        #if src_type == "VAULT_ACCOUNT" and src_name not in self.__vaults:
        #    raise UnknownVaultError(f"Unknown source vault '{src_name}'")
        #if dest_type == "VAULT_ACCOUNT" and dest_name not in self.__vaults:
        #    raise UnknownVaultError(f"Unknown destination vault '{dest_name}'")
        #if src_type == "EXTERNAL_WALLET" and src_name not in self.__external_wallets:
        #    raise UnknownVaultError(f"Unknown source external wallet '{src_name}'")
        #if dest_type == "EXTERNAL_WALLET" and dest_name not in self.__external_wallets:
        #    raise UnknownVaultError(f"Unknown destination external wallet '{dest_name}'")
        #if src_type == "EXCHANGE_ACCOUNT" and src_name not in self.__exchange_accounts:
        #    raise UnknownVaultError(f"Unknown source exchange account '{src_name}'")
        #if dest_type == "EXCHANGE_ACCOUNT" and dest_name not in self.__exchange_accounts:
        #    raise UnknownVaultError(f"Unknown destination exchange account '{dest_name}'")
        #if src_type == "INTERNAL_WALLET" and src_name not in self.__internal_wallets:
        #    raise UnknownVaultError(f"Unknown source internal wallet '{src_name}'")
        #if dest_type == "INTERNAL_WALLET" and dest_name not in self.__internal_wallets:
        #    raise UnknownVaultError(f"Unknown destination internal wallet '{dest_name}'")

        if src_type == "VAULT_ACCOUNT":
            src = self.__vaults[src_name]
        if src_type == "EXTERNAL_WALLET":
            src = self.__external_wallets[src_name]
        if src_type == "EXCHANGE_ACCOUNT":
            src = self.__exchange_accounts[src_name]
        if src_type == "INTERNAL_WALLET":
            src = self.__internal_wallets[src_name]

        if dest_type == "VAULT_ACCOUNT":
            dest = self.__vaults[dest_name]
        if dest_type == "EXTERNAL_WALLET":
            dest = self.__external_wallets[dest_name]
        if dest_type == "EXCHANGE_ACCOUNT":
            dest = self.__exchange_accounts[dest_name]
        if dest_type == "INTERNAL_WALLET":
            dest = self.__internal_wallets[dest_name]

        #if symbol not in src["assets"]:
        #    raise InvalidVaultAssetError(f"Asset '{symbol}' not inside '{src_name}' source's assets : {list(src['assets'].keys())}")
        #if amount > src["assets"][symbol]["balance"]:
        #    raise InvalidVaultAssetAmountError(f"Amount '{amount}' '{symbol}' greater than available balance inside '{src_name}' source : {src['assets'][symbol]['balance']}")

        log(f"Create transfer of {amount} {symbol} from source '{src_name}' to destination '{dest_name}'")
        source = {
            "type": src_type,
            "id": src["id"],
            "name": src["name"],
        }
        destination = {
            "type": dest_type,
            "id": dest["id"],
            "name": dest["name"],
        }
        response = self.__api_create_transfer(symbol, amount, source, destination)
        return response

    def set_confirmation_threshold(self, transaction_id, threshold=0):
        response = self.__api_set_confirmation_threshold(transaction_id, threshold)
        return response

    def create_vault_account(self, vault_name, customer_reference=None):
        response = self.__api_create_vault_account(vault_name, customer_reference)
        return response

################
### REST API ###
################

########################
### Privates methods ###
########################

################
### REST API ###
################

    def __api_get_vault_accounts(self):
        response = self.__request("GET", "/v1/vault/accounts_paged", auth=True)
        return response

    def __api_get_vault_account(self, account_id):
        response = self.__request("GET", f"/v1/vault/accounts/{account_id}", auth=True)
        return response

    def __api_get_exchange_accounts(self):
        response = self.__request("GET", "/v1/exchange_accounts", auth=True)
        return response

    def __api_get_exchange_account(self, account_id):
        response = self.__request("GET", f"/v1/exchange_accounts/{account_id}", auth=True)
        return response

    def __api_get_internal_wallets(self):
        response = self.__request("GET", f"/v1/internal_wallets", auth=True)
        return response

    def __api_get_external_wallets(self):
        response = self.__request("GET", f"/v1/external_wallets", auth=True)
        return response

    def __api_get_external_wallet(self, wallet_id):
        response = self.__request("GET", f"/v1/external_wallets/{wallet_id}", auth=True)
        return response

    def __api_get_supported_assets(self):
        response = self.__request("GET", f"/v1/supported_assets", auth=True)
        return response

    def __api_get_transaction(self, transaction_id):
        response = self.__request("GET", f"/v1/transactions/{transaction_id}", auth=True)
        return response

    def __api_list_transaction_history(self, src_type=None, src_id=None, dest_type=None, dest_id=None, symbols=[], before=None, after=None):
        payload = {}
        if src_type:
            payload["sourceType"] = src_type
        if src_id:
            payload["sourceId"] = src_id
        if dest_type:
            payload["destinationType"] = dest_type
        if dest_id:
            payload["destId"] = dest_id
        if symbols:
            payload["assets"] = ','.join(symbols)
        if before:
            payload["before"] = str(before)
        if after:
            payload["after"] = str(after)
        response = self.__request("GET", f"/v1/transactions", payload=payload, auth=True)
        return response

    def __api_create_transfer(self, symbol, amount, source, destination):
        payload = {
            "operation": "TRANSFER",
            "assetId": symbol,
            "amount": str(amount),
            "source": source,
            "destination": destination,
        }
        response = self.__request("POST", f"/v1/transactions", payload=payload, auth=True)
        return response

    def __api_set_confirmation_threshold(self, transaction_id, threshold):
        response = self.__request("POST", f"/v1/transactions/{transaction_id}/set_confirmation_threshold", payload={
            "numOfConfirmations": threshold,
        }, auth=True)
        return response

    def __api_create_vault_account(self, vault_name, customer_reference=None):
        payload = {
            "name": vault_name,
        }
        if customer_reference:
            payload["customerRefId"] = customer_reference
        response = self.__request("POST", f"/v1/vault/accounts", payload=payload, auth=True)
        return response

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        if method == "GET":
            if payload:
                path += f"?{urlencode(payload)}"
            body = ""
        else:
            headers["Content-Type"] = "application/json"
            body = payload

        if auth:
            timestamp = self.__get_timestamp()
            nonce = secrets.randbits(63)
            data = {
                "uri": path,
                "nonce": nonce,
                "iat": timestamp,
                "exp": timestamp + 55,
                "sub": self.api_key,
                "bodyHash": sha256(json.dumps(body).encode("utf-8")).hexdigest()
            }

            signature = self.__generate_signature(data)
            headers["X-API-Key"] = self.api_key
            headers["Authorization"] = f"Bearer {signature}"

        url = f"{self.base_url}{path}"

        if method == "GET":
            response = self.session.request(method, url, data=body, headers=headers)
        elif method == "POST":
            response = self.session.request(method, url, json=body, headers=headers)
        else:
            response = self.session.request(method, url, data=body, headers=headers)

        self.__request_log(response)
        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)
            if "error" in response_json:
                raise requests.exceptions.HTTPError(f"{status_code} : {response_json['error']}")
            if "message" in response_json:
                raise requests.exceptions.HTTPError(f"{status_code} : {response_json['message']}")
            raise requests.exceptions.HTTPError(status_code)
        else:
            response_json = response.json()
            return response_json

    def __get_timestamp(self):
        return int(time.time())

    def __generate_signature(self, data):
        return jwt.encode(data, key=self.api_secret, algorithm="RS256")

    def __request_log(self, response):
        try:
            file_path = "fireblocks_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}] [{self.strategy}] [TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}] [{self.strategy}] [FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except:
            pass

    def __str__(self):
        return f"{type(self).__name__}"