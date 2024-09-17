from connectors.crypto.connector.exchanges.coinbase import CbForex
from connectors.crypto.common.logger import log
from urllib.parse import urlencode
import requests
import datetime
import hashlib
import hmac
import time
import json

class BinanceLinkTransfer:

    def __init__(self, id, transaction_dict, transaction_type):
        self.id = id
        self.transaction_type = transaction_type
        self.tx_hash = transaction_dict["txnId"]
        self.status = transaction_dict["status"]
        self.source = transaction_dict["fromId"]
        self.destination = transaction_dict["toId"]
        self.amount = transaction_dict["qty"]
        self.symbol = transaction_dict["asset"]
        self.created_at = datetime.datetime.fromtimestamp(transaction_dict["time"]/1000)

    def to_database(self, database):
        source_strategy_id = None
        destination_strategy_id = None

        if self.source:
            source_strategy = database.getTable("strategies", arg=f"WHERE binance_link_subaccount_id = '{self.source}'")
            if not source_strategy.empty:
                source_strategy_id = source_strategy["id"].values[0]

        if self.destination:
            destination_strategy = database.getTable("strategies", arg=f"WHERE binance_link_subaccount_id = '{self.destination}'")
            if not destination_strategy.empty:
                destination_strategy_id = destination_strategy["id"].values[0]

        print(f"id : {self.id}")
        print(f"transaction_type : {self.transaction_type}")
        print(f"tx_hash : {self.tx_hash}")
        print(f"status : {self.status}")
        print(f"source : {self.source}")
        print(f"source_strategy_id : {source_strategy_id}")
        print(f"destination : {self.destination}")
        print(f"destination_strategy_id : {destination_strategy_id}")
        print(f"amount : {self.amount}")
        print(f"symbol : {self.symbol}")
        print(f"created_at : {self.created_at}")

        if self.status == "SUCCESS":
            now = datetime.datetime.now()

            # Fees transfer
            if self.transaction_type == "fees":
                #TODO: Do we need to handle the different types of fees ?
                if source_strategy_id:
                    database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params=(source_strategy_id, 'fees', self.tx_hash, self.created_at, self.amount, self.symbol, 0, 0, 1, True, now, now))
                    pass

                #TODO: Do we need to add a cashflow for incoming fees ?
                if destination_strategy_id:
                    pass

            if self.transaction_type == "brokering_fees":
                #TODO: Do we need to handle the different types of fees ?
                if source_strategy_id:
                    database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params=(source_strategy_id, 'brokering_fees', self.tx_hash, self.created_at, self.amount, self.symbol, 0, 0, 1, True, now, now))
                    pass

            # Feeder transfer
            elif self.transaction_type == "feeder_cashflow":
                #TODO: implement feeder cashflows
                pass
            # Internal transfer
            elif self.transaction_type == "internal_transfer":
                # Create cashflow for outgoing withdraw from internal transfer
                if source_strategy_id:

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
                    log(f"Add new cashflow '{cashflow_type}' for strategy {source_strategy_id} : {self.amount} {self.symbol}")
                    database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params=(source_strategy_id, cashflow_type, self.tx_hash, self.created_at, self.amount, self.symbol, number_shares_before, number_shares_after, nav, True, now, now))

                # Create cashflow for incoming deposit from internal transfer
                if destination_strategy_id:
                    self.__register_cashflow(destination_strategy_id, 'internal_deposit', database)

    def __register_cashflow(self, strategy_id, cashflow_type, database):
        log(f"Add new cashflow '{cashflow_type}' for strategy {strategy_id} : {self.amount} {self.symbol}")
        now = datetime.datetime.now()

        base_currency_symbol = database.getTable("strategies", arg=f"WHERE id = {strategy_id}")["base_currency"].values[0]

        # Get number shares and nav for strategy
        balances = database.getTable("users_balances", arg=f"WHERE strategy_id = {strategy_id}")
        if balances.empty:

            # Compute balances and shares
            nav = 1
            number_shares_before = 0
            number_shares_after = self.amount*CbForex(database).get_rate(self.symbol, base_currency_symbol, self.created_at)
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
        params=(strategy_id, cashflow_type, self.tx_hash, self.created_at, self.amount, self.symbol, number_shares_before, number_shares_after, nav, True, now, now))

class BinanceLink:
    PLATFORM_NAME = "BinanceLink"

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.binance.com"
        self.session = requests.Session()

    def get_broker_account(self):
        response = self.__api_get_broker_account()
        return response

    def get_subaccounts(self):
        response = self.__api_get_subaccounts()
        return response

    def get_subaccount(self, account_id):
        response = self.__api_get_subaccounts(account_id)
        return response

    def create_subaccount(self, name=None):
        response = self.__api_create_subaccount(name)
        return response

    def create_api_key(self, subaccount_id, can_trade=True):
        response = self.__api_create_api_key(subaccount_id, can_trade)
        return response

    def get_api_keys(self, subaccount_id):
        response = self.__api_get_api_keys(subaccount_id)
        return response

    def get_api_key(self, subaccount_id, api_key):
        response = self.__api_get_api_keys(subaccount_id, api_key)
        return response

    def delete_api_key(self, subaccount_id, subaccount_api_key):
        response = self.__api_delete_api_key(subaccount_id, subaccount_api_key)
        return response

    def set_subaccount_fees(self, subaccount_id, maker_fee, taker_fee):
        response = self.__api_change_subaccount_commission(subaccount_id, maker_fee, taker_fee)
        return response

    def get_ip_restriction(self, subaccount_id, api_key):
        response = self.__api_get_ip_restriction(subaccount_id, api_key)
        return response

    def add_ip_restriction(self, subaccount_id, api_key, ip):
        response = self.__api_update_ip_restriction(subaccount_id, api_key, 2, ip)
        return response

    def enable_ip_restriction(self, subaccount_id, api_key):
        response = self.__api_update_ip_restriction(subaccount_id, api_key, 2)
        return response

    def disable_ip_restriction(self, subaccount_id, api_key):
        response = self.__api_update_ip_restriction(subaccount_id, api_key, 1)
        return response

    def delete_ip_restriction(self, subaccount_id, api_key, ip):
        response = self.__api_delete_ip_restriction(subaccount_id, api_key, ip)
        return response

    def set_api_key_universal_transfer_permission(self, subaccount_id, api_key, status):
        response = self.__api_enable_universal_transfer(subaccount_id, api_key, status)
        return response

    def get_commissions(self, subaccount_id, start_date=None, end_date=None):
        data = []

        response = self.get_subaccount(subaccount_id)
        if len(response) == 0:
            return []

        if not start_date:
            subaccount_creation = datetime.datetime.fromtimestamp(response[0]["createTime"]/1000)
            start_date = subaccount_creation
        if not end_date:
            end_date = datetime.datetime.now()

        index = 0
        stop = end_date
        start = end_date
        time_delta = datetime.timedelta(days=7)
        while start > start_date:
            stop = end_date - index*time_delta
            if stop - start_date > time_delta:
                start = end_date - (index+1)*time_delta
            else:
                start = start_date

            page = 1
            commissions = []
            response = self.__api_get_broker_commission_recent_record(subaccount_id, round(start.timestamp())*1000, round(stop.timestamp())*1000)
            commissions.extend(response)

            while len(response) >= 500:
                page += 1
                response = self.__api_get_broker_commission_recent_record(subaccount_id, round(start.timestamp())*1000, round(stop.timestamp())*1000, offset=page)
                commissions.extend(response)

            for commission in commissions:
                data.append({
                    "subaccount_id": commission["subaccountId"],
                    "income": float(commission["income"]),
                    "asset": commission["asset"],
                    "symbol": commission["symbol"],
                    "date": datetime.datetime.fromtimestamp(commission["time"]/1000),
                    "trade_id": commission["tradeId"],
                    "status": commission["status"],
                })

            index += 1

        return data

    def get_transfer_history(self, from_subaccount_id=None, to_subaccount_id=None, custom_id=None):
        response = self.__api_get_subaccount_transfer_history(from_subaccount_id, to_subaccount_id, custom_id=custom_id)
        return response

    def make_transfer(self, symbol, amount, from_subaccount_id=None, to_subaccount_id=None, custom_id=None):
        response = self.__api_transfer(symbol, amount, from_subaccount_id, to_subaccount_id, custom_id=custom_id)
        return response

    def get_subaccounts_margin_summary(self):
        response = self.__api_get_margin_summary()
        return response

    def get_subaccount_margin_summary(self, subaccount_id):
        response = self.__api_get_margin_summary(subaccount_id)
        return response

    def get_subaccount_bnb_burn_status(self, subaccount_id):
        response = self.__api_get_subaccount_bnb_burn_status(subaccount_id)
        return response

    def change_subaccount_bnb_burn_status(self, subaccount_id, enable):
        response = self.__api_change_subaccount_bnb_burn_status(subaccount_id, enable)
        return response

    def __api_get_broker_account(self):
        response = self.__request("GET", "/sapi/v1/broker/info", auth=True)
        return response

    def __api_get_subaccounts(self, account_id=None):
        payload = {}
        if account_id:
            payload["subAccountId"] = account_id
        response = self.__request("GET", "/sapi/v1/broker/subAccount", payload=payload, auth=True)
        return response

    def __api_create_subaccount(self, tag=None):
        payload = {}
        if tag:
            payload["tag"] = tag[:32]
        response = self.__request("POST", "/sapi/v1/broker/subAccount", payload=payload, auth=True)
        return response

    def __api_create_api_key(self, subaccount_id, can_trade=True):
        payload = {
            "subAccountId": str(subaccount_id),
            "canTrade": str(can_trade),
        }
        response = self.__request("POST", "/sapi/v1/broker/subAccountApi", payload=payload, auth=True)
        return response

    def __api_get_api_keys(self, subaccount_id, api_key=None):
        payload = {
            "subAccountId": subaccount_id
        }
        if api_key:
            payload["subAccountApiKey"] = api_key
        response = self.__request("GET", "/sapi/v1/broker/subAccountApi", payload=payload, auth=True)
        return response

    def __api_delete_api_key(self, subaccount_id, subaccount_api_key):
        payload = {
            "subAccountId": str(subaccount_id),
            "subAccountApiKey": str(subaccount_api_key),
        }
        response = self.__request("DELETE", "/sapi/v1/broker/subAccountApi", payload=payload, auth=True)
        return response

    def __api_change_subaccount_commission(self, subaccount_id, maker_fee, taker_fee):
        payload = {
            "subAccountId": str(subaccount_id),
            "makerCommission": maker_fee,
            "takerCommission": taker_fee,
        }
        response = self.__request("POST", "/sapi/v1/broker/subAccountApi/commission", payload=payload, auth=True)
        return response

    def __api_get_broker_commission_recent_record(self, subaccount_id, start_timestamp=None, stop_timestamp=None, offset=None):
        payload = {}
        if subaccount_id:
            payload["subAccountId"] = str(subaccount_id)
        if start_timestamp:
            payload["startTime"] = start_timestamp
        if stop_timestamp:
            payload["endTime"] = stop_timestamp
        if offset:
            payload["page"] = offset
        response = self.__request("GET", "/sapi/v1/broker/rebate/recentRecord", payload=payload, auth=True)
        return response

    def __api_get_subaccount_transfer_history(self, from_subaccount_id=None, to_subaccount_id=None, custom_id=None, show_all_status=True):
        payload = {}
        if from_subaccount_id:
            payload["fromId"] = str(from_subaccount_id)
        if to_subaccount_id:
            payload["toId"] = str(to_subaccount_id)
        if custom_id:
            payload["clientTranId"] = custom_id
        if show_all_status:
            payload["showAllStatus"] = "true"
        response = self.__request("GET", "/sapi/v1/broker/transfer", payload=payload, auth=True)
        return response

    def __api_transfer(self, symbol, amount, from_subaccount_id=None, to_subaccount_id=None, custom_id=None):
        payload = {
            "asset": symbol,
            "amount": amount,
        }
        if from_subaccount_id:
            payload["fromId"] = from_subaccount_id
        if to_subaccount_id:
            payload["toId"] = to_subaccount_id
        if custom_id:
            payload["clientTranId"] = custom_id
        response = self.__request("POST", "/sapi/v1/broker/transfer", payload=payload, auth=True)
        return response

    def __api_get_margin_summary(self, subaccount_id=None):
        payload = {}
        if subaccount_id:
            payload["subAccountId"] = subaccount_id
        response = self.__request("GET", "/sapi/v1/broker/subAccount/marginSummary", payload=payload, auth=True)
        return response

    def __api_get_subaccount_bnb_burn_status(self, subaccount_id):
        payload = {
            "subAccountId": str(subaccount_id),
        }
        response = self.__request("GET", "/sapi/v1/broker/subAccount/bnbBurn/status", payload=payload, auth=True)
        return response

    def __api_change_subaccount_bnb_burn_status(self, subaccount_id, state):
        payload = {
            "subAccountId": str(subaccount_id),
            "spotBNBBurn": "true" if state else "false",
        }
        response = self.__request("POST", "/sapi/v1/broker/subAccount/bnbBurn/spot", payload=payload, auth=True)
        return response

    def __api_get_ip_restriction(self, subaccount_id, api_key):
        payload = {
            "subAccountId": str(subaccount_id),
            "subAccountApiKey": str(api_key),
        }
        response = self.__request("GET", "/sapi/v1/broker/subAccountApi/ipRestriction", payload=payload, auth=True)
        return response

    def __api_update_ip_restriction(self, subaccount_id, api_key, status, ip=None):
        payload = {
            "subAccountId": str(subaccount_id),
            "subAccountApiKey": str(api_key),
            "status": 2,
        }
        if ip:
            payload["ipAddress"] = str(ip)
        response = self.__request("POST", "/sapi/v2/broker/subAccountApi/ipRestriction", payload=payload, auth=True)
        return response

    def __api_delete_ip_restriction(self, subaccount_id, api_key, ip):
        payload = {
            "subAccountId": str(subaccount_id),
            "subAccountApiKey": str(api_key),
            "ipAddress": str(ip),
        }
        response = self.__request("DELETE", "/sapi/v1/broker/subAccountApi/ipRestriction/ipList", payload=payload, auth=True)
        return response

    def __api_enable_universal_transfer(self, subaccount_id, api_key, status):
        payload = {
            "subAccountId": str(subaccount_id),
            "subAccountApiKey": str(api_key),
            "ipAddress": "true" if status else "false",
        }
        response = self.__request("POST", "/sapi/v1/broker/subAccountApi/permission/universalTransfer", payload=payload, auth=True)
        return response

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if payload is None:
            payload = {}
        if headers is None:
            headers = {}

        headers["Content-Type"] = "application/x-www-form-urlencoded"

        url = f"{self.base_url}{path}"

        if auth:
            headers["X-MBX-APIKEY"] = self.api_key
            payload["timestamp"] = self.__get_timestamp()
            query_string = self.__query_string(payload)
            signature = self.__generate_signature(query_string)
            payload["signature"] = signature

            #print("---------------------------------------------")
            #print(f"data : {query_string}")
            #print(f"signature : {signature}")
            #print("---------------------------------------------")
            #print(json.dumps(payload, indent=4))
            #print("---------------------------------------------")

        response = self.session.request(method, url, params=payload, headers=headers)

        self.__request_log(response)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            try:
                response_json = response.json()
            except:
                raise requests.exceptions.HTTPError(status_code)
            raise requests.exceptions.HTTPError("{} : {}".format(status_code, response_json["msg"]))
        else:
            if not response.text:
                return {}
            else:
                response_json = response.json()
                return response_json

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
            return urlencode(filtered_query, True)

    def __request_log(self, response):
        try:
            file_path = "binance_link_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}][TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}][FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except:
            pass

    def __str__(self):
        return f"{self.PLATFORM_NAME}"