import requests
import hashlib
import hmac
import json

class BinanceKyc:

    def __init__(self, business_entity_key, api_secret):
        self.business_entity_key = business_entity_key
        self.api_secret = api_secret
        self.base_url = "https://cb.link-kycapi.com"
        self.session = requests.Session()

    def check_kyc_status(self, customer_id, kyc_type, main_account_id, subacccount_id):
        response = self.__api_check_kyc_status(customer_id, kyc_type, main_account_id, subacccount_id)
        return response

    def generate_kyc_url(self, customer_id, kyc_type, main_account_id, subacccount_id, webhook_url, redirect_url):
        response = self.__api_generate_kyc_url(customer_id, kyc_type, main_account_id, subacccount_id, webhook_url, redirect_url)
        return response

    def add_subaccount_to_customer(self, customer_id, main_account_id, subacccount_id):
        response = self.__api_add_account(customer_id, main_account_id, subacccount_id)
        return response

    def __api_check_kyc_status(self, customer_id, kyc_type, main_account_id, subacccount_id):
        response = self.__request("POST", "/bapi/ekyc/v2/public/ekyc/customer/check-kyc-status", {
            "bizEntityKey": self.business_entity_key,
            "entityCustomerId": customer_id,
            "kycType": kyc_type,
            "brokerMainAccountId": main_account_id,
            "brokerSubAccountId": subacccount_id,
        }, auth=True)
        return response

    def __api_generate_kyc_url(self, customer_id, kyc_type, main_account_id, subacccount_id, webhook_url, redirect_url):
        response = self.__request("POST", "/bapi/ekyc/v2/public/ekyc/customer/initial", {
            "bizEntityKey": self.business_entity_key,
            "entityCustomerId": customer_id,
            "kycType": kyc_type,
            "brokerMainAccountId": main_account_id,
            "brokerSubAccountId": subacccount_id,
            "notifyUrl": webhook_url,
            "redirectUrl": redirect_url,
        }, auth=True)
        return response

    def __api_add_account(self, customer_id, main_account_id, subacccount_id):
        response = self.__request("POST", "/bapi/ekyc/v2/public/ekyc/customer/add-account-info-list", {
            "bizEntityKey": self.business_entity_key,
            "accountList": [{
                "entityCustomerId": customer_id,
                "brokerMainAccountId": main_account_id,
                "brokerSubAccountId": subacccount_id,
            }],
        }, auth=True)
        return response

    def __request(self, method, path, payload=None, auth=False, headers=None):
        if payload is None:
            payload = {}
        if headers is None:
            headers = {}

        url = f"{self.base_url}{path}"

        if auth:
            signature = self.__generate_signature(json.dumps(payload, separators=(',', ':')))
            headers["X-SHA2-Signature"] = signature

        print("--------------------------------------------------------")
        print(f"data : {json.dumps(payload, separators=(',', ':'))}")
        print("--------------------------------------------------------")
        print(f"signature : {signature}")
        print("--------------------------------------------------------")

        response = self.session.request(method, url, data=json.dumps(payload, separators=(',', ':')), headers=headers)

        request = response.request
        print("--------------------------------------------------------")
        print(f"{request.method} {request.url}")
        print("--------------------------------------------------------")
        print(f"{request.headers}")
        print("--------------------------------------------------------")
        print(f"{request.body}")
        print("--------------------------------------------------------")
        print(response)
        print("--------------------------------------------------------")
        print(response.headers)
        print("--------------------------------------------------------")
        print(response.text)
        print("--------------------------------------------------------")

        return response

    def __generate_signature(self, data):
        hash = hmac.new(self.api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256)
        return hash.hexdigest()
