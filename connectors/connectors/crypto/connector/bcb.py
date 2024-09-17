import requests
import time
import json

class BCB:

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client_name = "TILVEST"
        self.base_url = "https://api.production.bcb.group"
        self.session = requests.Session()
        self.__auth_token = None
        self.__auth_timeout = None

        self.__auth_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlFVVXlSa0k1TWpSRlJVTTVRVVF3UlRCRk5FVkRSRGt4TURBM01rSTJNREJGUVRZMU9UQTRRZyJ9.eyJodHRwczovL2xvZ2luLmJjYmdyb3VwLmlvL2NsaWVudF9lbWFpbCI6InByb2R1Y3Rpb25AdGlsdmVzdC5jb20iLCJpc3MiOiJodHRwczovL2JjYi5ldS5hdXRoMC5jb20vIiwic3ViIjoiV1pwek1LYUJEVndmUWduZkVkc3o1Q2VjTXdybk9SNUNAY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vYXBpLnByb2R1Y3Rpb24uYmNiLmdyb3VwIiwiaWF0IjoxNzE4MjcwNzIyLCJleHAiOjE3MjA4NjI3MjIsInNjb3BlIjoiYmNiOmFwaSBjdXN0b20iLCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJXWnB6TUthQkRWd2ZRZ25mRWRzejVDZWNNd3JuT1I1QyJ9.aB6jjk32LlDGM6MKA_Xex8-h-Cg-yegmOzUowaxxzVTgXv1YmXCk-NyaNwXt5s359GpALUm3IsxM9YB9V8ceDzQlRdsD-nTEgPCd0F2Lba1fTytXJjGowQFnvd_Ou6Y-MCvkm96iprjWAzOMptRt1y-C6OConbYwLRJvYgW88Gm6Ds9NaOIkPrj4HF6DIslJnaLRpoY6e-iYoe-QH7KLl79NsZVbaDRhyVrxybdHRELwDZt8s_aLWd3doZ2p_7FlguMn2TqO9YKSbWO2tPuBcjtOvzgjCN_aDtEQBHRe0VCzivmpj2A29HR2URbXTfTRT4T-o4PzlqHvOEyTDx5Yeg"

    def get_accounts(self):
        response = self.__request("GET", "/v3/accounts")
        return response

    def get_account_balance(self, account_id):
        response = self.__request("GET", f"/v3/accounts/{account_id}")
        return response

    def get_account_transactions(self, account_id):
        response = self.__request("GET", f"/v3/accounts/{account_id}/transactions")
        return response

    def get_account_transaction(self, account_id, transaction_id):
        response = self.__request("GET", f"/v3/accounts/{account_id}/transactions/{transaction_id}")
        return response

    def get_beneficiaries(self):
        response = self.__request("GET", f"/v3/beneficiaries")
        return response

    def create_beneficiary(self, data):
        #TODO
        pass

    def authorize_payment(self, beneficiary_account_id, sender_account_id, counterparty_id, currency, amount, reason, reference, preferred_scheme):
        #AUTO > for external payments
        #BLINC > for payments inside BLINC network, must use BLINC accounts
        #INTERNAL > for payments between BCB accounts of the same counterparty

        payload = {
            "counterparty_id": counterparty_id,
            "sender_account_id": sender_account_id,
            "beneficiary_account_id": beneficiary_account_id,
            "ccy": currency,
            "amount": amount,
            "reason": reason,
            "reference": reference,
            "preferred_scheme": preferred_scheme,
        }

        response = self.__request("POST", f"/v4/payments/authorise", payload=payload)
        return response

    def test(self):
        self.__authenticate()

    def __request(self, method, path, payload=None, auth=True, headers=None, base_url=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}" if not base_url else f"{base_url}{path}"
        headers["Content-Type"] = "application/json"

        if auth:
            if not self.__is_authenticated():
                self.__authenticate()
            headers["Authorization"] = f"Bearer {self.__auth_token}"

        response = self.session.request(method, url, data=json.dumps(payload), headers=headers)

        print("--------------------------------------------------------")
        print(f"{response.request.method} {response.request.url}")
        print("--------------------------------------------------------")
        print(f"{response.request.headers}")
        print("--------------------------------------------------------")
        print(response.request.body if response.request.body else "")
        print("--------------------------------------------------------")
        print(response)
        print(response.text)
        print("--------------------------------------------------------")

        return response.json()

    def __is_authenticated(self):
        #if not self.__auth_token or not self.__auth_timeout or time.time() > self.__auth_timeout():
        if not self.__auth_token:
            return False
        return True

    def __authenticate(self):
        print("Authenticating")
        response = self.__request("POST", "/oauth/token", payload={
            "client_id": self.api_key,
            "client_secret": self.api_secret,
            "audience": "https://api.production.bcb.group",
            "grant_type":"client_credentials",
        }, auth=False, base_url="https://auth.production.bcb.group")
        self.__auth_token = response["access_token"]
        print(f"Got auth token : '{self.__auth_token}'")
        #self.__auth_timeout = time.time() + response["expires_in"]

