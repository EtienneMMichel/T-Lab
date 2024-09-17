import requests
import json
import time

class Sakana:

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.base_url = "https://api.sakana.capital"

        self.session = requests.Session()

        self.__auth_token_ttl = 30
        self.__auth_token = None
        self.__auth_timeout = None

    def get_scores(self):
        response = self.__request("GET", "/last_update", auth=True)
        return response

    def __request(self, method, path, payload=None, headers=None, auth=False):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        if auth:
            if not self.__is_authenticated():
                self.__authenticate()
            headers["Authorization"] = "Bearer " + self.__auth_token

        response = self.session.request(method, url, data=json.dumps(payload), headers=headers)
        request = response.request

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code != 200:
            raise requests.exceptions.HTTPError(f"{status_code} : {response.text}")
        else:
            return response.json()

    def __authenticate(self):
        response = self.__request("POST", "/login",
        payload={
            "login": self.login,
            "password": self.password,
        },
        headers={
            "Content-type": "application/json",
            "Accept": "application/json",
        })
        self.__auth_token = response["access_token"]
        self.__auth_timeout = time.time() + self.__auth_token_ttl

    def __is_authenticated(self):
        if not self.__auth_timeout:
            return False
        if self.__auth_timeout < time.time():
            return False
        return True