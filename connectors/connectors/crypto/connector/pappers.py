from urllib.parse import urlencode
import requests
import datetime
import json

class Pappers:

    def __init__(self, api_key, database=None):
        self.api_key = api_key
        self.database = database
        self.base_url = "https://api.pappers.fr/v2/"

        self.session = requests.Session()

    def get_company(self, siret=None):
        payload = {
            "siren": siret,
        }

        response = self.__request("GET", "entreprise", payload=payload, auth=True)
        print(response)
        sys.exit(1)
        return response

    def get_document(self, token, filepath=None, extension="pdf"):
        payload = {
            "token": token,
        }
        if not filepath:
            filepath = f"{token}.{extension}"

        response = self.__request("GET", "document/telechargement", payload=payload, auth=True, parse="binary")

        with open(filepath, "wb") as fp:
            fp.write(response)

        return filepath

    def __request(self, method, path, payload=None, headers=None, auth=False, parse="json"):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        if auth:
            payload["api_token"] = self.api_key

        if method == "GET":
            payload_str = f"?{urlencode(payload)}" if payload else ""
            response = self.session.request(method, f"{url}{payload_str}", headers=headers)
        else:
            response = self.session.request(method, url, data=json.dumps(payload), headers=headers)

        self.__request_log(response)

        return self.__parse_response(response, parse)

    def __parse_response(self, response, parse="json"):
        status_code = response.status_code
        if status_code != 200:
            raise requests.exceptions.HTTPError(f"{status_code} : {response.text}")
        elif parse == "json":
            return response.json()
        elif parse == "binary":
            return response.content
        else:
            return response

    def __request_log(self, response):
        try:
            file_path = "papers_requests.log"
            timestamp = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")
            with open(file_path, "a") as file:
                request = response.request
                request_body = request.body if request.body else ""
                file.write(f"[{timestamp}] [{self}] [TO] - HEADERS '{request.headers}' - REQUEST '{request.method} {request.url} {request_body}'\n")
                file.write(f"[{timestamp}] [{self}] [FROM] - {response} - HEADERS '{response.headers}' - '{response.text}'\n")
        except Exception as e:
            pass

    def __str__(self):
        return self.__class__.__name__