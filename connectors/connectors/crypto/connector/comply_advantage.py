from connectors.crypto.common.logger import log
import requests
import json


class ComplyAdvantage(object):

    def __init__(self, api_key):
        self.comply_advantage_key = api_key
        self.base_url = "https://api.complyadvantage.com"
        self.session = requests.Session()

    def get_user_risk(self, search_term, *, is_company, birth_year=None, country_codes=None, fuzziness=0.6):
        headers = {'Authorization': 'Token ' + self.comply_advantage_key}
        data = {
            "search_term": search_term,
            "fuzziness": fuzziness,
            "filters": {
                "types": [
                    "sanction",
                    "warning"
                ]
            },
        }

        if is_company:
            data["filters"]["entity_type"] = 'company'
        else:
            data["filters"]["entity_type"] = 'person'
            if birth_year is not None:
                data["filters"]["birth_year"] = birth_year

        if isinstance(country_codes, list) and country_codes is not None:
            data["filters"]["country_codes"] = country_codes

        analysis = self.__request("POST", "/searches", data, headers)
        return {"id":  analysis["content"]["data"]["id"], "ref":  analysis["content"]["data"]["ref"], "score": analysis["content"]["data"]["risk_level"], "matches": analysis["content"]["data"]["total_matches"]}

    def __request(self, method, path, payload=None, headers=None):
        if not payload:
            payload = {}
        if not headers:
            headers = {}

        url = f"{self.base_url}{path}"

        response = self.session.request(method, url, data=json.dumps(payload), headers=headers)

        return self.__parse_response(response)

    def __parse_response(self, response):
        status_code = response.status_code
        if status_code == 429:
            log(f"[ERROR][Comply Advantage] Too many requests error")
        if status_code != 200:
            raise requests.exceptions.HTTPError(f"{status_code} : {response.text}")
        else:
            return response.json()
