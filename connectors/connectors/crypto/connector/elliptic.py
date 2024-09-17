import requests
import json
import base64
import hmac as crypto
import hashlib
import time


class Elliptic(object):

	def __init__(self ,elliptic_key ,elliptic_secret):
		self.elliptic_key = elliptic_key
		self.elliptic_secret = elliptic_secret
		self.base_url = 'https://aml-api.elliptic.co/v2'
		self.session = requests.Session()


	def __get_elliptic_sign(self, timestamp, http_method, http_path, payload):

		# create a SHA256 HMAC using the supplied secret, decoded from base64
		hmac = crypto.new(base64.b64decode(self.elliptic_secret), digestmod=hashlib.sha256)

		# concatenate the text to be signed
		request_text = timestamp + http_method + http_path.lower() + payload

		# update the HMAC with the text to be signed
		hmac.update(request_text.encode('UTF-8'))

		# output the signature as a base64 encoded string
		return base64.b64encode(hmac.digest()).decode('utf-8')

	def get_transaction_risk(self, transacton_id, customer_reference, output_address, type='transaction', asset='holistic', blockchain='holistic'):
		payload = {
			"subject": {
				"type": type,
				"asset": "holistic",
				"blockchain": "holistic",
				"hash": transacton_id,
				"output_address": output_address
			},
			"type": "source_of_funds",
			"customer_reference": customer_reference
		}

		timestamp = str(int(round(time.time() * 1000)))
		headers = {
			"accept": "application/json",
			"content-type": "application/json",
			"x-access-key": self.elliptic_key,
			"x-access-sign": self.__get_elliptic_sign(timestamp ,'POST' ,'/v2/wallet/synchronous' ,json.dumps(payload, separators = (',', ':'))),
			"x-access-timestamp": timestamp
		}

		analysis = self.__request("POST" ,"/wallet/synchronous" ,payload ,headers)
		return {'score': analysis["evaluation_detail"]['risk_score'], 'response': analysis["evaluation_detail"]}


	def get_wallet_risk(self, wallet):
		payload = {
			"subject": {
				"type": "address",
				"asset": "holistic",
				"blockchain": "holistic",
				"hash": wallet
			},
			"type": "wallet_exposure"
		}

		timestamp = str(int(round(time.time() * 1000)))
		headers = {
			"accept": "application/json",
			"content-type": "application/json",
			"x-access-key": self.elliptic_key,
			"x-access-sign": self.__get_elliptic_sign(timestamp ,'POST' ,'/v2/wallet/synchronous' ,json.dumps(payload, separators = (',', ':'))),
			"x-access-timestamp": timestamp
		}

		analysis = self.__request("POST" ,"/wallet/synchronous" ,payload ,headers)
		return {'wallet': wallet, 'score': analysis["risk_score"], 'detail': analysis["risk_score_detail"],'error': analysis["error"]}


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
		if status_code != 200:
			raise requests.exceptions.HTTPError(f"{status_code} : {response.text}")
		else:
			return response.json()
