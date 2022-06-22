import requests
import json
from loguru import logger


class Ozonpop:
    def __init__(self, client_id: str, apikey: str):
        self.client_id = client_id
        self.apikey = apikey
        self.headers = {"Client-Id": client_id, "Api-Key": apikey}
        self.url = 'https://api-seller.ozon.ru'

    def get_action_candidates(self, action_id, offset=0, count=100):

        d_candidates = {
          "action_id": action_id,
          "limit": count,
          "offset": offset
        }
        s_req = f'{self.url}/v1/actions/candidates'
        res = requests.post(s_req, headers=self.headers, json=d_candidates)
        if not res.ok:
            return
        return json.loads(res.text)

    def get_actions_info(self):
        s_req = f'{self.url}/v1/actions'
        list_candidates = []
        res = requests.get(s_req, headers=self.headers)
        if not res.ok:
            return
        d_actions = json.loads(res.text)
        if 'result' in d_actions.keys():
            for action in d_actions['result']:
                i_off = 0
                while True:
                    d_candidates = self.get_action_candidates(action['id'], offset=i_off)
                    if 'result' in d_candidates.keys():
                        list_products = d_candidates['result']['products']
                        list_candidates.append(product['id'] for product in list_products)
                        i_off += int(d_candidates['result']['total'])
                    else:
                        break
        return list_candidates

    def get_postings_fbs(self, date_from, date_to, status):
        s_req = f'{self.url}/v3/posting/fbs/list'
        limit = 200

        def get_page(offset_p=0):
            d_params = {
                "dir": "ASC",
                "filter": {
                    "since": date_from,
                    "status": status,
                    "to": date_to
                },
                "limit": limit,
                "offset": offset_p,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }
            return requests.post(s_req, headers=self.headers, json=d_params)

        postings = []
        offset = 0
        while True:
            response = get_page(offset_p=offset)
            if not response.ok:
                logger.info(f"{response.status_code}, {response.text}")

            d_postings = json.loads(response.text)
            if 'result' in d_postings.keys():
                d_postings = d_postings['result']
                postings += d_postings['postings']
                if d_postings['has_next']:
                    offset += len(d_postings['postings'])
                else:
                    break
        return postings

    def get_postings_fbo(self, date_from, date_to, status):
        s_req = f'{self.url}/v2/posting/fbo/list'
        limit = 200

        def get_page(offset_p=0):
            d_params = {
                "dir": "ASC",
                "filter": {
                    "since": date_from,
                    "status": status,
                    "to": date_to
                },
                "limit": limit,
                "offset": offset_p,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }
            return requests.post(s_req, headers=self.headers, json=d_params)

        postings = []
        offset = 0
        while True:
            response = get_page(offset_p=offset)
            if not response.ok:
                logger.info(f"{response.status_code}, {response.text}")

            d_postings = json.loads(response.text)
            if 'result' in d_postings.keys():
                for order in d_postings['result']:
                    postings += order
            else:
                break
        return postings

