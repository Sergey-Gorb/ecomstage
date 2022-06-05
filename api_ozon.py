import requests
import json


class Ozonpop:
    def __init__(self, client_id: str, apikey: str):
        self.client_id = client_id
        self.apikey = apikey
        self.headers = {"Client-Id": client_id, "Api-Key": apikey}
        self.url = 'https://api-seller.ozon.ru/v1/actions'

    def get_action_candidates(self, action_id, offset=0, count=100):

        d_candidates = {
          "action_id": action_id,
          "limit": count,
          "offset": offset
        }
        s_req = f'{self.url}/candidates'
        res = requests.post(s_req, headers=self.headers, json=d_candidates)
        if not res.ok:
            return
        return json.loads(res.text)

    def get_actions_info(self):
        s_req = f'{self.url}'
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

