import requests
import json
from pprint import pprint
from loguru import logger
import pandas as pd
from request_page import get_page_post
from pandas.io.json import json_normalize
from tools import dict_flatten, list_dict_flatten


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

    def get_postings_fbs(self, date_from, date_to):
        s_req = f'{self.url}/v3/posting/fbs/list'
        limit = 200
        l_postings = []
        offset = 0
        d_params = {
            "dir": "ASC",
            "filter": {
                "since": date_from,
                "to": date_to
            },
            "limit": limit,
            "offset": offset,
            "translit": True,
            "with": {
                "analytics_data": True,
                "financial_data": True
            }
        }
        posting_df = pd.DataFrame()
        while True:
            #   response = get_page(offset_p=offset)
            d_params['offset'] = offset
            response = get_page_post(s_req, headers=self.headers, json=d_params)

            d_postings = json.loads(response.text)
            if 'result' in d_postings.keys() and d_postings['result']:
                postings = d_postings['result']['postings']
                for posting_raw in postings:
                    offset += 1
                    posting = dict_flatten(posting_raw, separator=None)

                    works_data = pd.json_normalize(posting,
                                                   max_level=0,
                                                   sep='_'
                                                   )
                    products_data = pd.json_normalize(posting_raw,
                                                      record_path=['products'],
                                                      meta=['order_id'],
                                                      )
                    temp = list_dict_flatten(posting_raw['financial_data']['products'], parent_key='fd')
                    fd_products_data = pd.json_normalize(temp)
                    products_merge = pd.merge(products_data, fd_products_data, how='left',
                                              left_on='sku', right_on='fd_product_id')
                    all_merge = pd.merge(products_merge, works_data, how='left',
                                         left_on='order_id', right_on='order_id')
                    temp = all_merge.to_dict('records')
                    l_postings.extend(temp)
                if d_postings['result']['has_next']:
                    pass
                else:
                    break
            else:
                break
        posting_df = pd.DataFrame(l_postings)
        return posting_df

    def get_postings_fbo(self, date_from, date_to):
        s_req = f'{self.url}/v2/posting/fbo/list'
        limit = 200
        postings = []
        offset = 0
        posting_df = pd.DataFrame()
        while True:
            d_params = {
                "dir": "ASC",
                "filter": {
                    "since": date_from,
                    "to": date_to
                },
                "limit": limit,
                "offset": offset,
                "translit": True,
                "with": {
                    "analytics_data": True,
                    "financial_data": True
                }
            }
            response = get_page_post(s_req, headers=self.headers, json=d_params)
            if not response:
                logger.info(f"Something wrong!")
                break
            d_postings = json.loads(response.text)

            if 'result' in d_postings.keys() and d_postings['result']:

                for order_raw in d_postings['result']:
                    order = dict_flatten(order_raw, separator=None)
                    works_data = pd.json_normalize(order,
                                                   max_level=0,
                                                   sep='_'
                                                   )
                    products_data = pd.json_normalize(order_raw,
                                                      record_path=['products'],
                                                      meta=['order_id'],
                                                      )
                    temp = list_dict_flatten(order_raw['financial_data']['products'], parent_key='fd')
                    fd_products_data = pd.json_normalize(temp)
                    products_merge = pd.merge(products_data, fd_products_data, how='left',
                                              left_on='sku', right_on='fd_product_id')
                    all_merge = pd.merge(products_merge, works_data, how='left',
                                         left_on='order_id', right_on='order_id')
                    temp = all_merge.to_dict('records')
                    postings.extend(temp)
                    offset += 1

            else:
                break
        posting_df = pd.DataFrame(postings)
        return posting_df


