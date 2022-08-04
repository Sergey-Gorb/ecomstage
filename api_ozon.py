import requests
import json
from pprint import pprint
from loguru import logger
import pandas as pd
from request_page import get_page_post
from pandas.io.json import json_normalize
from tools import dict_flatten, list_dict_flatten, change_dict_key
from uuid import uuid4


class Ozonpop:
    def __init__(self, client_id: str, apikey: str):
        self.client_id = client_id
        self.apikey = apikey
        self.headers = {"Client-Id": client_id, "Api-Key": apikey}
        self.url = 'https://api-seller.ozon.ru'
        self.result_fbs = True
        self.result_fbo = True

    def get_postings_fbs(self, date_from, date_to):
        s_req = f'{self.url}/v3/posting/fbs/list'
        limit = 200
        l_postings = []
        l_products = []
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
        l_products = []
        l_postings = []
        while True:
            d_params['offset'] = offset
            response = get_page_post(s_req, headers=self.headers, json=d_params)
            if not response:
                self.result = False
                return

            d_postings = json.loads(response.text)
            if 'result' in d_postings.keys() and d_postings['result']:
                postings = d_postings['result']['postings']
                for posting_raw in postings:
                    offset += 1
                    order_uuid = uuid4().hex
                    products_raw = posting_raw.pop('products', dict())
                    products_data = pd.json_normalize(products_raw)

                    temp = posting_raw['financial_data'].pop('products', dict())
                    temp = list_dict_flatten(temp, parent_key='fd')
                    fd_products_data = pd.json_normalize(temp)
                    products_merge = pd.merge(products_data, fd_products_data, how='left',
                                              left_on='sku', right_on='fd_product_id')
                    products_merge.insert(0, 'order_uuid', order_uuid, allow_duplicates=False)
                    l_products.extend(products_merge.to_dict('records'))

                    posting = dict_flatten(posting_raw, keys_aliases={'financial_data': 'fd'})
                    posting['order_uuid'] = order_uuid
                    l_postings.append(posting)
                if d_postings['result']['has_next']:
                    pass
                else:
                    break
            else:
                break
        products_df = pd.DataFrame(l_products)
        posting_df = pd.DataFrame(l_postings)
        return posting_df, products_df

    def get_postings_fbo(self, date_from, date_to):
        s_req = f'{self.url}/v2/posting/fbo/list'
        limit = 200
        l_postings = []
        l_products = []
        offset = 0
        # posting_df = pd.DataFrame()
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
                self.result = False
                return
            d_postings = json.loads(response.text)

            if 'result' in d_postings.keys() and d_postings['result']:

                for order_raw in d_postings['result']:

                    offset += 1
                    order_uuid = uuid4().hex
                    products_raw = order_raw.pop('products', dict())
                    products_data = pd.json_normalize(products_raw)

                    temp = order_raw['financial_data'].pop('products', dict())
                    temp = list_dict_flatten(temp, parent_key='fd')

                    fd_products_data = pd.json_normalize(temp)
                    products_merge = pd.merge(products_data, fd_products_data, how='left',
                                              left_on='sku', right_on='fd_product_id')
                    products_merge.insert(0, 'order_uuid', order_uuid, allow_duplicates=False)
                    l_products.extend(products_merge.to_dict('records'))

                    posting = dict_flatten(order_raw, keys_aliases={'financial_data': 'fd'})
                    change_dict_key(posting, 'cancel_reason_id', 'cancellation_cancel_reason_id')
                    change_dict_key(posting, 'analytics_data_warehouse_name', 'analytics_data_warehouse')
                    posting['order_uuid'] = order_uuid
                    l_postings.append(posting)

            else:
                break

        posting_df = pd.DataFrame(l_postings)
        products_df = pd.DataFrame(l_products)
        return posting_df, products_df


