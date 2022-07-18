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
        postings = []
        offset = 0
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
            #   response = get_page(offset_p=offset)
            response = get_page_post(s_req, headers=self.headers, json=d_params)

            d_postings = json.loads(response.text)
            if 'result' in d_postings.keys():
                d_postings = d_postings['result']
                postings += d_postings['postings']
                if d_postings['has_next']:
                    offset += len(d_postings['postings'])
                else:
                    break
        d_o = pd.json_normalize(postings)
        #   print(d_o)
        return postings

    def get_postings_fbo(self, date_from, date_to):
        print('FBO')
        s_req = f'{self.url}/v2/posting/fbo/list'
        limit = 200
        postings = []
        offset = 0
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
            #   response = get_page(offset_p=offset)
            response = get_page_post(s_req, headers=self.headers, json=d_params)
            if not response:
                logger.info(f"Something wrong!")
                break
            d_postings = json.loads(response.text)
            # pprint(d_postings, sort_dicts=False,
            #        #    depth=2
            #        )
            if 'result' in d_postings.keys() and d_postings['result']:
                #   d_j = d_o.to_json()
                #   pprint(d_j, sort_dicts=False, depth=1)
                column1 = ['order_id', 'order_number', 'posting_number','status', 'cancel_reason_id', 'created_at',
                           'in_process_at']
                # works_data.head(3)

                for order_raw in d_postings['result']:
                    # pprint(order, sort_dicts=False)
                    print('------------NEW ORDER')
                    order = dict_flatten(order_raw, separator=None)
                    pprint(order, sort_dicts=False)

                    # works_data = pd.json_normalize(order['analytics_data'],
                    #                                #    record_path=['analytics_data'],
                    #                                meta=column1,
                    #                                #     errors='ignore',
                    #                                #     max_level=1
                    #                                )

                    works_data = pd.json_normalize(order,
                                                   #                              record_path=['products'],
                                                   #                              meta=['order_id', 'order_number'],
                                                   #                              errors='ignore',
                                                   max_level=0,
                                                   sep='_'
                                                   )
                    # works_data = works_data.drop(columns=['products',
                    #                                       #'analytics_data',
                    #                                       #
                    #                                       'financial_data',
                    #                                       'additional_data'])
                    l_col = list(works_data.columns)
                    #  pprint(order['products'], sort_dicts=False)
                    print('----------------------------works_data')
                    pprint(l_col)
                    pprint(works_data.keys())

                    list_products = list_dict_flatten(order_raw['products'], separator='_')
                    print('---------------------------products_data_flatten')
                    pprint(list_products)

                    print('---------------------------products_data')
                    products_data = pd.json_normalize(order_raw,
                                                      record_path=['products'],
                                                      meta=['order_id', 'order_number'],
                                                      #    errors='ignore',
                                                      #    max_level=0
                                                      )
                    l_col = list(products_data.columns)
                    pprint(order_raw['products'], sort_dicts=False)
                    pprint(l_col)
                    pprint(products_data.keys())

                    print('------------------------fd products list dict flatten')
                    # for fd_products in order_raw['financial_data']['products']:
                    #     order_fd_product = dict_flatten(fd_products, parent_key='fd')
                    #     pprint(order_fd_product, sort_dicts=False)
                    temp = list_dict_flatten(order_raw['financial_data']['products'], parent_key='fd')
                    pprint(temp, sort_dicts=False)
                    # fd_products_data = pd.json_normalize(list_dict_flatten(order_raw['financial_data']['products'],
                    #                                                        parent_key='fd'))
                    fd_products_data = pd.json_normalize(temp)
                    #   pprint(fd_products_data, sort_dicts=False)
                    #   order_fd_posting = dict_flatten(order_raw['financial_data']['posting_services'],
                    print('---------------------------products_data marge fd_products')
                    products_merge = pd.merge(products_data, fd_products_data, how='left',
                                              left_on='sku', right_on='fd_product_id')
                    l_col = list(products_merge.columns)
                    # pprint(order['products'], sort_dicts=False)
                    pprint(l_col)
                    pprint(products_merge.keys())



                    #   fd_products_data = pd.json_normalize(order['financial_data']['products'],
                    #   fd_products_data = pd.json_normalize(order['financial_data']['products'],
                    print('------------------------fd posting flatten')
                    order_fd_posting = dict_flatten(order_raw['financial_data']['posting_services'],
                                                    parent_key='fd_posting',
                                                    #   separator=None
                                                    )
                    # order_fd_posting = dict_flatten(order_raw['financial_data'],
                    #                                 parent_key='fd', separator=None)
                    pprint(order_fd_posting, sort_dicts=False)

                    print('---------------------------fd_posting')
                    fd_posting = pd.json_normalize(order_raw['financial_data']['posting_services'],
                                                         sep='_',
                                                         #    record_path=['products'],
                                                         #    meta=['order_id', 'order_number'],
                                                         #    errors='ignore',
                                                         #    max_level=0
                                                         )
                    f_col = list(fd_posting.columns)
                    pprint(order_raw['financial_data']['posting_services'], sort_dicts=False)
                    pprint(f_col)
                    pprint(fd_posting.keys())

                    print('---------------------------fd_products_data')
                    #   fd_products_data = pd.json_normalize(order['financial_data']['products'],
                    #   fd_products_data = pd.json_normalize(order['financial_data']['products'],
                    # fd_products_data = pd.json_normalize(order_raw['financial_data']['products'],
                    #                                      sep='_',
                                                         #    record_path=['products'],
                                                         #    meta=['order_id', 'order_number'],
                                                         #    errors='ignore',
                                                         #    max_level=0
                                                         #)
                    #f_col = list(fd_products_data.columns)
                    #pprint(order_raw['financial_data']['products'], sort_dicts=False)
                    #pprint(f_col)
                    #pprint(fd_products_data.keys())
                    print('---------------------------fd_posting_data')
                    fd_posting_data = pd.json_normalize(order_raw['financial_data']['posting_services'],
                                                       #
                                                       #    record_path=['products'],
                                                       #    meta=['order_id', 'order_number'],
                                                       #    errors='ignore',
                                                       #    max_level=0
                                                       )
                    f_col = list(fd_posting_data.columns)
                    pprint(order_raw['financial_data']['posting_services'], sort_dicts=False)
                    pprint(f_col)
                    pprint(fd_posting_data.keys())
                    #   df_append = df1.append(df2, ignore_index=True)
                    postings += order
                    offset += 1
            else:
                break
        column1 = ["order_id", "order_number", "posting_number", "status", "cancel_reason_id", "created_at",
                   "in_process_at"]

        #   d_o = pd.json_normalize(postings,columns=column1)
        #   d_o = pd.read_json(postings)
        #   print(d_o)

        return postings

    columns = ["order_id", "order_number", "posting_number", "status", "cancel_reason_id", "created_at","in_process_at",
               #  "products": [
               #                "sku", "name", "quantity", "offer_id", "price", "digital_codes",
               #              ]
               #  "analytics_data": {
                                       "region", "city", "delivery_type", "is_premium", "payment_type_group_name",
                                       "warehouse_id", "warehouse_name", "is_legal",
               #                    }
               #   "financial_data": {
               #                        "products": [
               #                                        {
               "commission_amount", "commission_percent", "payout", "product_id", "old_price",
               "price", "total_discount_value", "total_discount_percent", "actions",
               "picking", "quantity", "client_price",
               # "item_services": {
               "marketplace_service_item_fulfillment", "marketplace_service_item_pickup",
               "marketplace_service_item_dropoff_pvz", "marketplace_service_item_dropoff_sc",
               "marketplace_service_item_dropoff_ff", "marketplace_service_item_direct_flow_trans",
               "marketplace_service_item_return_flow_trans", "marketplace_service_item_deliv_to_customer",
               "marketplace_service_item_return_not_deliv_to_customer", "marketplace_service_item_return_part_goods_customer",
               "marketplace_service_item_return_after_deliv_to_customer",
                                #  }
                # }
                            # ],
                # "posting_services": {
               "marketplace_service_item_fulfillment", "marketplace_service_item_pickup",
               "marketplace_service_item_dropoff_pvz", "marketplace_service_item_dropoff_sc",
               "marketplace_service_item_dropoff_ff",  "marketplace_service_item_direct_flow_trans",
               "marketplace_service_item_return_flow_trans", "marketplace_service_item_deliv_to_customer",
               "marketplace_service_item_return_not_deliv_to_customer", "marketplace_service_item_return_part_goods_customer",
               "marketplace_service_item_return_after_deliv_to_customer"]
    # }
    # },
    # "additional_data": []
    # }
    # ]
    #
    # ]
