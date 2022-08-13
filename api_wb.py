import requests
import json
from loguru import logger
from request_page import get_page_get
from tools import dict_flatten
import pandas as pd
from uuid import uuid4
from pprint import pprint

from tools import get_date


class WBpop:
    def __init__(self, apikey: str, token: str):
        self.apikey = apikey
        self.token = token
        self.result_fbo = True
        self.result_fbs = True
        self.headers = {
            'Authorization'
            '': f'{self.token}'
        }
        self.url = 'https://suppliers-stats.wildberries.ru'
        self.url2 = 'https://suppliers-api.wildberries.ru'

    def get_orders_fbo(self, date_start):
        s_req = f'{self.url}/api/v1/supplier/orders'

        params = {
            "dateFrom": date_start,
            "format": 0,
            "key": self.apikey
        }

        d_fields_o = {
            "order_id": "gNumber",
            "created_at": "date",
            "in_process_at": "lastChangeDate",
            "analytics_data_region": "oblast",
            "analytics_data_warehouse": "warehouseName"
        }

        d_fields_p = {
            "sku": "nmId",
            "fd_price": "totalPrice",
            "offer_id": "supplierArticle",
        }
        response = get_page_get(s_req, params=params)
        if not response:
            self.result_fbo = False
            return

        l_orders = []
        l_products = []
        batch = response.json()
        l_batch = json.loads(response.text)
        for order_raw in l_batch:
            # order_uuid = uuid4().hex
            order_id = order_raw['gNumber']
            order = dict_flatten(order_raw, separator=None)
            d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields_o.items()}
            # d_order['order_uuid'] = order_uuid
            l_orders.append(d_order)

            d_products = {f_key: order[f_val] for (f_key, f_val) in d_fields_p.items()}
            d_products['order_id'] = order_id
            l_products.append(d_products)
        orders_df = pd.DataFrame(l_orders)
        products_df = pd.DataFrame(l_products)

        return orders_df, products_df

    def get_orders_fbs(self, date_start):
        s_req = f'{self.url2}/api/v2/orders'
        offset = 200
        skip = 0
        params = {
            "skip": skip,
            "take": offset,
            "date_start": date_start,
        }
        d_fields_o = {
            "order_id": "orderId",
            "created_at": "dateCreated",
            "analytics_data_city": "city",
            "analytics_data_delivery_type": "deliveryType",
            "analytics_data_warehouse_id": "wbWhId",
        }

        d_fields_p = {
            "sku": "chrtId",
            "price": "totalPrice",
        }

        response = get_page_get(s_req, headers=self.headers, params=params)
        if not response:
            self.result_fbs = False
            return
        batch = response.json()
        total = int(batch.get("total"))
        attempt = 0
        l_orders = []
        l_products = []

        while total > 0:
            orders = batch['orders']
            for order_raw in orders:
                skip += 1
                # order_uuid = uuid4().hex
                order_id = order_raw['orderId']
                order = dict_flatten(order_raw, separator=None)
                d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields_o.items()}
                # d_order['order_uuid'] = order_id
                l_orders.append(d_order)

                d_products = {f_key: order[f_val] for (f_key, f_val) in d_fields_p.items()}
                d_products['order_id'] = order_id
                l_products.append(d_products)
            total -= skip
            if total <= 0:
                break
            params = {
                "skip": skip,
                "take": offset,
                "date_start": date_start
            }
            orders = get_page_get(s_req, headers=self.headers, params=params).json()["orders"]

        orders_df = pd.DataFrame(l_orders)
        products_df = pd.DataFrame(l_products)

        return orders_df, products_df


