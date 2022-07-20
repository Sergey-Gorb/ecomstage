import requests
import json
from loguru import logger
from request_page import get_page_get
from tools import dict_flatten
import pandas as pd
from pprint import pprint

from tools import get_date


class WBpop:
    def __init__(self, apikey: str, token: str):
        self.apikey = apikey
        self.token = token
        self.headers = {
            'Authorization'
            '': f'{self.token}'
        }
        self.url = 'https://suppliers-stats.wildberries.ru'
        self.url2 = 'https://suppliers-api.wildberries.ru'

    # def build_headers(self):
    #     return {
    #         "apiKey": self.apikey,
    #         "accept": "application/json",
    #         "Content-Type": "application/json",
    #     }

    def get_orders_fbo(self, date_start):
        s_req = f'{self.url}/api/v1/supplier/orders'

        params = {
            "dateFrom": date_start,
            "format": 0,
            "key": self.apikey
        }

        d_fields = {
            "order_id": "gNumber",
            "created_at": "Date",
            "sku": "nmId",
            "offer_id": "supplierArticle",
            "fd_price": "totalPrice",
            "region": "oblastOkrugName",
            "city": "regionName",
            "warehouseName": "warehouseName"
        }

        response = get_page_get(s_req, params=params)
        if not response:
            return pd.DataFrame()

        orders = []
        batch = response.json()
        for order_raw in orders:
            order = dict_flatten(order_raw)
            d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
            orders.append(d_order)

        logger.info(f"Got orders from marketplace {len(orders)} pcs.")
        orders_df = pd.DataFrame(orders)
        return orders_df

    def get_orders_fbs(self, date_start):
        s_req = f'{self.url2}/api/v2/orders'
        offset = 200
        skip = 0
        params = {
            "skip": skip,
            "take": offset,
            "date_start": date_start,
        }
        d_fields = {
            'order_id': 'orderId',
            "created_at": "dateCreated",
            "sku": "chrtId",
            "fd_price": "totalPrice",
            "city": "city",
            "delivery_type":"deliveryType",
            "warehouse_id": "wbWhId",
        }

        response = get_page_get(s_req, headers=self.headers, params=params)
        if not response:
            return pd.DataFrame()
        batch = response.json()
        total = int(batch.get("total"))
        logger.info(f"Total {total} products")
        attempt = 0
        l_orders = []
        while total > 0:
            orders = batch['orders']
            for order_raw in orders:
                skip += 1
                order = dict_flatten(order_raw, separator=None)
                d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
                l_orders.append(d_order)
            total -= skip
            if total <= 0:
                break
            params = {
                "skip": skip,
                "take": offset,
                "date_start": date_start
            }
            orders = get_page_get(s_req, headers=self.headers, params=params).json()["orders"]

        logger.info(f"Got orders from marketplace {len(l_orders)} pcs.")
        orders_df = pd.DataFrame(l_orders)

        return orders_df


