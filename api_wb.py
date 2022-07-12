import requests
import json
from loguru import logger
from request_page import get_page_get
from tools import dict_flatten
from tools import get_date


class WBpop:
    def __init__(self, apikey: str):
        # self.client_id = client_id
        self.apikey = apikey
        # self.headers = {"Client-Id": client_id, "Api-Key": apikey}
        self.url = f'https://suppliers-stats.wildberries.ru'

    def build_headers(self):
        return {
            "Authorization": self.apikey,
            "accept": "application/json",
            "Content-Type": "application/json",
        }

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
            "price": "totalPrice",
            "region": "oblastOkrugName",
            "city": "regionName",
            "warehouseName": "warehouseName"
        }

        response = get_page_get(s_req, self.build_headers(), params)
        if not response:
            return []

        orders = []
        batch = response.json()
        for order_raw in orders:
            order = dict_flatten(order_raw)
            #   print(order)
            d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
            orders.append(d_order)

        logger.info(f"Got orders from marketplace {len(orders)} pcs.")
        return orders

    def get_orders_fbs(self, date_start):
        s_req = f'{self.url}/api/v2/orders'
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
            #   "quantity": 1,
            "price": "totalPrice",
            "city": "city",
            "delivery_type":"deliveryType",
            "warehouse_id": "wbWhId",
        }

        response = get_page_get(s_req, self.build_headers(), params)
        # verifying from file
        # if not response:
        #     with open('wb.json',encoding='utf8') as f:
        #         batch = json.load(f)
        # return []

        batch = response.json()
        total = int(batch.get("total"))
        logger.info(f"Total {total} products")
        print(f"Total {total} products")
        attempt = 0
        l_orders = []
        while total > 0:
            orders = batch['orders']
            for order_raw in orders:
                order = dict_flatten(order_raw)
                #   print(order)
                d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
                # d_order = {
                #             "order_id": order('orderId'),
                #             "order_number": '',
                #              "posting_number": '',
                #             "status": 0,
                #             "cancel_reason_id": 0,
                #             "created_at": order('dateCreated'),
                #             "in_process_at":'',
                #             "sku": order('chrtId'),
                #             "name": '',
                #             "quantity": 1,
                #             "offer_id": '',
                #             "price": order('totalPrice'),
                #             "digital_codes": [],
                #             "region": '',
                #             "city": order('city'),
                #             "delivery_type": order('deliveryType'),
                #             "is_premium": False,
                #             "payment_type_group_name": '',
                #             "warehouse_id": order('wbWhId'),
                #             "warehouse_name": '',
                #             "commission_amount": 0
                #         }
                l_orders.append(d_order)
                skip += 1
            total -= skip
            if total <= 0:
                break
            params = {
                "skip": skip,
                "take": offset,
                "date_start": date_start
            }
            orders = get_page_get((s_req, self.build_headers(), params)).json()["orders"]

            logger.info(f"Got orders from marketplace {len(l_orders)} pcs.")
        return l_orders

