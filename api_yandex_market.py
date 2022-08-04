import requests
import json
from loguru import logger
from request_page import get_page_post
from tools import dict_flatten
from pprint import pprint
import pandas as pd
import uuid


class YMpop:
    def __init__(self, campaign_id: str, client_id: str, token: str):
        self.client_id = client_id
        self.token = token
        self.result = True
        self.headers = {
            'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'
        }
        self.url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders.json'

    def get_orders(self, date_from):
        limit = 200
        page_token = ''

        d_orders = {
            "dateFrom": date_from,
            "limit": limit
        }
        l_orders = []
        l_products = []
        while True:
            s_req = f'{self.url}'
            if page_token:
                s_req += f'?page_token={page_token}'

            response = get_page_post(s_req, headers=self.headers, json=d_orders, timeout=10)
            if not response:
                self.result = False
                return
            if not response.ok:
                logger.info(f"{response.status_code}, {response.text}")
                self.result = False
                break

            d_postings = json.loads(response.text)
            if d_postings['status'] == 'ERROR':
                logger.info(f"{d_postings['errors']['code']}, {d_postings['errors']['message']}")
                break
            if 'result' in d_postings.keys():
                orders = d_postings['result']['orders']

                for order_raw in orders:
                    d_order = dict()
                    d_item = dict()
                    uuid4_hex = uuid.uuid4().hex
                    d_order['order_uuid'] = uuid4_hex
                    l_items = order_raw.pop('items', list())
                    for item in l_items:
                        d_item['order_uuid'] = uuid4_hex
                        d_item['sku'] = item.get('marketSku', '')
                        d_item['sku'] = item.get('marketSku', '')
                        d_item['name'] = item.get('offerName', '')
                        d_item['quantity'] = item.get('count', 0)
                        d_item['offer_id'] = item.get('shopSku', 0)
                        d_item['fd_commission_amount'] = sum(commission.get('actual', 0)
                                                             for commission in order_raw['commissions'])
                        for price in item['prices']:
                            if price['type'] == 'BUYER':
                                d_item['fd_price'] = item.get('costPerItem', 0)
                            if price['type'] == 'MARKETPLACE':
                                d_item['fd_total_discount_value'] = item.get('costPerItem', 0)
                    order = dict_flatten(order_raw)
                    d_order['order_id'] = order.get('id', '')
                    d_order['status'] = order.get('status', 0)
                    d_order['created_at'] = order.get('creationDate', '')
                    d_order['in_process_at'] = order.get('statusUpdateDate', '')

                    d_order['analytics_data_city'] = order.get('deliveryRegion_name', '')
                    d_order['analytics_data_payment_type_group_name'] = order.get('paymentType', '')
                    d_order['analytics_data_warehouse_id'] = order.get('deliveryRegion_id', 0)
                    d_order['analytics_data_warehouse'] = order.get('deliveryRegion_name', '')
                    actual = 0
                l_orders.append(d_order)
                l_products.append(d_item)
            else:
                break

            if 'paging' in d_postings['result'].keys() and 'nextPageToken' in d_postings['result']['paging'].keys():
                page_token = d_postings['result']['paging']['nextPageToken']
            else:
                break

        orders_df = pd.DataFrame(l_orders)
        products_df = pd.DataFrame(l_products)
        return orders_df, products_df
