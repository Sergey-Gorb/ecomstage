import requests
import json
from loguru import logger
from request_page import get_page_get
from tools import dict_flatten

class YMpop:
    def __init__(self, campaign_id: str, client_id: str, token: str):
        self.client_id = client_id
        self.token = token
        self.headers = {
            "Authorization":
                {
                    "OAuth oauth_token": token,
                    "oauth_client_id": client_id,
                }
            }
        self.url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders.json'

    def get_orders(self, date_from):
        limit = 200
        page_token = ''

        d_orders = {
            "date_from": date_from,
            "limit": limit
        }

        d_fields = {
                  "order_id": 'id',
                  "status": 'status',
                  "created_at": 'createdDate',
                  "in_process_at": 'statusUpdateDate',
                  "sku": 'marketSku',
                  "name": 'offerName',
                  "quantity": 'count',
                  "offer_id": 'shopSku',
                  "price": 'costPerItem',
                  "city": 'name',
                  "payment_type_group_name": 'source',
                  "warehouse_id": 'id',
                  "warehouse_name": 'name',
                  "commission_amount": 'actual'
        }

        l_orders = []
        while True:
            s_req = f'{self.url}?limit={limit}'
            if page_token:
                s_req += f'page_token={page_token}'

            response = get_page_get(s_req, headers=self.headers, params=d_orders)

            if not response.ok:
                logger.info(f"{response.status_code}, {response.text}")
                break

            d_postings = json.loads(response.text)
            if d_postings['status'] == 'ERROR':
                logger.info(f"{d_postings['errors']['code']}, {d_postings['errors']['message']}")
                break
            if 'result' in d_postings.keys():
                orders = d_postings['result']['orders']
                for order_raw in orders:
                    order = dict_flatten(order_raw)
                    d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
                    l_orders.append(d_order)
            else:
                break

            if 'paging' in d_postings['result'].keys() and 'nextPageToken' in d_postings['result']['paging'].keys():
                page_token = d_postings['result']['paging']['nextPageToken']
            else:
                break

        return l_orders
