import requests
import json
from loguru import logger
from request_page import get_page_post
from tools import dict_flatten
from pprint import pprint

class YMpop:
    def __init__(self, campaign_id: str, client_id: str, token: str):
        self.client_id = client_id
        self.token = token
        self.headers = {
            'Authorization': f'OAuth oauth_token="{token}", oauth_client_id="{client_id}"'
        }
        self.url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id}/stats/orders.json'

    def get_orders(self, date_from):
        limit = 10
        page_token = ''

        d_orders = {
            "date_from": date_from,
            "limit": limit
        }

        d_fields = {
                  "order_id": 'id',
                  "status": 'status',
 
                  "created_at": 'creationDate',
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
            print(f'Page Token {page_token}')
            s_req = f'{self.url}'
            if page_token:

                s_req += f'page_token={page_token}'

            response = get_page_post(s_req, headers=self.headers, json=d_orders, timeout=10)
            if not response:
                break

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
                    print('NEW ORDER')
                    pprint(order_raw, sort_dicts=False)
                    order = dict_flatten(order_raw)
                    pprint(order, sort_dicts=False)
                    d_order = dict()
                    #   d_order = {f_key: order[f_val] for (f_key, f_val) in d_fields.items()}
                    for item in order['items']:
                        d_order['order_id'] = order.get('id', '')
                        d_order['status'] = order.get('status', 0)
                        d_order['created_at'] = order.get('creationDate', '')
                        d_order['in_process_at'] = order.get('statusUpdateDate', '')
                        d_order['sku"'] = item.get('marketSku', '')
                        d_order['name'] = item.get('offerName', '')
                        d_order['quantity'] = item.get('count', 0)
                        d_order['offer_id'] = item.get('shopSku', 0)
                        for price in item['prices']:
                            if price['type'] == 'BUYER':
                                d_order['fd_price'] = price.get('costPerItem', 0)
                            if price['type'] == 'MARKETPLACE':
                                d_order['fd_total_discount_value'] = price.get('costPerItem', 0)
                        d_order['city'] = order.get('name', '')
                        d_order['payment_type_group_name'] = order.get('paymentType', '')
                        d_order['warehouse_id'] = order.get('deliveryRegion_id', 0)
                        d_order['warehouse_name'] = order.get('deliveryRegion_name', '')
                        actual = 0
                    # for commission in order['commissions']:
                    #     actual += commission.get('actual', 0)
                    # d_order['fd_commission_amount'] = actual
                    d_order['fd_commission_amount'] = sum(commission.get('actual', 0)
                                                          for commission in order['commissions'])

                    print('NEW ORDER ITEM')
                    pprint(d_order, sort_dicts=False)
                    l_orders.append(d_order)
            else:
                break

            if 'paging' in d_postings['result'].keys() and 'nextPageToken' in d_postings['result']['paging'].keys():
                page_token = d_postings['result']['paging']['nextPageToken']
            else:
                break

        return l_orders
