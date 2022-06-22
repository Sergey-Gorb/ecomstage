import requests
import json
from loguru import logger


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

    def get_orders(self, date_from, date_to, orders=None,
                   statuses=["DELIVERY", "PROCESSING"]):
        limit = 200
        page_token = ''

        def get_page(page_token=None):

            d_orders = {
                "date_from": date_from,
                "date_to": date_to,
                "orders": orders,
                "statuses": statuses,
                "limit": limit
            }
            s_req = f'{self.url}?limit={limit}'
            if page_token:
                s_req +=f'page_token={page_token}'
            return requests.post(s_req, headers=self.headers, json=d_orders)

        orders = []
        while True:
            response = get_page(page_token=page_token)
            if not response.ok:
                logger.info(f"{response.status_code}, {response.text}")
                break

            d_postings = json.loads(response.text)
            if d_postings['status'] == 'ERROR':
                logger.info(f"{d_postings['errors']['code']}, {d_postings['errors']['message']}")
                break
            if 'result' in d_postings.keys():
                orders += d_postings['result']['orders']
            else:
                break

            if 'paging' in d_postings['result'].keys() and 'nextPageToken' in d_postings['result']['paging'].keys():
                page_token = d_postings['result']['paging']['nextPageToken']
            else:
                break

        return orders
