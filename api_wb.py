import requests
import json
from loguru import logger

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

    def get_orders(self, date_start):
        s_req = f'{self.url}/api/v2/orders'
        offset = 200

        def get_page(skip=0):
            get_params = {
                "skip": skip,
                "take": offset,
                "date_start": date_start,
            }
            return requests.get(s_req, get_params, headers=self.build_headers())

        response = get_page()
        if not response.ok:
            logger.info(f"{response.status_code}, {response.text}")
            return []

        orders = []
        batch = response.json()
        total = int(batch.get("total"))
        logger.info(f"Total {total} products")
        attempt = 1

        orders += batch["orders"]
        while total > offset * attempt:
            orders += get_page(offset * attempt).json()["orders"]
            attempt += 1
        logger.info(f"Got orders from marketplace {len(orders)} pcs.")
        return orders

