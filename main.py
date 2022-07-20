from api_ozon import Ozonpop
from api_wb import WBpop
from api_yandex_market import YMpop
from tools import get_date
import pandas as pd

if __name__ == '__main__':
        oz_client_id = "133183"
        oz_api_key = "358d11e7-da25-441f-bce8-f3c1d14e5a6d"
        oz_downloader = Ozonpop(oz_client_id, oz_api_key)
        oz_postings_fbs = oz_downloader.get_postings_fbs(get_date(days=1), get_date(days=0))
        if oz_postings_fbs.empty:
            print('OZON FBS Список отправлений пуст!')
        else:
            pass
            # to do something

        oz_postings_fbo = oz_downloader.get_postings_fbo(get_date(days=30), get_date(days=0))
        if oz_postings_fbo.empty:
            print('OZON FBO Список отправлений пуст!')
        else:
            pass
            # to do something

        wb_api_key64 ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjNlY2ZhMDQ4LTU4NjQtNGM5MC1hMjAyLTJhNjk3MDZhZTQyMCJ9.LT3CiMCHQgPdHINfkZlqNPKv72-1uoznmyu-SMf2e_8"
        wb_api_key = "OGY1Yjk5MjAtNjljZi00YTNjLTg4MGYtMzU1NTgyOTU3ZTJh"
        wb_downloader = WBpop(wb_api_key, wb_api_key64)
        wb_orders_fbo = wb_downloader.get_orders_fbo(get_date(week=4))
        if wb_orders_fbo.empty:
            print('WB FBO Список сборочных заданий поставщика пуст!')
        else:
            pass
            # to do something

        wb_orders_fbs = wb_downloader.get_orders_fbs(get_date(week=4))
        if wb_orders_fbs.empty:
            print('WB FBSСписок сборочных заданий поставщика пуст!')
        else:
            pass
            # to do something

        ym_client_id = "9b6958dd70d84bff873eac4f7919aed3"
        ym_token = "AQAAAABbH_tkAAeNgNoHmNN1WEzVuREf5abQVwo"
        ym_campaign_id = "22885074"
        ym_downloader = YMpop(ym_campaign_id, ym_client_id, ym_token)
        ym_orders = ym_downloader.get_orders(get_date(week=12, only_date=True))
        if ym_orders.empty:
            print('Yandex Список заказов поставщика пуст!')
        else:
            pass
            # to do something

