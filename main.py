from api_ozon import Ozonpop
from api_wb import WBpop
from api_yandex_market import YMpop
from tools import get_date

if __name__ == '__main__':
    oz_client_id = '...'
    oz_api_key = '...'
    oz_downloader = Ozonpop(oz_client_id, oz_api_key)
    postings_fbs_list = oz_downloader.get_postings_fbs(get_date(week=2), get_date(days=0), "awaiting_deliver")
    if len(postings_fbs_list):
        # to do something with postings_fbs_list
        pass
    else:
        print('Список отправлений пуст!')

    postings_fbo_list = oz_downloader.get_postings_fbo(get_date(week=10), get_date(days=0), "awaiting_deliver")
    if len(postings_fbs_list):
        # to do something with postings_fbo_list
        pass
    else:
        print('Список отправлений пуст!')

    wb_api_key = '...'
    wb_downloader = WBpop( wb_api_key)
    wb_orders_list = wb_downloader.get_orders(get_date(week=2))
    if len(wb_orders_list):
        # to do something with wb_orders_list
        pass
    else:
        print('Список сборочных заданий поставщика пуст!')

    ym_campaign_id = '...'
    ym_client_id = '...'
    ym_token = '...'
    ym_downloader = YMpop(ym_campaign_id, ym_client_id, ym_token)
    ym_orders_list = YMpop.get_orders(get_date(week=4), get_date(days=0))
    if len(ym_orders_list):
        # to do something with ym_orders_list
        pass
    else:
        print('Список заказов поставщика пуст!')

