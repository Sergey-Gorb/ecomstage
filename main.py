from api_ozon import Ozonpop
from api_wb import WBpop
from api_yandex_market import YMpop
from tools import get_date

if __name__ == '__main__':
    oz_client_id = '2663'
    oz_api_key = 'd99fa453-ff22-4335-805f-d69882119203'
    oz_downloader = Ozonpop(oz_client_id, oz_api_key)
    # postings_fbs_list = oz_downloader.get_postings_fbs(get_date(days=30), get_date(days=0))
    # if len(postings_fbs_list):
    #     # to do something with postings_fbs_list
    #     # pass
    #     print(postings_fbs_list)
    # else:
    #     print('Список отправлений пуст!')
    #
    # postings_fbo_list = oz_downloader.get_postings_fbo(get_date(days=30), get_date(days=0))
    # if len(postings_fbs_list):
    #     # to do something with postings_fbo_list
    #     pass
    # else:
    #     print('Список отправлений пуст!')
    #   exit(0)

    wb_api_key = 'cmP8lIjjS5PXupiLG6elL92R1NciWuL9l0vW0KjEclSyRC0pBGq8C8DFO830QmiXdg3kpctMm6F3_4ffxw'
    wb_api_key ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjNlY2ZhMDQ4LTU4NjQtNGM5MC1hMjAyLTJhNjk3MDZhZTQyMCJ9.LT3CiMCHQgPdHINfkZlqNPKv72-1uoznmyu-SMf2e_8"
    wb_downloader = WBpop(wb_api_key)
    wb_orders_list = wb_downloader.get_orders_fbo(get_date(week=4))
    if len(wb_orders_list):
        # to do something with wb_orders_list
        pass
    else:
        print('Список сборочных заданий поставщика пуст!')

    wb_orders_list = wb_downloader.get_orders_fbs(get_date(week=4))
    if len(wb_orders_list):
        # to do something with wb_orders_list
        pass
    else:
        print('Список сборочных заданий поставщика пуст!')

    # ym_client_id = "9b6958dd70d84bff873eac4f7919aed3"
    # ym_token = "AQAAAABbH_tkAAeNgNoHmNN1WEzVuREf5abQVwo"
    # ym_campaign_id = "22885074"
    # ym_downloader = YMpop(ym_campaign_id, ym_client_id, ym_token)
    # ym_orders_list = ym_downloader.get_orders(get_date(week=4))
    # if len(ym_orders_list):
    #     # to do something with ym_orders_list
    #     pass
    # else:
    #     print('Список заказов поставщика пуст!')

