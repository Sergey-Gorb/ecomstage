from api_ozon import Ozonpop
from api_wb import WBpop
from api_yandex_market import YMpop
from tools import get_date
from db_api import dbConnect, insertToTable, createTable, insertMPTable, insertToTable, insertIntoTable, insertDF
import pandas as pd

if __name__ == '__main__':
    dbHost = 'localhost'
    databaseName = 'ecom1pr'
    dbUser = 'sergg'
    dbPwd = 'sergg_1'
    conn, cur = dbConnect(databaseName, dbUser, dbHost, dbPwd)
    # createTable(cur, 'mp_logs')
    # oz_client_id = "2663"
    # oz_api_key = "d99fa453-ff22-4335-805f-d69882119203"
    oz_client_id = "133183"
    oz_api_key = "358d11e7-da25-441f-bce8-f3c1d14e5a6d"
    oz_downloader = Ozonpop(oz_client_id, oz_api_key)
    oz_postings_fbs, oz_products_fbs = oz_downloader.get_postings_fbs(get_date(days=1), get_date(days=0))
    if oz_downloader.result_fbs:
        insertDF(conn, cur, oz_postings_fbs, 'orders')

        insertDF(conn, cur, oz_products_fbs, 'products')

    oz_postings_fbo, oz_products_fbo = oz_downloader.get_postings_fbo(get_date(days=30), get_date(days=0))
    if oz_downloader.result_fbo:
        insertDF(conn, cur, oz_postings_fbo, 'orders')

        insertDF(conn, cur, oz_products_fbo, 'products')

    wb_api_key64 ="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjNlY2ZhMDQ4LTU4NjQtNGM5MC1hMjAyLTJhNjk3MDZhZTQyMCJ9.LT3CiMCHQgPdHINfkZlqNPKv72-1uoznmyu-SMf2e_8"
    wb_api_key = "OGY1Yjk5MjAtNjljZi00YTNjLTg4MGYtMzU1NTgyOTU3ZTJh"
    wb_downloader = WBpop(wb_api_key, wb_api_key64)
    wb_orders_fbo, wb_products_fbo = wb_downloader.get_orders_fbo(get_date(week=4))
    if wb_downloader.result_fbo:
        insertDF(conn, cur, wb_orders_fbo, 'orders')

        insertDF(conn, cur, wb_products_fbo, 'products')

    wb_orders_fbs, wb_products_fbs = wb_downloader.get_orders_fbs(get_date(week=4))
    if wb_downloader.result_fbs:
        insertDF(conn, cur, wb_orders_fbs, 'orders')

        insertDF(conn, cur, wb_products_fbs, 'products')

    ym_client_id = "9b6958dd70d84bff873eac4f7919aed3"
    ym_token = "AQAAAABbH_tkAAeNgNoHmNN1WEzVuREf5abQVwo"
    ym_campaign_id = "22885074"
    ym_downloader = YMpop(ym_campaign_id, ym_client_id, ym_token)
    ym_orders, ym_products = ym_downloader.get_orders(get_date(week=12, only_date=True))
    if ym_orders.empty:
        print('Yandex Список заказов поставщика пуст!')
    else:
        insertDF(conn, cur, ym_orders, 'orders')

        insertDF(conn, cur, ym_products, 'products')
    conn.close

