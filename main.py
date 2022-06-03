from api_ozon import Ozonpop

if __name__ == '__main__':
    client_id = '...'
    api_key = '...'
    downloader = Ozonpop(client_id, api_key)
    action_candidates_list = downloader.get_actions_info()
    if len(action_candidates_list):
        # to do something with action_candidates_list
        pass
    else:
        print('Список доступных для акций товаров пуст!')
