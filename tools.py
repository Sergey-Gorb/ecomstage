import datetime
import functools
import urllib.parse

# from django.shortcuts import redirect
#
# from wb.models import ApiKey
#


def list_dict_flatten(in_list, list_out=None, keys_aliases=None, parent_key=None, separator='_'):
    if list_out is None:
        list_out = []

    for d_list in in_list:
        list_out.append(dict_flatten(d_list, keys_aliases=keys_aliases,
                                     parent_key=parent_key, separator=separator))

    return list_out


def dict_flatten(in_dict, dict_out=None, keys_aliases=None, parent_key=None, separator="_"):
    if dict_out is None:
        dict_out = {}

    for k, v in in_dict.items():
        if keys_aliases and k in keys_aliases:
            k = keys_aliases[k]
        k = f"{parent_key}{separator}{k}" if separator and parent_key else k
        if isinstance(v, dict):
            #   dict_flatten(in_dict=v, dict_out=dict_out, parent_key=k if parent_key else None, separator=separator)
            dict_flatten(in_dict=v, dict_out=dict_out, keys_aliases=keys_aliases, parent_key=k, separator=separator)
        elif isinstance(v, list):
            if len(v) == 0:
                dict_out[k] = '[]'
            else:
                dict_out[k] = ','.join(str(x) for x in v)
        elif v is None:
            continue
        else:
            dict_out[k] = v

    return dict_out


def change_dict_key(dict_in, key_old, key_new):
    dict_in[key_new] = dict_in[key_old]
    dict_in.pop(key_old)


def get_date(week=None, days=None, only_date=False):

    date = datetime.datetime.today()
    if days:
        date = date - datetime.timedelta(days=days)
    elif week:
        date = date - datetime.timedelta(weeks=week)
        # date = date - datetime.timedelta(days=(date.weekday()))
    res = date.strftime("%Y-%m-%d") if only_date else date.strftime("%Y-%m-%dT00:00:00.000+03:00")
    return res


# def api_key_required(func):
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         if ApiKey.objects.filter(user=args[0].user.id).exists():
#             return func(*args, **kwargs)
#         else:
#             return redirect("api")
#
#     return wrapper