import datetime
import functools
import urllib.parse

# from django.shortcuts import redirect
#
# from wb.models import ApiKey
#


def dict_flatten(in_dict, dict_out=None, parent_key=None, separator="_"):
    if dict_out is None:
        dict_out = {}

    for k, v in in_dict.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            dict_flatten(in_dict=v, dict_out=dict_out, parent_key=None)
            continue

        dict_out[k] = v

    return dict_out


def get_date(week=None, days=None):

    date = datetime.datetime.today()
    if days:
        date = date - datetime.timedelta(days=days)
    elif week:
        date = date - datetime.timedelta(weeks=week)
        # date = date - datetime.timedelta(days=(date.weekday()))
    return date.strftime("%Y-%m-%dT00:00:00.000+03:00")


# def api_key_required(func):
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         if ApiKey.objects.filter(user=args[0].user.id).exists():
#             return func(*args, **kwargs)
#         else:
#             return redirect("api")
#
#     return wrapper