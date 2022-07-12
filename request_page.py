import requests
from loguru import logger
import json


def get_page_post(s_req, headers=None, json=None, timeout=3.05):

    try:
        response = requests.post(s_req, headers=headers, json=json, timeout=timeout)
        response.raise_for_status()
    except requests.ConnectionError as e:
        logger.error(
            "Connection Error. Make sure you are connected to Internet.")
        print(str(e))
        return None
    except requests.Timeout as e:
        logger.error("Timeout Error")
        print(str(e))
        return None
    except requests.RequestException as e:
        logger.error("General Error")
        print(str(e))
        return None
    except json.decoder.JSONDecodeError as e:
        logger.info(f"Invalid json: {e}")
        print(str(e))
        return None
    if not response.ok:
        logger.info(f"{response.status_code}, {response.text}")
        return None
    return response


def get_page_get(s_req, headers=None, params=None, timeout=3.05):

    try:
        response = requests.get(s_req, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.ConnectionError as e:
        logger.error(
            "Connection Error. Make sure you are connected to Internet.")
        print(str(e))
        return None
    except requests.Timeout as e:
        logger.error("Timeout Error")
        print(str(e))
        return None
    except requests.RequestException as e:
        logger.error("General Error")
        print(str(e))
        return None
    except json.decoder.JSONDecodeError as e:
        logger.info(f"Invalid json: {e}")
        print(str(e))
        return None
    if not response.ok:
        logger.info(f"{response.status_code}, {response.text}")
        return None
    return response
