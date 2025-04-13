# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from dotenv import load_dotenv
import os
import requests
import logging
from model import CondoMesage
import json
import time
import logging
import schedule
from exception import AuthenticationException, ServerErrorException
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)
redis_client = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

condo_service_url = str(os.getenv("CONDO_SERVICE_URL"))
user_service_url = str(os.getenv("USER_SERVICE_URL"))
notification_service_url = str(os.getenv("NOTIFICATION_SERVICE_URL"))

notification_interval_second = os.getenv("NOTIFICATION_INTERVAL_SECOND")
user_tier = os.getenv("USER_TIER")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
range_condo_created_min = os.getenv("RANGE_CONDO_CREATED_MIN") or 60

headers = {"ClientId": client_id, "ClientSecret": client_secret}


def get_user_by_tier():
    try:
        response = requests.get(user_service_url + '/user/list?tier=' + user_tier, headers=headers)
        if response.status_code == 401:
            logger.error("401 Error from User Service: get_user_by_tier " + str(response))
            raise AuthenticationException
        elif response.status_code != 200:
            logger.error("500 Error from User Service: get_user_by_tier " + str(response))
            return ServerErrorException
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("500 Error from User Service: get_user_by_tier " + str(e))
        return ServerErrorException


WEBHOOK_URL = 'https://discordapp.com/api/webhooks/1350265746243977298/ez0SddlwDqN-07cYwiP3VImUFriwjBPotN6dSaiHBPz0YLiOd57i2UpG4h7N4IAVs1Bh'
USER_MONITORING_WEBHOOK_URL = 'https://discordapp.com/api/webhooks/1360641225983787169/7-p7OxN5krx7mej4s4R8IHpR-XiV-RfBAzuqcvOpMNf1qAGmTDDP_UDNNLG4p5qkLbB9'


def send_alert(message: str):
    # Construct the payload
    payload = {
        "content": message
    }

    headers = {
        "Content-Type": "application/json"
    }

    # Send POST request to Discord Webhook URL
    response = requests.post(WEBHOOK_URL, data=json.dumps(payload), headers=headers)

    # Check if the request was successful
    if response.status_code == 204:
        print("Alert sent successfully!")
    else:
        print(f"Failed to send alert: {response.status_code} - {response.text}")


def get_favorite_search(user_id):
    response = requests.get(user_service_url + '/get/favorite_search?user_id=' + str(user_id), headers=headers)
    if response.status_code == 401:
        logger.error("401 Error from User Service: get_favorite_search" + str(response))
        raise AuthenticationException
    elif response.status_code != 200:
        logger.error("500 Error from User Service: get_favorite_search" + str(response))
        return ServerErrorException
    return response.json()


def search_condo(price_search_from, price_search_to, space_search_from, space_search_to,
                 room_search_from, room_search_to, toilet_search_from, toilet_search_to,
                 floor_search_from, floor_search_to, location_search, desc_search, limit=5):
    try:
        url = condo_service_url + '/internal/search/condo?'
        url += 'price_search_from=' + str(price_search_from)
        url += '&price_search_to=' + str(price_search_to)
        url += '&space_search_from=' + str(space_search_from)
        url += '&space_search_to=' + str(space_search_to)
        url += '&room_search_from=' + str(room_search_from)
        url += '&room_search_to=' + str(room_search_to)
        url += '&toilet_search_from=' + str(toilet_search_from)
        url += '&toilet_search_to=' + str(toilet_search_to)
        url += '&floor_search_from=' + str(floor_search_from)
        url += '&floor_search_to=' + str(floor_search_to)
        url += '&limit=' + str(limit)
        if location_search is not None:
            url += '&location_search=' + str(location_search)
        if desc_search is not None:
            url += '&desc_search=' + str(desc_search)
        url += '&created_within_mins=' + str(range_condo_created_min)
        response = requests.get(url, headers=headers)

        logger.info("URL: " + url + " Search condo response: " + str(response))
        if response.status_code == 401:
            logger.error("401 Error from Condo Service: search_condo" + str(response))
            raise AuthenticationException
        elif response.status_code != 200:
            logger.error("500 Error from Condo Service: search_condo" + str(response))
            return ServerErrorException
    except requests.exceptions.RequestException as e:
        logger.error("500 Error from Condo Service: search_condo" + str(e))
        return ServerErrorException
    return response.json()


def send_notification(line_user_id, messages):
    url = notification_service_url + '/line/notification/'
    req_obj = {'messages': [msg.serialize() for msg in messages], 'line_user_id': line_user_id}
    try:
        response = requests.post(url, json=req_obj, headers=headers)
        if response.status_code == 401:
            logger.error("401 Error from Notification Service: send_notification" + str(response))
            raise AuthenticationException
        elif response.status_code != 200:
            logger.error("500 Error from Notification Service: send_notification" + str(response))
            return ServerErrorException
    except Exception as e:
        logger.error("500 Error from Notification Service: send_notification" + str(e))
        return ServerErrorException


def send_user_monitoring(message: str):
    payload = {
        "content": message
    }

    headers = {
        "Content-Type": "application/json"
    }

    # Send POST request to Discord Webhook URL
    response = requests.post(USER_MONITORING_WEBHOOK_URL, data=json.dumps(payload), headers=headers)

    # Check if the request was successful
    if response.status_code == 204:
        print("User monitoring sent successfully!")
    else:
        print(f"Failed to send user monitoring: {response.status_code} - {response.text}")


# Press the green button in the gutter to run the script.
def schedule_notification():
    logger.info('Start schedule notification')
    try:
        user_list = get_user_by_tier()
        for user in user_list:
            # find user favorite condo
            user_id = user['id']

            logger.info('Start getting favorite search for user: ' + str(user_id))
            favorite_search_list = get_favorite_search(user_id)
            if len(favorite_search_list) == 0:
                continue
            for favorite_search in favorite_search_list:
                logger.info('Start search condo for user: ' + str(user_id))
                condo_list = search_condo(favorite_search['price_search_from'], favorite_search['price_search_to'],
                                          favorite_search['space_search_from'], favorite_search['space_search_to'],
                                          favorite_search['room_search_from'], favorite_search['room_search_to'],
                                          favorite_search['toilet_search_from'], favorite_search['toilet_search_to'],
                                          favorite_search['floor_search_from'], favorite_search['floor_search_to'],
                                          favorite_search['location_search'], favorite_search['desc_search'],
                                          favorite_search['limit'])

                try:
                    if len(condo_list) == 0:
                        continue
                except Exception as e:
                    send_alert("Schedule_noti_condo len(condo_list) error: " + str(e) + " user_id : " + str(
                        user_id) + "favorite id:  " + str(favorite_search['id']))
                    logger.error("Error at schedule notification: " + str(e))
                    continue

                messages = []
                line_user_id = user['line_user_id']

                # Validate notification duplication
                notified_list = redis_client.lrange(str(user_id) + '_' + str(favorite_search['id']), 0, -1)

                search_list = [(lambda condo: condo.get('unique_validator'))(condo) for condo in condo_list]

                to_be_notify_unique_list = [x for x in search_list if x not in notified_list]

                condo_list = [x for x in condo_list if x.get('unique_validator') in to_be_notify_unique_list]

                # Todo: send notification to user
                for condo in condo_list:
                    logger.info("Send user notification: " + condo['unique_validator'])
                    image = condo['image_url1']
                    desc = condo['short_desc'][:50]
                    price = condo['price_from']
                    price = f'{price:,}'
                    link = condo['link']
                    logger.info('Send notification to user: ' + str(line_user_id) + ' with condo: ' + str(link))
                    send_user_monitoring('Send user: ' + user['username'] + ' Condo link: ' + + str(link))
                    messages.append(CondoMesage(image, desc, price, link))

                if len(messages) > 0:
                    send_notification(line_user_id, messages)
                if len(to_be_notify_unique_list) > 0:
                    redis_client.lpush(str(user_id) + '_' + str(favorite_search['id']),
                                       *to_be_notify_unique_list)
    except Exception as e:
        send_alert("Schedule_noti_condo Error at schedule notification: " + str(e))
        logger.error("Error at schedule notification: " + str(e))
        return
    logger.info('End schedule notification')


try:
    schedule.every(int(notification_interval_second)).seconds.do(schedule_notification)
    logger.info('Start schedule notification at with interval:"' + notification_interval_second + '" seconds')
except Exception as e:
    send_alert("Schedule_noti_condo Error at schedule notification: " + str(e))
    logger.error("Error at schedule notification: " + str(e))
while True:
    schedule.run_pending()
    time.sleep(1)
