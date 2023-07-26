import time
from functools import wraps
import logging
from enum import Enum
import json

from elasticsearch import Elasticsearch
import requests
import sqlalchemy.exc
import redis

from config import settings


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)
        
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    current_sleep_time = start_sleep_time

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal current_sleep_time
            while current_sleep_time < border_sleep_time:
                try:
                    return func(*args, **kwargs)

                except sqlalchemy.exc.OperationalError as e:
                    error_message = str(e)
                    if 'connection to server at' in error_message and 'failed: Connection refused' in error_message:
                        logging.error('Connection to the database server was refused. Trying to reconnect...')
                    else:
                        logging.error('An OperationalError occurred:', error_message)

                except redis.exceptions.ConnectionError as e:
                    error_message = str(e)
                    if 'Error 61 connecting to' in error_message and 'Connection refused' in error_message:
                        logging.error('Connection to Redis server was refused. Trying to reconnect...')
                    else:
                        logging.error('A ConnectionError occurred:', error_message)

                except Exception as e:
                    logging.error(str(e))

                finally:
                    time.sleep(current_sleep_time)
                    current_sleep_time *= factor

        return inner

    return func_wrapper


def repeat_every(interval):
    def decorator(func):
        def wrapper(*args, **kwargs):
            while True:
                func(*args, **kwargs)
                time.sleep(interval)

        return wrapper

    return decorator


@backoff()
def create_elastic_index_if_not_exist():
    index_name = 'movies'
    index_url = f'{settings.elastic_scheme}://{settings.elastic_host}:{settings.elastic_port}/{index_name}'
    response = requests.head(index_url)

    if response.status_code == 200:
        logging.info(f"The index '{index_name}' exists.")

    elif response.status_code == 404:
        logging.info(f"The index '{index_name}' does not exist. Creating it.")

        es = Elasticsearch(settings.elastic_url)

        with open('elastic_index.json', 'r') as file:
            index_settings = file.read()

        index_settings = json.loads(index_settings)

        response = es.indices.create(index=index_name, body=index_settings)

        if response.get('acknowledged', False):
            logging.info(f"Index '{index_name}' created successfully.")
        else:
            logging.error(f"Failed to create index '{index_name}'. Status code: {response.status_code}")

    else:
        logging.error(f'Unexpected response: {response.status_code}')
