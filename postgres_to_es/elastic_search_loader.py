import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import redis

from config import settings
from utils import backoff


class ElasticsearchLoader:
    es = Elasticsearch(settings.elastic_url)
    index_name = 'movies'
    r = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=0)

    @backoff()
    def insert_into_elastic(self, movie_documents, last_dates):
        if not movie_documents:
            self.update_last_dates_in_redis(last_dates=last_dates)
            return

        def generate_documents(movie_documents):
            for document in movie_documents:
                yield {'_index': self.index_name, '_source': document, '_id': document['id']}

        success, _ = bulk(self.es, generate_documents(movie_documents))

        if success:
            self.update_last_dates_in_redis(last_dates=last_dates)
            logging.info('Documents indexed successfully!')
        else:
            logging.info('Failed to index documents.')

    @backoff()
    def update_last_dates_in_redis(self, last_dates):
        for dic in last_dates:
            for key, value in dic.items():
                if value is not None:
                    self.r.set(str(key), str(value))
