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
    def insert_into_elastic(self, movie_documents, last_date):
        def generate_documents(movie_documents):
            for document in movie_documents:
                yield {'_index': self.index_name, '_source': document}

        success, _ = bulk(self.es, generate_documents(movie_documents))

        if success:
            self.update_redis_last_date(last_date=last_date)
            print('Documents indexed successfully!')
        else:
            print('Failed to index documents.')

    @backoff()
    def update_redis_last_date(self, last_date):
        print(f'last_date is {last_date}')
        self.r.set("date", str(last_date))
