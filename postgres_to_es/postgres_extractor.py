from sqlalchemy import create_engine, text
import redis

from config import settings
from utils import backoff


class PostgresExtractor:
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.r = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=0)

    @backoff()
    def extract_movies(self, batch_size: int = 100):

        with open('query.sql', 'r') as file:
            query = file.read()

        date = self.r.get('date')

        if not date:
            date = "01-01-0001"
            self.r.set('date', date)

        with self.engine.connect() as conn:
            movies = conn.execute(text(query.format("'01-01-0001'", batch_size))).all()

        return movies, movies[-1][5]
