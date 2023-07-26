from typing import List, Tuple, Dict, Optional

from sqlalchemy import create_engine, text
import redis

from config import settings
from utils import backoff


class PostgresExtractor:
    def __init__(self):
        self.engine = create_engine(settings.database_url)
        self.r = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=0)

    @backoff()
    def extract_movies(self, table_names: List[str], batch_size: int = 100) -> Tuple[List, Dict]:
        last_dates = []
        unique_movies_ids = set()

        for table_name in table_names:
            if batch_size < 1:
                break
            ids, last_date = self._get_modified_ids_from(table_name, batch_size)
            last_dates.append(last_date)

            if table_name == 'film_work':
                unique_movies_ids.update(ids)
            elif ids:
                movies_ids_tuples = self._execute_sql_file(
                    'sql_files/movies_ids_by_modified_ids.sql',
                    table_name,
                    ids if len(ids) != 1 else f"('{ids[0]}')",
                    batch_size,
                )
                movies_ids = [tuple[0] for tuple in movies_ids_tuples]
                batch_size -= len(movies_ids)
                unique_movies_ids.update(movies_ids)

        if not movies_ids:
            return [], {}

        movies = self._execute_sql_file(
            'sql_files/movies_by_ids.sql',
            tuple(unique_movies_ids) if len(unique_movies_ids) != 1 else f"('{list(unique_movies_ids)[0]}')",
        )
        return movies, last_dates

    def _get_modified_ids_from(self, table_name: str, batch_size: int = 100) -> Tuple[List, Optional[Dict]]:
        redis_date_key = f'{table_name}_date'
        date = self._get_last_date(redis_key=redis_date_key)
        ids_with_modified = self._execute_sql_file('sql_files/modified_ids.sql', table_name, date, batch_size)
        ids, dates = zip(*ids_with_modified) if ids_with_modified else ([], None)
        last_date = str(dates[-1]) if dates else None
        return ids, {redis_date_key: last_date}

    def _get_last_date(self, redis_key) -> str:
        date = self.r.get(redis_key)
        return f"'{date.decode('utf-8') if date else '01-01-0001'}'"

    def _execute_sql_file(self, file_path: str, *args) -> List[Tuple]:
        with open(file_path, 'r') as file:
            query = file.read()

        with self.engine.connect() as conn:
            result = conn.execute(text(query.format(*args))).all()

        return result
