from postgres_extractor import PostgresExtractor
from data_transform import DataTransform
from elastic_search_loader import ElasticsearchLoader
from utils import repeat_every, backoff
from config import settings
from utils import create_elastic_index_if_not_exist


TABLE_NAMES = ['film_work', 'person', 'genre']


@repeat_every(settings.repeat_time_seconds)
def elastic_index_updater():
    postgres_extractor = PostgresExtractor()
    data_transform = DataTransform()
    elastic_search_loader = ElasticsearchLoader()

    movies, last_dates = postgres_extractor.extract_movies(table_names=TABLE_NAMES, batch_size=1000)

    movie_documents = data_transform.transform_to_elastic(movies=movies)

    elastic_search_loader.insert_into_elastic(movie_documents, last_dates=last_dates)


@backoff()
def main():
    create_elastic_index_if_not_exist()
    elastic_index_updater()


if __name__ == '__main__':
    main()
