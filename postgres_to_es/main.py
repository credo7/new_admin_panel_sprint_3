from postgres_extractor import PostgresExtractor
from data_transform import DataTransform
from elastic_search_loader import ElasticsearchLoader
from utils import repeat_every


@repeat_every(60*5)
def main():
    postgres_extractor = PostgresExtractor()
    data_transform = DataTransform()
    elastic_search_loader = ElasticsearchLoader()

    movies, last_date = postgres_extractor.extract_movies()

    movie_documents = data_transform.transform_to_elastic(movies=movies)

    elastic_search_loader.insert_into_elastic(movie_documents, last_date=last_date)


if __name__ == '__main__':
    main()
