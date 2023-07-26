class DataTransform:
    @staticmethod
    def transform_to_elastic(movies):
        if not movies:
            return []

        transformed_movies = [
            {
                'id': str(movie[0]),
                'imdb_rating': movie[1],
                'genre': movie[2],
                'title': movie[3],
                'description': movie[4],
                'director': ' '.join(
                    [person['person_name'] for person in movie[6] if person['person_role'] == 'director']
                ),
                'actors_names': ' '.join(
                    [person['person_name'] for person in movie[6] if person['person_role'] == 'actor']
                ),
                'writers_names': [person['person_name'] for person in movie[6] if person['person_role'] == 'writer'],
                'actors': [
                    {'id': person['person_id'], 'name': person['person_name']}
                    for person in movie[6]
                    if person['person_role'] == 'actor'
                ],
                'writers': [
                    {'id': person['person_id'], 'name': person['person_name']}
                    for person in movie[6]
                    if person['person_role'] == 'writer'
                ],
            }
            for movie in movies
        ]

        return transformed_movies
