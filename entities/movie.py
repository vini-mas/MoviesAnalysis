class Movie:
    def __init__(self, movie_id, movie_imdb_id, type, title, year, genres, average_rating, votes):
        self.movie_id = movie_id
        self.movie_imdb_id = movie_imdb_id
        self.type = type
        self.title = title
        self.year = year
        self.genres = genres
        self.average_rating = average_rating
        self.votes = votes