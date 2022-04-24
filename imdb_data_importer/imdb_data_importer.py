import pandas as pd
import numpy as np

class ImdbDataImporter:
    
    ratings: pd.DataFrame
    movies: pd.DataFrame
    directors_and_writers: pd.DataFrame

    def __init__(self):
        self.read_ratings()
        self.read_movies()
        self.read_directors_and_writers()

    def read_ratings(self):
        column_types = { 
            'tconst': np.str_, 
            'averageRating': np.float64, 
            'numVotes': np.float64 
        }
        self.ratings = pd.read_csv("imdb_data/ratings.tsv", sep='\t', dtype = column_types)

    def read_movies(self):
        column_types = { 
            'tconst': np.str_, 
            'titleType': np.str_, 
            'primaryTitle': np.str_, 
            'originalTitle': np.str_, 
            'isAdult': pd.Int64Dtype(), 
            'startYear': np.str_, 
            'endYear': np.str_, 
            'runtimeMinutes': np.str_, 
            'genres': np.str_ 
        }
        self.movies = pd.read_csv("imdb_data/movies.tsv", sep='\t', dtype=column_types, na_values="\\N")

    def read_directors_and_writers(self):
        column_types = { 
            'tconst': np.str_, 
            'directors': np.str_, 
            'writers': np.str_, 
        }
        self.directors_and_writers = pd.read_csv("imdb_data/directors_writers.tsv", sep='\t', dtype=column_types, na_values="\\N")