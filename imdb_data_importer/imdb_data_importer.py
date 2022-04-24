import pandas as pd
import numpy as np

class ImdbDataImporter:
    
    ratings: pd.DataFrame
    movies: pd.DataFrame
    directors_and_writers: pd.DataFrame

    def __init__(self):
        self.read_movies_and_ratings()
        # self.read_directors_and_writers()
        

    def read_movies_and_ratings(self):
        column_types = { 
            'tconst': np.str_, 
            'averageRating': np.float64, 
            'numVotes': np.float64 
        }
        ratings = pd.read_csv("imdb_data/ratings.tsv", sep='\t', dtype = column_types).reset_index()

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
        movies = pd.read_csv("imdb_data/movies.tsv", sep='\t', dtype=column_types, na_values="\\N").reset_index()

        self.movies = pd.concat([movies, ratings.reindex(movies.index)], axis=1)
        
        self.movies['originalTitle'] = self.movies['originalTitle'].fillna('unknown')
        self.movies['startYear'] = self.movies['startYear'].fillna(0)
        self.movies['averageRating'] = self.movies['averageRating'].fillna(0)
        self.movies['numVotes'] = self.movies['numVotes'].fillna(0)

    def read_directors_and_writers(self):
        column_types = { 
            'tconst': np.str_, 
            'directors': np.str_, 
            'writers': np.str_, 
        }
        self.directors_and_writers = pd.read_csv("imdb_data/directors_writers.tsv", sep='\t', dtype=column_types, na_values="\\N").reset_index()