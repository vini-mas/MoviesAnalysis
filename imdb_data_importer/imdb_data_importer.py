import pandas as pd
import numpy as np

class ImdbDataImporter:
    
    ratings: pd.DataFrame
    movies: pd.DataFrame
    directors_and_writers: pd.DataFrame
    genres: list[str] = []

    def __init__(self):
        print("\nConverting Data to Dataframe...")
        self.read_movies_and_ratings()
        # self.read_directors_and_writers()
        

    def read_movies_and_ratings(self):
        column_types = { 
            'tconst': np.str_, 
            'averageRating': np.float64, 
            'numVotes': np.float64 
        }
        ratings = pd.read_csv("imdb_data/ratings.tsv", sep='\t', dtype = column_types)

        column_types = { 
            'tconst': np.str_, 
            'titleType': np.str_, 
            'primaryTitle': np.str_, 
            'originalTitle': np.str_, 
            'isAdult': pd.Int64Dtype(), 
            'startYear': pd.Int64Dtype(), 
            'endYear': pd.Int64Dtype(), 
            'runtimeMinutes': np.str_, 
            'genres': np.str_ 
        }
        movies = pd.read_csv("imdb_data/movies.tsv", sep='\t', dtype=column_types, na_values="\\N")

        self.movies = pd.merge(movies, ratings, how='left', on='tconst')
        
        self.movies.dropna(subset = ["originalTitle"], inplace=True)
        self.movies['startYear'] = self.movies['startYear'].fillna(0)
        self.movies['averageRating'] = self.movies['averageRating'].fillna(0)
        self.movies['numVotes'] = self.movies['numVotes'].fillna(0)
        self.movies['titleType'] = self.movies['titleType'].str[:240]
        self.movies['tconst'] = self.movies['tconst'].str[:240]
        self.movies['originalTitle'] = self.movies['originalTitle'].str[:240]

        # Drop Unused Columns
        dropped_columns = ['isAdult', 'primaryTitle', 'endYear', 'runtimeMinutes']
        self.movies = self.movies.drop(columns=[e for e in dropped_columns])

        for genres in self.movies["genres"]:
            if(not pd.isnull(genres)):
                self.genres.extend([genre for genre in genres.split(',') if genre not in self.genres])


    def read_directors_and_writers(self):
        column_types = { 
            'tconst': np.str_, 
            'directors': np.str_, 
            'writers': np.str_, 
        }
        self.directors_and_writers = pd.read_csv("imdb_data/directors_writers.tsv", sep='\t', dtype=column_types, na_values="\\N").reset_index()