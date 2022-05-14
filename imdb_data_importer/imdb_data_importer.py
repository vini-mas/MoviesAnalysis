import pandas as pd
import numpy as np

class ImdbDataImporter:
    
    ratings: pd.DataFrame
    movies: pd.DataFrame
    directors_and_writers: pd.DataFrame
    persons: pd.DataFrame
    genres: list[str] = []

    def __init__(self):
        print("\nConverting Data to Dataframe...")
        self.read_movies_and_ratings()
        self.read_directors_and_writers()
        

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
        
        for genres in self.movies["genres"]:
            if(not pd.isnull(genres)):
                self.genres.extend([genre for genre in genres.split(',') if genre not in self.genres])

        self.movies['genres'] = self.movies['genres'].fillna('')

        # Drop Unused Columns
        dropped_columns = ['isAdult', 'primaryTitle', 'endYear', 'runtimeMinutes']
        self.movies = self.movies.drop(columns=[e for e in dropped_columns])

    def read_directors_and_writers(self):
        self.directors_and_writers = pd.read_csv("imdb_data/directors_writers.tsv", sep='\t', dtype={ 
            'tconst': np.str_, 
            'directors': np.str_, 
            'writers': np.str_, 
        }, na_values="\\N").reset_index(drop=True)
        self.directors_and_writers['directors'].fillna("", inplace=True)        
        self.directors_and_writers['writers'].fillna("", inplace=True)

        personsColumns = { 
            'nconst': np.str_,
            'primaryName': np.str_,
            'birthYear': pd.Int64Dtype(),
            'deathYear': pd.Int64Dtype(),
            'primaryProfession': np.str_,
            'knownForTitles': np.str_,
        }
        self.persons = pd.read_csv("imdb_data/persons.tsv", sep='\t', dtype=personsColumns, na_values="\\N").reset_index(drop=True)        
        self.persons['birthYear'].fillna(0, inplace=True)        
        self.persons.dropna(subset = ["primaryName"], inplace=True)

        dropped_columns = ['deathYear', 'primaryProfession', 'knownForTitles']
        self.persons = self.persons.drop(columns=[e for e in dropped_columns])