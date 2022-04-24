from database_manager.database_manager import DatabaseManager
from entities.genre import Genre
from entities.movie import Movie
from imdb_data_importer.imdb_data_importer import ImdbDataImporter
import numpy as np
import pandas as pd
import progressbar
import os

def initialize_progress_bar(label: str, size: int):  
    print(f'\n{label}')
    bar = progressbar.ProgressBar(maxval=size, widgets=[
        progressbar.Bar('■', ' [', ']'), ' • ', 
        progressbar.Counter(), f'/{size} - ', progressbar.Percentage(), ' • ', 
        progressbar.AdaptiveETA()],
        term_width=round(os.get_terminal_size().columns*0.8))
    bar.start()

    return bar

if __name__ == "__main__":
    database_manager = DatabaseManager('root', 'admin')
    
    if database_manager.connection:
        database_manager.reset_database()
        database_manager.create_tables()

        imdb_data_importer = ImdbDataImporter()


        # print(imdb_data_importer.movies.head())
        # print(imdb_data_importer.ratings.head())
        # print(imdb_data_importer.directors_and_writers.head())

        added_genres = []

        # Insert Movie
        bar = initialize_progress_bar(f'\nInserting {imdb_data_importer.movies.size} Movies', size=imdb_data_importer.movies.size)
        for index, row in imdb_data_importer.movies.iterrows():  
            bar.update(index)          
            movie = database_manager.insert_into_movie_table(Movie(0, row['originalTitle'], row['startYear'], row["averageRating"], row["numVotes"]))

            try:
                if(not pd.isnull(row['genres'])):
                    new_genres = row['genres'].split(',')

                    # Insert Genre and MovieGenre
                    for index, new_genre in enumerate(new_genres):
                        inserted_genre = [x for x in added_genres if x.name == new_genre]

                        if(len(inserted_genre) > 0):   
                            database_manager.insert_into_movie_genre_table(movie.movie_id, inserted_genre[0].genre_id)
                        else:
                            added_genre = database_manager.insert_into_genre_table(Genre(0, new_genre))
                            added_genres.insert(0, added_genre)
                            database_manager.insert_into_movie_genre_table(movie.movie_id, added_genre.genre_id)
            except:
                raise Exception(row['genres'])
            
            

            




        # genre = Genre("terror")
        # database_manager.insert_into_genres_table(genre)

        # movie = Movie("ana", 1999, "linda", None)
        # database_manager.insert_into_movies_table(movie)

        # rating = Rating(10, 20)
        # database_manager.insert_into_ratings_table(rating)

        database_manager.close_connection()
    else: 
        raise Exception('Unable to connect to database')