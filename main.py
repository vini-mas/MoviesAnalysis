from database_manager.database_manager import DatabaseManager
from entities.genre import Genre
from entities.movie import Movie
from entities.movie_genre import MovieGenre
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

        added_genres: list[Genre] = []
        bar = initialize_progress_bar(f'Inserting {len(imdb_data_importer.genres)} Genres', size=len(imdb_data_importer.genres))
        for index, genre in enumerate(imdb_data_importer.genres):
            bar.update(index)        
            added_genres.append(database_manager.insert_into_genre_table(Genre(0, genre)))            
        bar.finish()

        # Insert Movie
        ## 500k entries ~40min (genre takes a lot of time)
        movies: list[Movie] = []
        bar = initialize_progress_bar(f'Mapping {len(imdb_data_importer.movies)} Movies', size=len(imdb_data_importer.movies))
        for index, row in imdb_data_importer.movies.iterrows():  
            bar.update(index)
            movies.append(Movie(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        bar.finish()

        print("\nInserting Movies...")
        database_manager.insert_many_into_movie_table(movies)

        print("\nSelecting Inserted Movies...")
        inserted_movies = database_manager.get_all_movies()

        movie_genres: list[MovieGenre] = []
        bar = initialize_progress_bar(f'Mapping Genres from {len(inserted_movies)} Movies', size=len(inserted_movies))
        for index, movie_tuple in enumerate(inserted_movies): 
            bar.update(index)
            genres_string = next((x for x in movies if x.movie_imdb_id == movie_tuple[1]), []).genres
            
            if(not pd.isnull(genres_string)):
                genres_string_list = genres_string.split(',')  
                genres = [x for x in added_genres if x.name in genres_string_list]

                for genre in genres:
                    movie_genres.append(MovieGenre(movie_tuple[0], genre.genre_id))
        bar.finish()
        
        print("\nInserting MovieGenres...")
        database_manager.insert_many_into_movie_genre_table(movie_genres)

        print("Done.")
        # genre = Genre("terror")
        # database_manager.insert_into_genres_table(genre)

        # movie = Movie("ana", 1999, "linda", None)
        # database_manager.insert_into_movies_table(movie)

        # rating = Rating(10, 20)
        # database_manager.insert_into_ratings_table(rating)

        database_manager.close_connection()
    else: 
        raise Exception('Unable to connect to database')