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

        imdb_data_importer = ImdbDataImporter(limit_movie_rows=500000)

        added_genres: list[Genre] = []
        bar = initialize_progress_bar(f'Inserting {len(imdb_data_importer.genres)} Genres', size=len(imdb_data_importer.genres))
        for index, genre in enumerate(imdb_data_importer.genres):
            bar.update(index)        
            added_genres.append(database_manager.insert_into_genre_table(Genre(0, genre)))            
        bar.finish()

        # Insert Movie
        ## 500k entries ~40min or more (genre takes a lot of time)
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

        movie_genre_list: list[MovieGenre] = []
        bar = initialize_progress_bar(f'Mapping MovieGenres from {len(added_genres)} Genres', size=len(added_genres))

        for index, added_genre in enumerate(added_genres):
            bar.update(index)
            genre_movies = [MovieGenre(x[0], added_genre.genre_id) for x in inserted_movies if added_genre.name in x[4]]
            movie_genre_list.extend(genre_movies)
        bar.finish()
        
        print("\nInserting MovieGenres...")
        database_manager.insert_many_into_movie_genre_table(movie_genre_list)

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