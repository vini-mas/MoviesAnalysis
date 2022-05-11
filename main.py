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

def movie_has_votes(votes):
    return str(votes) != "0.0"

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
        ## 500k entries ~40min or more (genre takes a lot of time)
        imdb_data_importer.movies.reset_index(drop=True, inplace=True)

        movies: list[Movie] = []
        bar = initialize_progress_bar(f'Mapping {len(imdb_data_importer.movies.index)} Movies', size=len(imdb_data_importer.movies.index))
        for index, row in imdb_data_importer.movies.iterrows():
            bar.update(index)
            if movie_has_votes(row[6]):
                movies.append(Movie(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        bar.finish()

        steppedMovies = range(0, len(movies), 500)
        bar = initialize_progress_bar(f'Inserting {len(movies)} Movies', size=len(movies))
        for i in steppedMovies:
            if len(movies) > i+500:
                bar.update(i+500)
                database_manager.insert_many_into_movie_table(movies[i:i+500])
            else:
                bar.update(len(movies))
                database_manager.insert_many_into_movie_table(movies[i:len(movies)])
        bar.finish()

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

        database_manager.close_connection()
    else: 
        raise Exception('Unable to connect to database')