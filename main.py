from pyclbr import Function

from numpy import void
from database_manager.database_manager import DatabaseManager
from entities.cast import Cast
from entities.genre import Genre
from entities.movie import Movie
from entities.movie_genre import MovieGenre
from entities.person import Person
from imdb_data_importer.imdb_data_importer import ImdbDataImporter
import progressbar
import os
from typing import Callable, TypeVar

T = TypeVar('T')

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

    def steppedInsertion(insetionList: list[T], insertion: Callable[[list[T]], void], name: str):
        steppedLists = range(0, len(insetionList), 500)
        bar = initialize_progress_bar(f'Inserting {len(insetionList)} {name}', size=len(insetionList))
        for i in steppedLists:
            if len(insetionList) > i+500:
                bar.update(i+500)
                insertion(insetionList[i:i+500])
            else:
                bar.update(len(insetionList))
                insertion(insetionList[i:len(insetionList)])
        bar.finish()
    
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

        steppedInsertion(movies, database_manager.insert_many_into_movie_table, 'Movies')          

        print("\nSelecting Inserted Movies...")
        inserted_movies = database_manager.get_all_movies()

        movie_genre_list: list[MovieGenre] = []
        bar = initialize_progress_bar(f'Mapping MovieGenres from {len(added_genres)} Genres', size=len(added_genres))

        for index, added_genre in enumerate(added_genres):
            bar.update(index)
            genre_movies = [MovieGenre(x[0], added_genre.genre_id) for x in inserted_movies if added_genre.name in x[4]]
            movie_genre_list.extend(genre_movies)
        bar.finish()
        
        steppedInsertion(movie_genre_list, database_manager.insert_many_into_movie_genre_table, 'MovieGenres')

        bar = initialize_progress_bar(f'Mapping {len(imdb_data_importer.persons.index)} Persons', size=len(imdb_data_importer.persons.index))        
        persons: list[Person] = []
        for index, row in imdb_data_importer.persons.iterrows():
            bar.update(index)
            persons.append(Person(0, row[0], row[1], row[2]))
        bar.finish()
        
        steppedInsertion(persons, database_manager.insert_many_into_person_table, 'Persons')        
        
        print("\nSelecting Inserted Persons...")
        inserted_persons = database_manager.get_all_persons()

        casts: list[Cast] = []
        
        # i_moviesdf = database_manager.get_all_movies_df().rename(columns={'MovieImdbId':'tconst'})
        # merged_dir_wri = pd.merge(imdb_data_importer.directors_and_writers, i_moviesdf, on='tconst', how='left').reset_index(drop=True)        
        # merged_dir_wri.drop(columns=[e for e in ['Type', 'Title', 'Genres', 'Year', 'AverageRating', 'Votes', 'tconst']], inplace=True)

        bar = initialize_progress_bar(f'Mapping Cast from {len(imdb_data_importer.directors_and_writers.index)} Movies', size=len(imdb_data_importer.directors_and_writers.index)) 
        for index, row in imdb_data_importer.directors_and_writers.iterrows():
            bar.update(index)
            directors: list[str] = row['directors'].split(',')
            writers: list[str] = row['writers'].split(',')

            movie_casts: list[Cast] = []
            for director in directors:
                if director in writers:
                    writers.remove(director)
                    movie_casts.append(Cast(0, director, row['tconst'], True, True))
                else:                    
                    movie_casts.append(Cast(0, director, row['tconst'], True, False))
            
            for writer in writers:
                movie_casts.append(Cast(0, writer, row['tconst'], False, True))            
            casts.extend(movie_casts)
        bar.finish()
        
        steppedInsertion(casts, database_manager.insert_many_into_cast_table, 'Casts')
        
        database_manager.close_connection()
    else: 
        raise Exception('Unable to connect to database')

    
