from database_manager.database_manager import DatabaseManager
from database_manager.genre import Genre
from database_manager.movie import Movie
from imdb_data_importer.imdb_data_importer import ImdbDataImporter

if __name__ == "__main__":
    database_manager = DatabaseManager('root', 'ana123')
    
    if database_manager.connection:
        database_manager.reset_database()
        database_manager.create_tables()

        # imdb_data_importer = ImdbDataImporter()

        # print(imdb_data_importer.movies.head())
        # print(imdb_data_importer.ratings.head())
        # print(imdb_data_importer.directors_and_writers.head())

        genre = Genre("terror")
        database_manager.insert_into_genres_table(genre)

        movie = Movie("ana", 1999, "linda", None)
        database_manager.insert_into_movies_table(movie)

        database_manager.close_connection()
    else: 
        raise Exception('Unable to connect to database')