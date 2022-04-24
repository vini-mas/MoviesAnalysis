from database_manager.database_manager import DatabaseManager
from imdb_data_importer.imdb_data_importer import ImdbDataImporter

if __name__ == "__main__":
    database_manager = DatabaseManager('root', 'admin')
    
    if database_manager.connection:
        database_manager.reset_database()
        database_manager.create_tables()
    else: raise Exception('Unable to connect to database')
    
    imdb_data_importer = ImdbDataImporter()

    print(imdb_data_importer.movies.head())
    print(imdb_data_importer.ratings.head())
    print(imdb_data_importer.directors_and_writers.head())