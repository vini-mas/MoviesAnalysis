from multiprocessing import connection
import mysql.connector
from mysql.connector import Error
from entities.genre import Genre

from entities.movie import Movie
from entities.movie_genre import MovieGenre

class DatabaseManager:
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self.connection = self.initialize_connection()

    def initialize_connection(self):
        try:
            connection = mysql.connector.connect(host='localhost', database='movies', user=self.user, password=self.password)

            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()

                #extra things
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)

                cursor.execute('set global max_allowed_packet=67108864')

                return connection

        except Error as e:
            raise Exception("Error while connecting to MySQL", e)

        return None
    
    def close_connection(self):
        if self.connection.is_connected():
            cursor = self.connection.cursor()
            cursor.close()
            self.connection.close()
            print("MySQL connection is closed")

    def execute_query(self, query_name: str, query: str):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            print("Query " + query_name + " executed successfully ")

        except mysql.connector.Error as error:
            raise Exception("Failed to run query " + query_name + " in MySQL: {}".format(error))

    def execute_commit_query(self, query_name: str, query: str, values: tuple):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()
            return cursor.lastrowid
            
        except mysql.connector.Error as error:
            print(query)
            print(values)
            raise Exception("Failed to run commit \nQuery Name: " + query_name + "\nQuery:"+ query + "".join(values) +"\n in MySQL: {}".format(error))
    
    def execute_many_commit_query(self, query_name: str, query: str, values: list[tuple]):
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, values)
            self.connection.commit()
            return cursor.lastrowid
            
        except mysql.connector.Error as error:
            raise Exception("Failed to run commit Query Name: " + query_name + " in MySQL: {}".format(error))

    def create_movie_table(self): 
        self.execute_query('Create Movie Table',
            """CREATE TABLE Movie ( 
                MovieId int(11) NOT NULL AUTO_INCREMENT,
                MovieImdbId varchar(250) NOT NULL,
                Type varchar(250),
                Title varchar(250) NOT NULL,
                Year int(11) NOT NULL,
                AverageRating float(11) NOT NULL,
                Votes int(11) NOT NULL,
                PRIMARY KEY (MovieId))""")

    def create_genre_table(self):
        self.execute_query('Create Genre Table', 
            """CREATE TABLE Genre ( 
                GenreId int(11) NOT NULL AUTO_INCREMENT,
                Name varchar(250) NOT NULL,
                PRIMARY KEY (GenreId))""")
        
    def create_movie_genre_table(self):
        self.execute_query('Create MovieGenre Table', 
            """CREATE TABLE MovieGenre (                 
                MovieId INT NOT NULL,
                GenreId INT NOT NULL,
                FOREIGN KEY(MovieId) REFERENCES Movie(MovieId),
                FOREIGN KEY(GenreId) REFERENCES Genre(GenreId),
                PRIMARY KEY CLUSTERED (MovieId, GenreId))""")

    def insert_many_into_movie_table(self, movies: list[Movie]):
        values = []
        for movie in movies:
            values.append((movie.movie_imdb_id, movie.type, movie.title, movie.year, movie.average_rating, movie.votes))
        
        query =  "INSERT INTO movies.movie (MovieImdbId, Type, Title, Year, AverageRating, Votes) VALUES (%s, %s, %s, %s, %s, %s)"
        self.execute_many_commit_query('Insert Many into Movie Table', query, values)

    def insert_into_genre_table(self, genre: Genre):
        query =  "INSERT INTO movies.genre (Name) VALUES (%s)"
        values = (genre.name, )
        genre.genre_id = self.execute_commit_query('Insert into Genre Table', query, values)
        return genre

    def insert_many_into_movie_genre_table(self, movie_genres: list[MovieGenre]):
        values = []
        for movie_genre in movie_genres:
            values.append((movie_genre.movie_id, movie_genre.genre_id))
        
        query =  "INSERT INTO movies.moviegenre (MovieId, GenreId) VALUES (%s, %s)"
        self.execute_many_commit_query('Insert Many into MovieGenre Table', query, values)

    def get_all_movies(self):        
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM movie")
        records = cursor.fetchall()

        # return tuple (MovieId, MovieImdbId, Title, Year, AverageRating, Votes)
        return records



    def create_tables(self):
        self.create_genre_table()
        self.create_movie_table()
        self.create_movie_genre_table()
                  
    def drop_table(self, table_name):
        self.execute_query('Drop Table' + table_name, "DROP TABLE IF EXISTS "  + table_name + " ; ")

    def reset_database(self):
        self.drop_table('Movies')
        self.drop_table('Ratings')
        self.drop_table('Genres')
        self.drop_table('MovieGenre')
        self.drop_table('Movie')
        self.drop_table('Rating')
        self.drop_table('Genre')