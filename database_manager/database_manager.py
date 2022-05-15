from multiprocessing import connection
import mysql.connector
from mysql.connector import Error
import pandas as pd
from entities.cast import Cast
from entities.genre import Genre

from entities.movie import Movie
from entities.movie_genre import MovieGenre
from entities.person import Person

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
                Genres varchar(250),
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
                MovieId int(11) NOT NULL,
                GenreId int(11) NOT NULL,
                FOREIGN KEY(MovieId) REFERENCES Movie(MovieId),
                FOREIGN KEY(GenreId) REFERENCES Genre(GenreId),
                PRIMARY KEY CLUSTERED (MovieId, GenreId))""")
                
    def create_person_table(self):
        self.execute_query('Create Person Table', 
            """CREATE TABLE Person ( 
                PersonId int(11) NOT NULL AUTO_INCREMENT,
                PersonImdbId varchar(250) NOT NULL,
                Name varchar(250) NOT NULL,
                BirthYear int(11) NOT NULL,
                PRIMARY KEY (PersonId))""")

    def create_cast_table(self):
        self.execute_query('Create Cast Table', 
            """CREATE TABLE Cast (                 
                CastId int(11) NOT NULL,
                PersonImdbId varchar(250) NOT NULL,
                MovieImdbId varchar(250) NOT NULL,
                IsDirector BIT NOT NULL,
                IsWriter BIT NOT NULL,
                PRIMARY KEY (CastId))""")

    def insert_many_into_movie_table(self, movies: list[Movie]):
        values = []
        for movie in movies:
            values.append((movie.movie_imdb_id, movie.type, movie.title, movie.genres, movie.year, movie.average_rating, movie.votes))
        
        query =  "INSERT INTO movies.movie (MovieImdbId, Type, Title, Genres, Year, AverageRating, Votes) VALUES (%s, %s, %s, %s, %s, %s, %s)"
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
        
    def insert_many_into_person_table(self, persons: list[Person]):
        values = []
        for person in persons:
            values.append((person.person_imdb_id, person.name, person.birth_date))
        
        query = "INSERT INTO movies.person (PersonImdbId, Name, BirthYear) VALUES (%s, %s, %s)"
        self.execute_many_commit_query('Insert Many into Person Table', query, values)
        
    def insert_many_into_cast_table(self, casts: list[Cast]):
        values = []
        for cast in casts:
            values.append((cast.person_imdb_id, cast.movie_imdb_id, cast.is_director, cast.is_writer))
        
        query = "INSERT INTO movies.cast (PersonImdbId, MovieImdbId, IsDirector, IsWriter) VALUES (%s, %s, %s, %s)"
        self.execute_many_commit_query('Insert Many into Cast Table', query, values)

    def get_all_movies(self):        
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM movie")
        records = cursor.fetchall()
        return records
        
    def get_all_movies_df(self):        
        df = pd.read_sql('SELECT * FROM movie', con=self.connection, columns=["movie_id", "movie_imdb_id", "type", "title", "year", "genres", "average_rating", "votes"])
        return df

    def get_all_persons(self):        
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM person")
        records = cursor.fetchall()

        return records



    def create_tables(self):
        self.create_genre_table()
        self.create_movie_table()
        self.create_movie_genre_table()
        self.create_person_table()
        self.create_cast_table()
                  
    def drop_table(self, table_name):
        self.execute_query('Drop Table' + table_name, "DROP TABLE IF EXISTS "  + table_name + " ; ")

    def reset_database(self):
        self.drop_table('Movies')
        self.drop_table('Ratings')
        self.drop_table('Genres')
        self.drop_table('MovieGenre')
        self.drop_table('Cast')
        self.drop_table('Person')
        self.drop_table('Movie')
        self.drop_table('Rating')
        self.drop_table('Genre')