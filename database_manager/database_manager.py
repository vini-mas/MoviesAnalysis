from multiprocessing import connection
import mysql.connector
from mysql.connector import Error
from entities.genre import Genre

from entities.movie import Movie

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

    def create_movie_table(self): 
        self.execute_query('Create Movie Table',
            """CREATE TABLE Movie ( 
                MovieId int(11) NOT NULL AUTO_INCREMENT,
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

    def insert_into_movie_table(self, movie: Movie):
        query =  "INSERT INTO movies.movie (Title, Year, AverageRating, Votes) VALUES (%s, %s, %s, %s)"
        values = (movie.title, movie.year, movie.average_rating, movie.votes)
        movie.movie_id = self.execute_commit_query('Insert into Movie Table', query, values)
        return movie


    def insert_into_genre_table(self, genre: Genre):
        query =  "INSERT INTO movies.genre (Name) VALUES (%s)"
        values = (genre.name, )
        genre.genre_id = self.execute_commit_query('Insert into Genre Table', query, values)
        return genre

    def insert_into_movie_genre_table(self, movie_id, genre_id):
        query =  "INSERT INTO movies.moviegenre (MovieId, GenreId) VALUES (%s, %s)"
        values = (movie_id, genre_id)
        return self.execute_commit_query('Insert into MovieGenre Table', query, values)

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