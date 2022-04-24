from multiprocessing import connection
import mysql.connector
from mysql.connector import Error

class DatabaseManager:
    
    user: str
    password: str
    connection: any

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
            print("Error while connecting to MySQL", e)

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
            print("Failed to run query " + query_name + " in MySQL: {}".format(error))

    def create_movies_table(self): 
        self.execute_query('Create Movies Table',
            """CREATE TABLE Movies ( 
                MovieId int(11) NOT NULL,
                Title varchar(250) NOT NULL,
                Year int(11) NOT NULL,
                Genre varchar(250) NOT NULL,
                RatingId int(11) NOT NULL,
                PRIMARY KEY (MovieId), 
                FOREIGN KEY (RatingId) REFERENCES Ratings(RatingId))""")

    def create_ratings_table(self):
        self.execute_query('Create Ratings Table', 
            """CREATE TABLE Ratings ( 
                RatingId int(11) NOT NULL,
                AverageRating float(11) NOT NULL,
                Votes int(11) NOT NULL,
                PRIMARY KEY (RatingId)) """)

    def create_genres_table(self):
        self.execute_query('Create Genres Table', 
            """CREATE TABLE Genres ( 
                GenreId int(11) NOT NULL,
                Name varchar(250) NOT NULL,
                PRIMARY KEY (GenreId)) """)


    def insert_into_genres_table(self):
        self.execute_query('Insert Genres Table', """INSERT INTO movies.genres (GenreId, Name) VALUES (1, "oi")""")
        #self.execute_query('Insert Genres Table', 'INSERT INTO Genres ('+ genre.id + ', ' + genre.name + ');')
        self.connection.commit()

    def create_tables(self):
        self.create_genres_table()
        self.create_ratings_table()
        self.create_movies_table()
        self.insert_into_genres_table()
                  
    def drop_table(self, table_name):
        self.execute_query('Drop Table' + table_name, "DROP TABLE IF EXISTS "  + table_name + " ; ")

    def reset_database(self):
        self.drop_table('Movies')
        self.drop_table('Ratings')
        self.drop_table('Genres')