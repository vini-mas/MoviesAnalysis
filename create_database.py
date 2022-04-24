import mysql.connector
from mysql.connector import Error

def create_movie_table(connection):
    try:
        create_movie_query = """CREATE TABLE Movie ( 
                                movie_id int(11) NOT NULL,
                                title varchar(250) NOT NULL,
                                year int(11) NOT NULL,
                                genre varchar(250) NOT NULL,
                                rating_id int(11) NOT NULL,
                                PRIMARY KEY (movie_id), 
                                FOREIGN KEY (rating_id) REFERENCES Rating(rating_id))"""
        cursor = connection.cursor()
        result = cursor.execute(create_movie_query)
        print("Movie Table created successfully ")

    except mysql.connector.Error as error:
        print("Failed to create Movie table in MySQL: {}".format(error))


def create_rating_table(connection):
    try:
        create_movie_query = """CREATE TABLE Rating ( 
                                rating_id int(11) NOT NULL,
                                rating int(11) NOT NULL,
                                review varchar(250) NOT NULL,
                                PRIMARY KEY (rating_id)) """
        cursor = connection.cursor()
        result = cursor.execute(create_movie_query)
        print("Rating Table created successfully ")

    except mysql.connector.Error as error:
        print("Failed to create Rating table in MySQL: {}".format(error))


def connect_to_database():
    try:
        connection = mysql.connector.connect(host='localhost',
                                            database='movies',
                                            user='root',
                                            password='ana123')
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

def disconnect_from_database(connection):
    if connection.is_connected():
        cursor = connection.cursor()
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
        

def reset_database(connection):
    try:
        reset_query = """DROP TABLE IF EXISTS Movie; """
        cursor = connection.cursor()
        result = cursor.execute(reset_query)

        reset_query = """DROP TABLE IF EXISTS Rating; """
        cursor = connection.cursor()
        result = cursor.execute(reset_query)
        
        print("Database was reset successfully")

    except mysql.connector.Error as error:
        print("Failed to reset database: {}".format(error))


if __name__ == "__main__":
    connection = connect_to_database()
    
    if connection:
        reset_database(connection)
        create_rating_table(connection)
        create_movie_table(connection)
        disconnect_from_database(connection)