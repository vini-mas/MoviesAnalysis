from database_manager.database_manager import DatabaseManager

if __name__ == "__main__":
    database_manager = DatabaseManager('root', 'ana123')
    
    if database_manager.connection:
        database_manager.reset_database()
        database_manager.create_tables()
        database_manager.close_connection()