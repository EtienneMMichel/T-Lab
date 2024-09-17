import pymysql
from dotenv import load_dotenv
import os

load_dotenv()

IP = os.getenv('IP')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DBNAME')




class Connector():
    def __init__(self, ip, user, password, database) -> None:
        self.__connection = pymysql.connect(host=ip, user=user, password=password, database=database)
        
    def fetch(self, create_table_query):
        cursor = self.__connection.cursor()
        cursor.execute(create_table_query)
        self.__connection.commit()
        print("Table created successfully")
        
if __name__ == "__main__":
    conn = Connector(ip=IP, user=USER, password=PASSWORD, database=DATABASE)
    create_table_query = """
        CREATE TABLE IF NOT EXISTS collector_instructions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            exchange INTEGER,
            symbol VARCHAR(255),
            rate INTEGER
        );
        """
    res = conn.fetch(create_table_query)