import mysql.connector

from src.utils.logging_config import logger
from resources.dev.config import *

def get_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info("log")
        if connection.is_connected():
            logger.info("Connected to MySQL database")

        return connection

    except mysql.connector.Error as e:
        print("Error while connecting to MySQL:", e)
        return None