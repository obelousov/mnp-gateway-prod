from fastapi import HTTPException
import mysql.connector
from mysql.connector import Error
from config import settings

def get_db_connection():
    """Create and return MySQL database connection"""
    try:
        # connection = mysql.connector.connect(**MYSQL_CONFIG)
        connection = mysql.connector.connect(**settings.mysql_config)
        return connection
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}") from e
