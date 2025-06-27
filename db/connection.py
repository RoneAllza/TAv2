import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def connect_db(config):
    return mysql.connector.connect(**config)

def get_sensor_db_connection():
    config = {
        "host": os.getenv("DB_SENSOR_HOST"),
        "user": os.getenv("DB_SENSOR_USER"),
        "password": os.getenv("DB_SENSOR_PASSWORD"),
        "database": os.getenv("DB_SENSOR_NAME"),
        "port": int(os.getenv("DB_SENSOR_PORT")),
    }
    return connect_db(config)

def get_laravel_db_connection():
    config = {
        "host": os.getenv("DB_LARAVEL_HOST"),
        "user": os.getenv("DB_LARAVEL_USER"),
        "password": os.getenv("DB_LARAVEL_PASSWORD"),
        "database": os.getenv("DB_LARAVEL_NAME"),
        "port": int(os.getenv("DB_LARAVEL_PORT")),
    }
    return connect_db(config)
