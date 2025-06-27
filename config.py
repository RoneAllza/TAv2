import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_SENSOR = {
        "host": os.getenv("DB_SENSOR_HOST"),
        "user": os.getenv("DB_SENSOR_USER"),
        "password": os.getenv("DB_SENSOR_PASSWORD"),
        "database": os.getenv("DB_SENSOR_NAME"),
        "port": int(os.getenv("DB_SENSOR_PORT")),
    }

    DB_LARAVEL = {
        "host": os.getenv("DB_LARAVEL_HOST"),
        "user": os.getenv("DB_LARAVEL_USER"),
        "password": os.getenv("DB_LARAVEL_PASSWORD"),
        "database": os.getenv("DB_LARAVEL_NAME"),
        "port": int(os.getenv("DB_LARAVEL_PORT")),
    }

    THINGSPEAK_URL = os.getenv("THINGSPEAK_URL")
    THINGSPEAK_API_KEY = os.getenv("THINGSPEAK_API_KEY")
