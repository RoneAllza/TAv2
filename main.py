import os
import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()

# Koneksi database
db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))
)
cursor = db.cursor()

# URL API ThingSpeak
url = os.getenv("THINGSPEAK_URL")
API_KEY = os.getenv("THINGSPEAK_API_KEY") # Ganti dengan API Key Anda
LIMIT = 5  # ambil 5 data terakhir untuk contoh

params = {
    "api_key": API_KEY,
    "results": LIMIT
}

response = requests.get(url, params=params)
data = response.json()

def fetch_and_store():
    response = requests.get(url, params=params)
    data = response.json()
    for feed in data.get("feeds", []):
        try:
            entry_id = int(feed["entry_id"])
            created_at = datetime.strptime(feed["created_at"], "%Y-%m-%dT%H:%M:%SZ")

            wind_speed = float(feed["field1"]) if feed.get("field1") else None
            wind_dir = float(feed["field2"]) if feed.get("field2") else None
            temp = float(feed["field3"]) if feed.get("field3") else None
            humid = float(feed["field4"]) if feed.get("field4") else None
            pm25 = float(feed["field5"]) if feed.get("field5") else None
            pm10 = float(feed["field6"]) if feed.get("field6") else None
            co2 = float(feed["field7"]) if feed.get("field7") else None
            ch4 = float(feed["field8"]) if feed.get("field8") else None

            # Hitung jumlah field yang None (null)
            fields = [wind_speed, wind_dir, temp, humid, pm25, pm10, co2, ch4]
            null_count = sum(1 for f in fields if f is None)

            # Jika lebih dari 4 field null, skip insert
            if null_count > 4:
                print(f"Data entry_id {entry_id} dilewati karena terlalu banyak data null ({null_count}/8).")
                continue

            query = """
                INSERT INTO sensor_data 
                (entry_id, created_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (entry_id, created_at, wind_speed, wind_dir, temp, humid, pm25, pm10, co2, ch4))
            print(f"Data entry_id {entry_id} inserted.")
        
        except Exception as e:
            print(f"Error inserting entry_id {feed.get('entry_id')}: {e}")

    db.commit()

try:
    while True:
        fetch_and_store()
        print("Waiting for 1 hour before next fetch...")
        time.sleep(3600)  # 1 hour
except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    cursor.close()
    db.close()
