import os
import time
import json
import logging
import requests
from datetime import datetime
from db.connection import get_sensor_db_connection

class SensorFetcher:
    def run(self):
        while True:
            try:
                db_sensor = get_sensor_db_connection()
                cursor = db_sensor.cursor()

                url = os.getenv("THINGSPEAK_URL")
                params = {"api_key": os.getenv("THINGSPEAK_API_KEY"), "results": 100}
                response = requests.get(url, params=params)
                data = response.json()

                for feed in data.get("feeds", []):
                    try:
                        entry_id = int(feed["entry_id"])
                        cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE entry_id = %s", (entry_id,))
                        if cursor.fetchone()[0] > 0:
                            continue

                        created_at = datetime.strptime(feed["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                        values = {
                            "wind_speed": float(feed["field1"]) if feed.get("field1") else None,
                            "wind_dir": float(feed["field2"]) if feed.get("field2") else None,
                            "temp": float(feed["field3"]) if feed.get("field3") else None,
                            "humid": float(feed["field4"]) if feed.get("field4") else None,
                            "pm25": float(feed["field5"]) if feed.get("field5") else None,
                            "pm10": float(feed["field6"]) if feed.get("field6") else None,
                            "co2": float(feed["field7"]) if feed.get("field7") else None,
                            "ch4": float(feed["field8"]) if feed.get("field8") else None
                        }

                        if sum(1 for v in values.values() if v is None) > 4:
                            continue

                        cursor.execute("""
                            INSERT INTO sensor_data 
                            (entry_id, created_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            entry_id, created_at, values["wind_speed"], values["wind_dir"], values["temp"],
                            values["humid"], values["pm25"], values["pm10"], values["co2"], values["ch4"]
                        ))
                        logging.info(f"[SensorFetcher] Inserted entry_id {entry_id}")

                    except Exception as e:
                        logging.error(f"[SensorFetcher] Error entry {feed.get('entry_id')}: {e}")

                db_sensor.commit()
                cursor.close()
                db_sensor.close()

            except Exception as e:
                logging.error(f"[SensorFetcher] Connection error: {e}")
            logging.info("[SensorFetcher] Waiting for 1 hour before the next run...")
            # Sleep for 1 hour before the next run
            logging.info("[SensorFetcher] All sensors processed successfully.")
            
            time.sleep(3600)
