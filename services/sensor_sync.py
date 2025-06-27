import time
import logging
from db.connection import get_sensor_db_connection, get_laravel_db_connection

class SensorSyncer:
    def run(self):
        while True:
            logging.info("Running SensorSyncer...")
            try:
                db_sensor = get_sensor_db_connection()
                db_laravel = get_laravel_db_connection()

                cursor_sensor = db_sensor.cursor(dictionary=True)
                cursor_laravel = db_laravel.cursor()

                cursor_sensor.execute("SELECT * FROM sensor_data")
                for row in cursor_sensor.fetchall():
                    cursor_laravel.execute("SELECT COUNT(*) FROM sensor_entries WHERE entry_id = %s", (row["entry_id"],))
                    if cursor_laravel.fetchone()[0] > 0:
                        continue

                    cursor_laravel.execute("""
                        INSERT INTO sensor_entries 
                        (entry_id, inserted_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4, sensor_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row["entry_id"], row["created_at"], row["wind_speed"], row["wind_direction"],
                        row["temperature"], row["humidity"], row["pm25"], row["pm10"],
                        row["co2"], row["ch4"], 1
                    ))
                    logging.info(f"[SensorSyncer] Synced entry_id {row['entry_id']}")

                db_laravel.commit()
                cursor_sensor.close()
                cursor_laravel.close()
                db_sensor.close()
                db_laravel.close()

            except Exception as e:
                logging.error(f"[SensorSyncer] Error: {e}")

            logging.info("[SensorSyncer] Waiting for 1 hour before the next run...")
            # Sleep for 1 hour before the next run
            logging.info("[SensorSyncer] All sensors processed successfully.")

            time.sleep(3600)
