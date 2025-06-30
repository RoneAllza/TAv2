import time
import logging
from db.connection import get_sensor_db_connection, get_laravel_db_connection

def ppm_to_ton(ppm, molecular_weight, volume_m3=1000):
    """
    Mengonversi ppm ke ton berdasarkan berat molekul dan volume udara.
    Asumsi: 25°C, 1 atm, konstanta molar volume 24.45 L/mol.
    """
    mg_per_m3 = ppm * (molecular_weight / 24.45)
    kg = mg_per_m3 * volume_m3 / 1_000_000  # dari mg ke kg
    ton = kg / 1000  # dari kg ke ton
    return round(ton, 6)

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

                    # Konversi CO2 dan CH4 dari ppm ke ton
                    co2_ton = ppm_to_ton(row["co2"], 44.01)
                    ch4_ton = ppm_to_ton(row["ch4"], 16.04)

                    cursor_laravel.execute("""
                        INSERT INTO sensor_entries 
                        (entry_id, inserted_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4, sensor_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row["entry_id"], row["created_at"], row["wind_speed"], row["wind_direction"],
                        row["temperature"], row["humidity"], row["pm25"], row["pm10"],
                        co2_ton, ch4_ton, 1
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
            time.sleep(3600)
