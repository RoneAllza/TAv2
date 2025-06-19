import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import threading
import json
import logging
import ast

load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("emission_pipeline.log", mode='a'),
        logging.StreamHandler()
    ]
)

def fetch_and_store_sensor():
    while True:
        try:
            db_sensor = mysql.connector.connect(
                host=os.getenv("DB_SENSOR_HOST"),
                user=os.getenv("DB_SENSOR_USER"),
                password=os.getenv("DB_SENSOR_PASSWORD"),
                database=os.getenv("DB_SENSOR_NAME"),
                port=int(os.getenv("DB_SENSOR_PORT"))
            )
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
                    wind_speed = float(feed["field1"]) if feed.get("field1") else None
                    wind_dir = float(feed["field2"]) if feed.get("field2") else None
                    temp = float(feed["field3"]) if feed.get("field3") else None
                    humid = float(feed["field4"]) if feed.get("field4") else None
                    pm25 = float(feed["field5"]) if feed.get("field5") else None
                    pm10 = float(feed["field6"]) if feed.get("field6") else None
                    co2 = float(feed["field7"]) if feed.get("field7") else None
                    ch4 = float(feed["field8"]) if feed.get("field8") else None

                    fields = [wind_speed, wind_dir, temp, humid, pm25, pm10, co2, ch4]
                    null_count = sum(1 for f in fields if f is None)
                    if null_count > 4:
                        continue

                    cursor.execute("""
                        INSERT INTO sensor_data 
                        (entry_id, created_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (entry_id, created_at, wind_speed, wind_dir, temp, humid, pm25, pm10, co2, ch4))
                    logging.info(f"[Sensor] Data entry_id {entry_id} inserted.")
                except Exception as e:
                    logging.error(f"[Sensor] Error processing entry_id {feed.get('entry_id')}: {e}")

            db_sensor.commit()
            cursor.close()
            db_sensor.close()

        except Exception as e:
            logging.error(f"[Sensor] Connection or fetching error: {e}")

        time.sleep(3600)

def sync_sensor_data_to_laravel():
    while True:
        logging.info("Running sync_sensor_data_to_laravel()...")
        try:
            db_sensor = mysql.connector.connect(
                host=os.getenv("DB_SENSOR_HOST"),
                user=os.getenv("DB_SENSOR_USER"),
                password=os.getenv("DB_SENSOR_PASSWORD"),
                database=os.getenv("DB_SENSOR_NAME"),
                port=int(os.getenv("DB_SENSOR_PORT"))
            )
            db_laravel = mysql.connector.connect(
                host=os.getenv("DB_LARAVEL_HOST"),
                user=os.getenv("DB_LARAVEL_USER"),
                password=os.getenv("DB_LARAVEL_PASSWORD"),
                database=os.getenv("DB_LARAVEL_NAME"),
                port=int(os.getenv("DB_LARAVEL_PORT"))
            )
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                """, (
                    row["entry_id"], row["created_at"], row["wind_speed"], row["wind_direction"],
                    row["temperature"], row["humidity"], row["pm25"], row["pm10"], row["co2"], row["ch4"]
                ))
                logging.info(f"[Sync] Sensor entry_id {row['entry_id']} synced to Laravel DB.")

            db_laravel.commit()
            cursor_sensor.close()
            cursor_laravel.close()
            db_sensor.close()
            db_laravel.close()

        except Exception as e:
            logging.error(f"[Sync] Error: {e}")

        time.sleep(3600)

def insert_fugitive_emission():
    while True:
        logging.info("Running insert_fugitive_emission()...")
        try:
            db_sensor = mysql.connector.connect(
                host=os.getenv("DB_SENSOR_HOST"),
                user=os.getenv("DB_SENSOR_USER"),
                password=os.getenv("DB_SENSOR_PASSWORD"),
                database=os.getenv("DB_SENSOR_NAME"),
                port=int(os.getenv("DB_SENSOR_PORT"))
            )
            db_laravel = mysql.connector.connect(
                host=os.getenv("DB_LARAVEL_HOST"),
                user=os.getenv("DB_LARAVEL_USER"),
                password=os.getenv("DB_LARAVEL_PASSWORD"),
                database=os.getenv("DB_LARAVEL_NAME"),
                port=int(os.getenv("DB_LARAVEL_PORT"))
            )
            cursor_sensor = db_sensor.cursor()
            cursor_laravel = db_laravel.cursor()

            def data_exists(cursor, table, column, date):
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE DATE({column}) = %s", (date,))
                return cursor.fetchone()[0] > 0

            for days_ago in range(0, 30):
                target_date = datetime.now().date() - timedelta(days=days_ago)
                if data_exists(cursor_laravel, "fugitive_emissions", "period", target_date):
                    continue

                cursor_sensor.execute("""
                    SELECT AVG(ch4), AVG(co2) FROM sensor_data WHERE DATE(created_at) = %s
                """, (target_date,))
                result = cursor_sensor.fetchone()
                if result is None or result[0] is None or result[1] is None:
                    continue

                avg_ch4, avg_co2 = result
                emission_factor = 0.02
                ch4_emission_ton = avg_ch4 * emission_factor
                co2_emission_ton = avg_co2
                co2e_emission_ton = ch4_emission_ton + co2_emission_ton

                cursor_laravel.execute("""
                    INSERT INTO fugitive_emissions
                    (source_name, production_amount, emission_factor, ch4_emission_ton, co2_emission_ton, co2e_emission_ton, period, company_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, ("Sensor CH4", avg_ch4, emission_factor, ch4_emission_ton, co2_emission_ton, co2e_emission_ton, target_date, 1))
                logging.info(f"[FugitiveEmission] Inserted for {target_date}")

            db_laravel.commit()
            cursor_sensor.close()
            cursor_laravel.close()
            db_sensor.close()
            db_laravel.close()

        except Exception as e:
            logging.error(f"[FugitiveEmission] Error: {e}")

        time.sleep(86400)

def insert_report():
    while True:
        logging.info("Running insert_report()...")
        try:
            db_laravel = mysql.connector.connect(
                host=os.getenv("DB_LARAVEL_HOST"),
                user=os.getenv("DB_LARAVEL_USER"),
                password=os.getenv("DB_LARAVEL_PASSWORD"),
                database=os.getenv("DB_LARAVEL_NAME"),
                port=int(os.getenv("DB_LARAVEL_PORT"))
            )
            cursor = db_laravel.cursor()

            def data_exists(cursor, period_type, date):
                cursor.execute("""
                    SELECT COUNT(*) FROM reports
                    WHERE period_type = %s AND period_date = %s
                """, (period_type, date))
                return cursor.fetchone()[0] > 0

            def get_emission_from_json(period_clause, period_values):
                query = f"""
                    SELECT total_emission_ton FROM fuel_combustion_activities
                    WHERE {period_clause}
                """
                cursor.execute(query, period_values)

                co2_total = ch4_total = n2o_total = 0.0
                count = 0
                for (json_str,) in cursor.fetchall():
                    if json_str:
                        try:
                            data = json.loads(json_str)
                            co2_total += float(data.get("co2", 0))
                            ch4_total += float(data.get("ch4", 0))
                            n2o_total += float(data.get("n2o", 0))
                            count += 1
                        except Exception as e:
                            logging.warning(f"[Report] JSON decode error: {e}")
                if count == 0:
                    return None
                return (
                    round(co2_total, 10), round(ch4_total, 10), round(n2o_total, 10),
                    round(co2_total / count, 10), round(ch4_total / count, 10), round(n2o_total / count, 10)
                )

            today = datetime.now().date()
            now = datetime.now()

            # Harian
            for days_ago in range(0, 30):
                day = today - timedelta(days=days_ago)
                if data_exists(cursor, "harian", day):
                    continue

                emissions = get_emission_from_json("DATE(period) = %s", (day,))
                if emissions is None:
                    continue
                total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o = emissions

                cursor.execute("""
                    SELECT id FROM sensor_entries
                    WHERE DATE(inserted_at) = %s
                    ORDER BY inserted_at ASC LIMIT 1
                """, (day,))
                sensor_row = cursor.fetchone()
                if not sensor_row:
                    logging.warning(f"[Report] Skipped {day}: No sensor data found.")
                    continue
                sensor_id = sensor_row[0]

                cursor.execute("""
                    SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                    FROM sensor_entries
                    WHERE DATE(inserted_at) = %s
                """, (day,))
                avg_row = cursor.fetchone()
                avg_wind, avg_dir, avg_pm25, avg_pm10 = avg_row if avg_row else (0, 0, 0, 0)

                cursor.execute("""
                    INSERT INTO reports
                    (period_type, period_date, category_code, total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                     avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10, komentar, perusahaan_id, sumber_emisi_id, sensor_id,
                     created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, ("harian", day, "1A1", total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                      avg_wind, avg_dir, avg_pm25, avg_pm10, "Auto-generated", 1, 1, sensor_id, now, now))
                logging.info(f"[Report] Harian inserted for {day}")

            # Bulanan
            for month_delta in range(0, 12):
                month_date = (today.replace(day=1) - timedelta(days=month_delta * 30)).replace(day=1)
                if data_exists(cursor, "bulanan", month_date):
                    continue

                emissions = get_emission_from_json("MONTH(period) = %s AND YEAR(period) = %s", (month_date.month, month_date.year))
                if emissions is None:
                    continue
                total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o = emissions

                cursor.execute("""
                    SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                    FROM sensor_entries
                    WHERE MONTH(inserted_at) = %s AND YEAR(inserted_at) = %s
                """, (month_date.month, month_date.year))
                avg_row = cursor.fetchone()
                avg_wind, avg_dir, avg_pm25, avg_pm10 = avg_row if avg_row else (0, 0, 0, 0)

                cursor.execute("""
                    INSERT INTO reports
                    (period_type, period_date, category_code, total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                     avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10, komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, ("bulanan", month_date, "1A1", total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                      avg_wind, avg_dir, avg_pm25, avg_pm10, "Auto-generated", 1, 1, 1, now, now))
                logging.info(f"[Report] Bulanan inserted for {month_date:%Y-%m}")

            # Tahunan
            for year_offset in range(0, 3):
                year_date = today.replace(month=1, day=1) - timedelta(days=365 * year_offset)
                if data_exists(cursor, "tahunan", year_date):
                    continue

                emissions = get_emission_from_json("YEAR(period) = %s", (year_date.year,))
                if emissions is None:
                    continue
                total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o = emissions

                cursor.execute("""
                    SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                    FROM sensor_entries
                    WHERE YEAR(inserted_at) = %s
                """, (year_date.year,))
                avg_row = cursor.fetchone()
                avg_wind, avg_dir, avg_pm25, avg_pm10 = avg_row if avg_row else (0, 0, 0, 0)

                cursor.execute("""
                    INSERT INTO reports
                    (period_type, period_date, category_code, total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                     avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10, komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, ("tahunan", year_date, "1A1", total_co2, total_ch4, total_n2o, avg_co2, avg_ch4, avg_n2o,
                      avg_wind, avg_dir, avg_pm25, avg_pm10, "Auto-generated", 1, 1, 1, now, now))
                logging.info(f"[Report] Tahunan inserted for {year_date.year}")

            db_laravel.commit()
            cursor.close()
            db_laravel.close()

        except Exception as e:
            logging.error(f"[Report] Error: {e}")

        time.sleep(86400)

def insert_fuel_combustion():
    while True:
        logging.info("Running insert_fuel_combustion()...")
        try:
            db_laravel = mysql.connector.connect(
                host=os.getenv("DB_LARAVEL_HOST"),
                user=os.getenv("DB_LARAVEL_USER"),
                password=os.getenv("DB_LARAVEL_PASSWORD"),
                database=os.getenv("DB_LARAVEL_NAME"),
                port=int(os.getenv("DB_LARAVEL_PORT"))
            )
            cursor = db_laravel.cursor()

            def data_exists(cursor, table, column, date):
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE DATE({column}) = %s", (date,))
                return cursor.fetchone()[0] > 0

            for days_ago in range(0, 30):
                target_date = datetime.now().date() - timedelta(days=days_ago)
                if data_exists(cursor, "fuel_combustion_activities", "period", target_date):
                    continue

                cursor.execute("SELECT * FROM sumber_emisis")
                sumber_emisis = cursor.fetchall()

                for sumber in sumber_emisis:
                    id, sumber_name, tipe_sumber, kapasitas_output, durasi_pemakaian, category_code, frekuensi_hari, unit, emission_factors, fuel_properties_id, dokumentasi, *_ = sumber
                    jumlah_konsumsi = (kapasitas_output or 1) * durasi_pemakaian * frekuensi_hari

                    cursor.execute("SELECT conversion_factor, co2_factor, ch4_factor, n2o_factor FROM fuel_properties WHERE id = %s", (fuel_properties_id,))
                    fuel = cursor.fetchone()
                    if not fuel:
                        continue

                    conversion_factor = float(fuel[0])
                    co2_factor = float(fuel[1])
                    ch4_factor = float(fuel[2])
                    n2o_factor = float(fuel[3])
                    energi_TJ = jumlah_konsumsi * conversion_factor

                    if jumlah_konsumsi == 0 or energi_TJ == 0:
                        logging.warning(f"[FuelCombustion] Lewati sumber ID {id}, energi = 0")
                        continue

                    ef_dict = {
                        "co2": co2_factor,
                        "ch4": ch4_factor,
                        "n2o": n2o_factor
                    }
                    ef_json = json.dumps(ef_dict)

                    total_emission = {
                        "co2": round((energi_TJ * co2_factor) / 1000, 16),
                        "ch4": round((energi_TJ * ch4_factor) / 1000, 16),
                        "n2o": round((energi_TJ * n2o_factor) / 1000, 16)
                    }

                    cursor.execute("""
                        INSERT INTO fuel_combustion_activities
                        (sumber_emisi_id, source_name, fuel_type, quantity_used, unit, conversion_factor, emission_factor, total_emission_ton, period)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (id, sumber_name, tipe_sumber, jumlah_konsumsi, unit, conversion_factor, ef_json, json.dumps(total_emission), target_date))
                    logging.info(f"[FuelCombustion] Inserted for {target_date} - sumber {id}")

            db_laravel.commit()
            cursor.close()
            db_laravel.close()

        except Exception as e:
            logging.error(f"[FuelCombustion] Error: {e}")

        time.sleep(86400)

def start_threaded(func):
    thread = threading.Thread(target=func)
    thread.daemon = True
    thread.start()
    return thread

def run():
    threads = [
        start_threaded(fetch_and_store_sensor),
        start_threaded(sync_sensor_data_to_laravel),
        start_threaded(insert_fugitive_emission),
        start_threaded(insert_report),
        start_threaded(insert_fuel_combustion)
    ]

    while True:
        time.sleep(60)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.info("Stopped by user.")
