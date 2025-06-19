import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time
import threading
import calendar
import json

load_dotenv()

# Koneksi ke DB Sensor
db_sensor = mysql.connector.connect(
    host=os.getenv("DB_SENSOR_HOST"),
    user=os.getenv("DB_SENSOR_USER"),
    password=os.getenv("DB_SENSOR_PASSWORD"),
    database=os.getenv("DB_SENSOR_NAME"),
    port=int(os.getenv("DB_SENSOR_PORT"))
)
cursor_sensor = db_sensor.cursor()

# Koneksi ke DB Laravel
db_laravel = mysql.connector.connect(
    host=os.getenv("DB_LARAVEL_HOST"),
    user=os.getenv("DB_LARAVEL_USER"),
    password=os.getenv("DB_LARAVEL_PASSWORD"),
    database=os.getenv("DB_LARAVEL_NAME"),
    port=int(os.getenv("DB_LARAVEL_PORT"))
)
cursor_laravel = db_laravel.cursor()

# Fungsi ambil dan simpan data sensor
def fetch_and_store_sensor():
    url = os.getenv("THINGSPEAK_URL")
    params = {
        "api_key": os.getenv("THINGSPEAK_API_KEY"),
        "results": 100
    }

    response = requests.get(url, params=params)
    data = response.json()

    for feed in data.get("feeds", []):
        try:
            entry_id = int(feed["entry_id"])
            cursor_sensor.execute("SELECT COUNT(*) FROM sensor_data WHERE entry_id = %s", (entry_id,))
            if cursor_sensor.fetchone()[0] > 0:
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

            query = """
                INSERT INTO sensor_data 
                (entry_id, created_at, wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor_sensor.execute(query, (entry_id, created_at, wind_speed, wind_dir, temp, humid, pm25, pm10, co2, ch4))
            print(f"[Sensor] Data entry_id {entry_id} inserted.")
        except Exception as e:
            print(f"[Sensor] Error: {e}")

    db_sensor.commit()

def data_exists(table, column, date):
    cursor_laravel.execute(f"SELECT COUNT(*) FROM {table} WHERE DATE({column}) = %s", (date,))
    return cursor_laravel.fetchone()[0] > 0

def insert_fugitive_emission():
    print("Running insert_fugitive_emission()...")
    try:
        for days_ago in range(0, 30):
            target_date = datetime.now().date() - timedelta(days=days_ago)
            if data_exists("fugitive_emissions", "period", target_date):
                continue

            query = """
            SELECT AVG(ch4), AVG(co2)
            FROM sensor_entries
            WHERE DATE(inserted_at) = %s
            """
            cursor_laravel.execute(query, (target_date,))
            result = cursor_laravel.fetchone()

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
            print(f"[FugitiveEmission] Inserted for {target_date}")

        db_laravel.commit()
    except Exception as e:
        print(f"[FugitiveEmission] Error: {e}")

def insert_fuel_combustion():
    print("Running insert_fuel_combustion()...")
    try:
        for days_ago in range(0, 30):
            target_date = datetime.now().date() - timedelta(days=days_ago)
            if data_exists("fuel_combustion_activities", "period", target_date):
                continue

            cursor_laravel.execute("SELECT * FROM sumber_emisis")
            sumber_emisis = cursor_laravel.fetchall()

            for sumber in sumber_emisis:
                id, sumber_name, tipe_sumber, kapasitas_output, durasi_pemakaian, category_code, frekuensi_hari, unit, emission_factors, fuel_properties_id, dokumentasi, *_ = sumber
                quantity_used = (kapasitas_output or 1) * durasi_pemakaian * frekuensi_hari

                try:
                    if isinstance(emission_factors, str):
                        emission_factors = json.loads(emission_factors)
                    if isinstance(emission_factors, str):
                        emission_factors = json.loads(emission_factors)

                    ef = []
                    if isinstance(emission_factors, dict):
                        ef = [float(v) for v in emission_factors.values()]
                    elif isinstance(emission_factors, list):
                        for item in emission_factors:
                            if isinstance(item, dict) and 'value' in item:
                                ef.append(float(item['value']))
                            elif isinstance(item, (int, float, str)):
                                ef.append(float(item))
                    else:
                        raise ValueError("Unknown JSON structure")

                except Exception as e:
                    print(f"[FuelCombustion] JSON parse error for sumber {id} ({sumber_name}): {e}. Raw value: {emission_factors}")
                    continue

                total_emission = [e * quantity_used for e in ef]

                cursor_laravel.execute("""
                    INSERT INTO fuel_combustion_activities
                    (sumber_emisi_id, source_name, fuel_type, quantity_used, unit, conversion_factor, emission_factor, total_emission_ton, period)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (id, sumber_name, tipe_sumber, quantity_used, unit, 1.0, json.dumps(ef), json.dumps(total_emission), target_date))
                print(f"[FuelCombustion] Inserted for {target_date} - sumber {id}")

        db_laravel.commit()
    except Exception as e:
        print(f"[FuelCombustion] Error: {e}")

def insert_report():
    print("Running insert_report()...")
    try:
        for days_ago in range(0, 30):
            target_date = datetime.now().date() - timedelta(days=days_ago)
            if data_exists("reports", "period_date", target_date):
                continue

            cursor_laravel.execute("""
                SELECT SUM(co2_emission_ton), SUM(ch4_emission_ton)
                FROM fugitive_emissions
                WHERE period = %s
            """, (target_date,))
            total_co2, total_ch4 = cursor_laravel.fetchone()

            if total_co2 is None or total_ch4 is None:
                continue

            avg_co2 = total_co2 / 1
            avg_ch4 = total_ch4 / 1

            cursor_laravel.execute("""
                INSERT INTO reports
                (period_type, period_date, category_code, total_co2, total_ch4, avg_co2, avg_ch4, total_n2o, avg_n2o, komentar, perusahaan_id, sumber_emisi_id, sensor_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ("daily", target_date, "1A1", total_co2, total_ch4, avg_co2, avg_ch4, 0.0, 0.0, "Auto-generated", 1, 1, 1))
            print(f"[Report] Inserted for {target_date}")

        db_laravel.commit()
    except Exception as e:
        print(f"[Report] Error: {e}")

def run_sensor_loop():
    while True:
        fetch_and_store_sensor()
        time.sleep(3600)

def run_daily_loop():
    while True:
        insert_fugitive_emission()
        insert_fuel_combustion()
        insert_report()
        time.sleep(86400)

try:
    thread_sensor = threading.Thread(target=run_sensor_loop)
    thread_daily = threading.Thread(target=run_daily_loop)
    thread_sensor.start()
    thread_daily.start()
    thread_sensor.join()
    thread_daily.join()

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    cursor_sensor.close()
    db_sensor.close()
    cursor_laravel.close()
    db_laravel.close()
