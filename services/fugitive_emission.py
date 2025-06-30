import time
import logging
from datetime import datetime, timedelta
from db.connection import get_sensor_db_connection, get_laravel_db_connection

class FugitiveEmitter:
    def run(self):
        while True:
            logging.info("Running FugitiveEmitter...")
            try:
                db_sensor = get_sensor_db_connection()
                db_laravel = get_laravel_db_connection()

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
                    if not result or result[0] is None or result[1] is None:
                        continue

                    avg_ch4, avg_co2 = result
                    ch4_emission_ton = avg_ch4  # sudah ton
                    co2_emission_ton = avg_co2  # sudah ton
                    co2e = ch4_emission_ton + co2_emission_ton

                    cursor_laravel.execute("""
                        INSERT INTO fugitive_emissions
                        (source_name, production_amount, emission_factor, ch4_emission_ton,
                         co2_emission_ton, co2e_emission_ton, period, company_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, ("Sensor CH4", avg_ch4, 1.0, ch4_emission_ton,
                          co2_emission_ton, co2e, target_date, 1))
                    
                    logging.info(f"[FugitiveEmitter] Inserted for {target_date}")

                db_laravel.commit()
                cursor_sensor.close()
                cursor_laravel.close()
                db_sensor.close()
                db_laravel.close()

            except Exception as e:
                logging.error(f"[FugitiveEmitter] Error: {e}")
                
            logging.info("[FugitiveEmitter] Waiting for 24 hours before the next run...")
            time.sleep(86400)
