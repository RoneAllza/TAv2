import time
import json
import logging
from datetime import datetime, timedelta
from db.connection import get_laravel_db_connection

class FuelCombustionInserter:
    def run(self):
        while True:
            logging.info("Running FuelCombustionInserter...")
            try:
                db = get_laravel_db_connection()
                cursor = db.cursor()

                def data_exists(table, column, date):
                    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE DATE({column}) = %s", (date,))
                    return cursor.fetchone()[0] > 0

                for days_ago in range(0, 30):
                    target_date = datetime.now().date() - timedelta(days=days_ago)
                    if data_exists("fuel_combustion_activities", "period", target_date):
                        continue

                    cursor.execute("SELECT * FROM sumber_emisis")
                    sumber_emisis = cursor.fetchall()

                    for sumber in sumber_emisis:
                        (id_sumber, sumber_name, tipe_sumber, kapasitas_output, durasi_pemakaian,
                         category_code, frekuensi_hari, unit, emission_factors,
                         fuel_properties_id, dokumentasi, *_) = sumber

                        jumlah_konsumsi = (kapasitas_output or 1) * durasi_pemakaian * frekuensi_hari

                        cursor.execute("SELECT conversion_factor, co2_factor, ch4_factor, n2o_factor FROM fuel_properties WHERE id = %s", (fuel_properties_id,))
                        fuel = cursor.fetchone()
                        if not fuel:
                            continue

                        conversion_factor, co2_factor, ch4_factor, n2o_factor = map(float, fuel)
                        energi_TJ = jumlah_konsumsi * conversion_factor

                        if jumlah_konsumsi == 0 or energi_TJ == 0:
                            logging.warning(f"[FuelCombustion] Skip source {id_sumber}, energi = 0")
                            continue

                        ef_dict = {"co2": co2_factor, "ch4": ch4_factor, "n2o": n2o_factor}
                        total_emission = {
                            "co2": round((energi_TJ * co2_factor) / 1000, 16),
                            "ch4": round((energi_TJ * ch4_factor) / 1000, 16),
                            "n2o": round((energi_TJ * n2o_factor) / 1000, 16)
                        }

                        cursor.execute("""
                            INSERT INTO fuel_combustion_activities
                            (sumber_emisi_id, source_name, fuel_type, quantity_used, unit,
                             conversion_factor, emission_factor, total_emission_ton, period)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            id_sumber, sumber_name, tipe_sumber, jumlah_konsumsi, unit,
                            conversion_factor, json.dumps(ef_dict), json.dumps(total_emission), target_date
                        ))
                        logging.info(f"[FuelCombustion] Inserted for {target_date} - sumber {id_sumber}")

                db.commit()
                cursor.close()
                db.close()

            except Exception as e:
                logging.error(f"[FuelCombustion] Error: {e}")

            logging.info("[FuelCombustion] All sources processed successfully.")
            # Sleep for 24 hours before the next run
            logging.info("[FuelCombustion] Waiting for 24 hours before the next run...")

            time.sleep(86400)
