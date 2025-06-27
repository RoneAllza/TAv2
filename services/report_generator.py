import time
import json
import logging
from datetime import datetime, timedelta
from db.connection import get_laravel_db_connection

class ReportGenerator:
    def run(self):
        while True:
            logging.info("Running ReportGenerator...")
            try:
                db = get_laravel_db_connection()
                cursor = db.cursor()

                today = datetime.now().date()
                now = datetime.now()

                def data_exists(period_type, date):
                    cursor.execute("""
                        SELECT COUNT(*) FROM reports
                        WHERE period_type = %s AND period_date = %s
                    """, (period_type, date))
                    return cursor.fetchone()[0] > 0

                def get_emission(period_clause, values):
                    query = f"""
                        SELECT total_emission_ton FROM fuel_combustion_activities
                        WHERE {period_clause}
                    """
                    cursor.execute(query, values)

                    co2 = ch4 = n2o = count = 0
                    for (js,) in cursor.fetchall():
                        if js:
                            try:
                                data = json.loads(js)
                                co2 += float(data.get("co2", 0))
                                ch4 += float(data.get("ch4", 0))
                                n2o += float(data.get("n2o", 0))
                                count += 1
                            except Exception as e:
                                logging.warning(f"[Report] JSON decode error: {e}")
                    if count == 0:
                        return None
                    return (round(co2, 10), round(ch4, 10), round(n2o, 10),
                            round(co2 / count, 10), round(ch4 / count, 10), round(n2o / count, 10))

                # Harian
                for i in range(30):
                    d = today - timedelta(days=i)
                    if data_exists("harian", d):
                        continue

                    emissions = get_emission("DATE(period) = %s", (d,))
                    if not emissions:
                        continue
                    t_co2, t_ch4, t_n2o, a_co2, a_ch4, a_n2o = emissions

                    cursor.execute("""
                        SELECT id FROM sensor_entries WHERE DATE(inserted_at) = %s
                        ORDER BY inserted_at ASC LIMIT 1
                    """, (d,))
                    row = cursor.fetchone()
                    if not row:
                        logging.warning(f"[Report] No sensor data {d}")
                        continue
                    sensor_id = row[0]

                    cursor.execute("""
                        SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                        FROM sensor_entries WHERE DATE(inserted_at) = %s
                    """, (d,))
                    w, dir, p25, p10 = cursor.fetchone()

                    report_name = d.strftime("GRK_%Y_%m_%d")
                    cursor.execute("""
                        INSERT INTO reports
                        (period_type, period_date, category_code, report_name, total_co2, total_ch4, total_n2o,
                         avg_co2, avg_ch4, avg_n2o, avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10,
                         komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, ("harian", d, "1A1", report_name, t_co2, t_ch4, t_n2o,
                          a_co2, a_ch4, a_n2o, w, dir, p25, p10,
                          "Auto-generated", 1, 1, sensor_id, now, now))
                    logging.info(f"[Report] Harian {report_name} inserted.")

                                # Bulanan (selalu perbarui ulang)
                for m in range(12):
                    date = (today.replace(day=1) - timedelta(days=m * 30)).replace(day=1)

                    # Hapus report lama
                    cursor.execute("""
                        DELETE FROM reports WHERE period_type = 'bulanan' AND period_date = %s
                    """, (date,))

                    emissions = get_emission("MONTH(period) = %s AND YEAR(period) = %s", (date.month, date.year))
                    if not emissions:
                        continue
                    t_co2, t_ch4, t_n2o, a_co2, a_ch4, a_n2o = emissions

                    cursor.execute("""
                        SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                        FROM sensor_entries WHERE MONTH(inserted_at) = %s AND YEAR(inserted_at) = %s
                    """, (date.month, date.year))
                    w, dir, p25, p10 = cursor.fetchone()

                    report_name = date.strftime("GRK_%Y_%m")
                    cursor.execute("""
                        INSERT INTO reports
                        (period_type, period_date, category_code, report_name, total_co2, total_ch4, total_n2o,
                         avg_co2, avg_ch4, avg_n2o, avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10,
                         komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, ("bulanan", date, "1A1", report_name, t_co2, t_ch4, t_n2o,
                          a_co2, a_ch4, a_n2o, w, dir, p25, p10,
                          "Auto-regenerated", 1, 1, 1, now, now))
                    logging.info(f"[Report] Bulanan {report_name} updated.")

                # Tahunan (selalu perbarui ulang)
                for y in range(3):
                    date = today.replace(month=1, day=1) - timedelta(days=365 * y)
                    year = date.year

                    # Hapus report lama
                    cursor.execute("""
                        DELETE FROM reports WHERE period_type = 'tahunan' AND period_date = %s
                    """, (date,))

                    emissions = get_emission("YEAR(period) = %s", (year,))
                    if not emissions:
                        continue
                    t_co2, t_ch4, t_n2o, a_co2, a_ch4, a_n2o = emissions

                    cursor.execute("""
                        SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                        FROM sensor_entries WHERE YEAR(inserted_at) = %s
                    """, (year,))
                    w, dir, p25, p10 = cursor.fetchone()

                    report_name = date.strftime("GRK_%Y")
                    cursor.execute("""
                        INSERT INTO reports
                        (period_type, period_date, category_code, report_name, total_co2, total_ch4, total_n2o,
                         avg_co2, avg_ch4, avg_n2o, avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10,
                         komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, ("tahunan", date, "1A1", report_name, t_co2, t_ch4, t_n2o,
                          a_co2, a_ch4, a_n2o, w, dir, p25, p10,
                          "Auto-regenerated", 1, 1, 1, now, now))
                    logging.info(f"[Report] Tahunan {report_name} updated.")
                logging.info("[Report] All reports generated successfully.")

                db.commit()
                cursor.close()
                db.close()

            except Exception as e:
                logging.error(f"[ReportGenerator] Error: {e}")

            logging.info("[ReportGenerator] Waiting for 24 hours before the next run...")
            # Sleep for 24 hours before the next run
            logging.info("[ReportGenerator] All reports processed successfully.")

            time.sleep(86400)
