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

                def report_exists(period_type, date):
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
                    return (
                        round(co2, 10), round(ch4, 10), round(n2o, 10),
                        round(co2 / count, 10), round(ch4 / count, 10), round(n2o / count, 10)
                    )

                def get_sensor_data(period_clause, values):
                    cursor.execute(f"""
                        SELECT AVG(wind_speed), AVG(wind_direction), AVG(pm25), AVG(pm10)
                        FROM sensor_entries WHERE {period_clause}
                    """, values)
                    return cursor.fetchone()

                def get_sensor_id(date):
                    cursor.execute("""
                        SELECT id FROM sensor_entries
                        WHERE DATE(inserted_at) = %s
                        ORDER BY inserted_at ASC LIMIT 1
                    """, (date,))
                    row = cursor.fetchone()
                    return row[0] if row else None

                def insert_or_update_report(period_type, date, report_name, emissions, sensor_data, sensor_id):
                    if not sensor_data:
                        return

                    t_co2, t_ch4, t_n2o, a_co2, a_ch4, a_n2o = emissions
                    w, dir, p25, p10 = sensor_data

                    if report_exists(period_type, date):
                        cursor.execute("""
                            UPDATE reports SET
                                report_name = %s, total_co2 = %s, total_ch4 = %s, total_n2o = %s,
                                avg_co2 = %s, avg_ch4 = %s, avg_n2o = %s, avg_wind_speed = %s,
                                avg_wind_dir = %s, avg_pm25 = %s, avg_pm10 = %s,
                                komentar = %s, updated_at = %s
                            WHERE period_type = %s AND period_date = %s
                        """, (
                            report_name, t_co2, t_ch4, t_n2o, a_co2, a_ch4, a_n2o, w, dir, p25, p10,
                            "Auto-updated", now, period_type, date
                        ))
                        logging.info(f"[Report] {period_type.title()} {report_name} updated.")
                    else:
                        cursor.execute("""
                            INSERT INTO reports
                            (period_type, period_date, category_code, report_name, total_co2, total_ch4, total_n2o,
                             avg_co2, avg_ch4, avg_n2o, avg_wind_speed, avg_wind_dir, avg_pm25, avg_pm10,
                             komentar, perusahaan_id, sumber_emisi_id, sensor_id, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            period_type, date, "1A1", report_name, t_co2, t_ch4, t_n2o,
                            a_co2, a_ch4, a_n2o, w, dir, p25, p10,
                            "Auto-generated", 1, 1, sensor_id or 1, now, now
                        ))
                        logging.info(f"[Report] {period_type.title()} {report_name} inserted.")

                # Harian
                for i in range(30):
                    d = today - timedelta(days=i)
                    emissions = get_emission("DATE(period) = %s", (d,))
                    if not emissions:
                        continue
                    sensor_id = get_sensor_id(d)
                    sensor_data = get_sensor_data("DATE(inserted_at) = %s", (d,))
                    report_name = d.strftime("GRK_%Y_%m_%d")
                    insert_or_update_report("harian", d, report_name, emissions, sensor_data, sensor_id)

                # Bulanan
                for m in range(12):
                    d = (today.replace(day=1) - timedelta(days=30 * m)).replace(day=1)
                    emissions = get_emission("MONTH(period) = %s AND YEAR(period) = %s", (d.month, d.year))
                    if not emissions:
                        continue
                    sensor_data = get_sensor_data("MONTH(inserted_at) = %s AND YEAR(inserted_at) = %s", (d.month, d.year))
                    report_name = d.strftime("GRK_%Y_%m")
                    insert_or_update_report("bulanan", d, report_name, emissions, sensor_data, sensor_id=1)

                # Tahunan
                for y in range(3):
                    d = today.replace(month=1, day=1) - timedelta(days=365 * y)
                    emissions = get_emission("YEAR(period) = %s", (d.year,))
                    if not emissions:
                        continue
                    sensor_data = get_sensor_data("YEAR(inserted_at) = %s", (d.year,))
                    report_name = d.strftime("GRK_%Y")
                    insert_or_update_report("tahunan", d, report_name, emissions, sensor_data, sensor_id=1)

                db.commit()
                cursor.close()
                db.close()

            except Exception as e:
                logging.error(f"[ReportGenerator] Error: {e}")

            logging.info("[ReportGenerator] Waiting for 24 hours before next run...")
            time.sleep(86400)
