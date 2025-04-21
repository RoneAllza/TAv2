import os
import json
import mysql.connector
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import deque

load_dotenv()

# Koneksi ke database MySQL
db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))
)
cursor = db.cursor()

buffer_data = deque(maxlen=10)
start_time = datetime.now()

# Fungsi untuk mengambil 10 data terakhir dari database berdasarkan entry_id
def get_last_10_data():
    query = """
    SELECT entry_id, sensor_id, ch4_value, co2_value 
    FROM sensor_data 
    ORDER BY entry_id DESC 
    LIMIT 10
    """
    cursor.execute(query)
    return cursor.fetchall()

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker with result code " + str(rc))
    client.subscribe(os.getenv("MQTT_TOPIC"))

def on_message(client, userdata, msg):
    global buffer_data, start_time
    try:
        payload = json.loads(msg.payload.decode())
        print(f"Received: {payload}")
        buffer_data.append(payload)

        current_time = datetime.now()

        if current_time - start_time >= timedelta(hour=1):  # setiap 1 jam, kalo mau testing tinggal diganti ke timedelta(minutes=10)
            if buffer_data:
                sensor_id = buffer_data[-1]["sensor_id"]
                avg_ch4 = round(sum(p["ch4"] for p in buffer_data) / len(buffer_data), 2)
                avg_co2 = round(sum(p["co2"] for p in buffer_data) / len(buffer_data), 2)

                # Generate unique entry_id based on current timestamp
                entry_id = datetime.now().strftime("%Y%m%d%H%M%S")

                # Simpan data ke database
                query = "INSERT INTO sensor_data (entry_id, sensor_id, ch4_value, co2_value) VALUES (%s, %s, %s, %s)"
                cursor.execute(query, (entry_id, sensor_id, avg_ch4, avg_co2))
                db.commit()

                print(f"[{current_time}] Inserted entry {entry_id}: CH4={avg_ch4}, CO2={avg_co2} from {len(buffer_data)} data points")

                # Ambil 10 data terakhir berdasarkan entry_id
                last_10_data = get_last_10_data()
                print("Last 10 data from DB:", last_10_data)

            start_time = current_time

    except Exception as e:
        print(f"Error: {e}")
    except KeyboardInterrupt:
        print("Stopped receiving.")
        client.disconnect()

# Setup MQTT Client
client = mqtt.Client()
client.username_pw_set(os.getenv("MQTT_USER"), os.getenv("MQTT_PASSWORD"))
client.on_connect = on_connect
client.on_message = on_message
client.connect(os.getenv("MQTT_BROKER"), int(os.getenv("MQTT_PORT")), 60)

client.loop_forever()
