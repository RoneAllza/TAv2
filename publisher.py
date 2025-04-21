import json
import time
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os
import random

load_dotenv()

broker = os.getenv("MQTT_BROKER")
port = int(os.getenv("MQTT_PORT"))
topic = os.getenv("MQTT_TOPIC")
username = os.getenv("MQTT_USER")
password = os.getenv("MQTT_PASSWORD")

client = mqtt.Client()
client.username_pw_set(username, password)
client.connect(broker, port, 60)

try:
    while True:
        payload = {
            "sensor_id": "sensor_gas_01",
            "ch4": round(random.uniform(50.0, 150.0), 2),  # ppm
            "co2": round(random.uniform(400.0, 800.0), 2)  # ppm
        }

        client.publish(topic, json.dumps(payload))
        print(f"Published: {payload}")
        time.sleep(1)  # Kirim setiap detik

except KeyboardInterrupt:
    print("Stopped publishing.")
    client.disconnect()
