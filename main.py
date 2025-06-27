import time
import threading
import logging

from services.sensor_fetcher import SensorFetcher
from services.sensor_sync import SensorSyncer
from services.fugitive_emission import FugitiveEmitter
from services.fuel_combustion import FuelCombustionInserter
from services.report_generator import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("emission_pipeline.log", mode='a'),
        logging.StreamHandler()
    ]
)

def start_threaded(instance):
    thread = threading.Thread(target=instance.run)
    thread.daemon = True
    thread.start()
    return thread

def run():
    threads = [
        start_threaded(SensorFetcher()),
        start_threaded(SensorSyncer()),
        start_threaded(FugitiveEmitter()),
        start_threaded(FuelCombustionInserter()),
        start_threaded(ReportGenerator())
    ]

    while True:
        time.sleep(60)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.info("Stopped by user.")
