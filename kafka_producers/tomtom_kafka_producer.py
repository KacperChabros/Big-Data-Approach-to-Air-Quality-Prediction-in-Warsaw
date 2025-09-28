import requests
import json
import time
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer

API_KEY = "API_KEY"
UNIT = "KMPH"
SLEEP_TIME = 0.1
KAFKA_TOPIC = "TomTom"
KAFKA_SERVERS = ['namenode:9092', 'datanode1:9092']

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

now = datetime.now()
day_str = now.strftime("%Y-%m-%d")
hour_str = now.strftime("%H")
OUTPUT_DIR = Path("/home/hadoop/data") / day_str / hour_str
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
log_file = OUTPUT_DIR / "log.txt"

def log(message):
    with open(log_file, "a", encoding="utf-8") as logf:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logf.write(f"[{timestamp}] {message}\n")

log("Starting Kafka producer download...")
print("Starting Kafka producer download...")

coords_file = Path("/home/hadoop/venvs/open_meteo/src/points.txt")
if not coords_file.exists():
    raise FileNotFoundError("Brak pliku 'points.txt' w folderze.")

with open(coords_file, "r") as f:
    points = [line.strip().split(",") for line in f if line.strip()]
    coords = [(float(lat), float(lon)) for lat, lon in points]

segment_data = []
MAX_ZOOM = 22
MIN_ZOOM = 10

for lat, lon in coords:
    success = False
    for zoom in range(MAX_ZOOM, MIN_ZOOM - 1, -1):
        url = (
            f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/"
            f"{zoom}/json?point={lat},{lon}&unit={UNIT}&key={API_KEY}"
        )
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                data['lat'] = lat
                data['lon'] = lon
                data['zoom'] = zoom
                segment_data.append(data)
                print(f"Zebrano segment: {lat:.4f}, {lon:.4f}, zoom {zoom}")
                log(f"Zebrano segment: {lat:.4f}, {lon:.4f}, zoom {zoom}")
                success = True
                break
            elif response.status_code == 400:
                continue
            else:
                print(f"Błąd {response.status_code} przy {lat:.4f},{lon:.4f}, zoom {zoom}")
                log(f"Błąd {response.status_code} przy {lat:.4f},{lon:.4f}, zoom {zoom}")
                break
        except Exception as e:
            print(f"Błąd połączenia: {e}")
            log(f"Błąd połączenia: {e}")
            break

    if not success:
        print(f"Nie udało się pobrać segmentu dla {lat:.4f},{lon:.4f}")
        log(f"Nie udało się pobrać segmentu dla {lat:.4f},{lon:.4f}")

    time.sleep(SLEEP_TIME)

now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
timestamp = now.strftime('%Y-%m-%d %H:%M:%S')

producer.send(KAFKA_TOPIC, value={
    "timestamp": timestamp,
    "segments": segment_data
})

producer.flush()
producer.close()
print(f"Wysłano {len(segment_data)} segmentów do Kafki jako jedna paczka.")
log(f"Wysłano {len(segment_data)} segmentów do Kafki jako jedna paczka.")
