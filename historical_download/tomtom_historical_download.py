API_KEY = "API_KEY"
UNIT = "KMPH"
SLEEP_TIME = 0.1

import requests
import json
import time
from pathlib import Path
from datetime import datetime

now = datetime.now()
day_str = now.strftime("%Y-%m-%d")
hour_str = now.strftime("%H")
OUTPUT_DIR = Path("/home/home_dir/data") / day_str / hour_str
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
log_file = OUTPUT_DIR / "log.txt"
def log(message):
    with open(log_file, "a", encoding="utf-8") as logf:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logf.write(f"[{timestamp}] {message}\n")
log("Starting download...")
coords_file = Path("points.txt")
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
                segment_data.append(data)
                filename = OUTPUT_DIR / f"segment_{lat:.4f}_{lon:.4f}_z{zoom}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                success = True
                break
            elif response.status_code == 400:
                continue
            else:
                log(f"Błąd {response.status_code} przy {lat:.4f},{lon:.4f}, zoom {zoom}")
                break
        except Exception as e:
            log(f"Błąd połączenia: {e}")
            break

    if not success:
        log(f"Nie udało się pobrać segmentu dla {lat:.4f},{lon:.4f}")

    time.sleep(SLEEP_TIME)

log(f"Zakończono pobieranie ({len(segment_data)} segmentów).")

