import json
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone, timedelta

INTERESUJACE_WSKAZNIKI = {
    ("benzen", "c6h6"),
    ("tlenek węgla", "co"),
    ("dwutlenek azotu", "no2"),
    ("dwutlenek siarki", "so2"),
    ("ozon", "o3"),
    ("pył zawieszony pm10", "pm10"),
    ("pył zawieszony pm2.5", "pm2.5")
}

ID_STACJI = 10955
KOD_STACJI = "MzWarChrosci"

STANOWISKA = [
    {"Identyfikator stanowiska": 18200, "Wskaźnik": "pył zawieszony PM10", "Wskaźnik - kod": "PM10"},
    {"Identyfikator stanowiska": 18201, "Wskaźnik": "pył zawieszony PM2.5", "Wskaźnik - kod": "PM2.5"},
    {"Identyfikator stanowiska": 20408, "Wskaźnik": "dwutlenek azotu", "Wskaźnik - kod": "NO2"},
    {"Identyfikator stanowiska": 20410, "Wskaźnik": "ozon", "Wskaźnik - kod": "O3"},
    {"Identyfikator stanowiska": 20411, "Wskaźnik": "dwutlenek siarki", "Wskaźnik - kod": "SO2"},
]

KAFKA_BROKER = "namenode:9092,datanode1:9092"
KAFKA_TOPIC = "GIOS"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

size = 500
sort = "Data"
headers = {"accept": "application/ld+json"}
bledne_id = []
wszystkie_dane = []

def round_down_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

for stanowisko in STANOWISKA:
    wskaznik = stanowisko["Wskaźnik"].lower()
    kod = stanowisko["Wskaźnik - kod"].lower()
    id_stanowiska = stanowisko["Identyfikator stanowiska"]

    if (wskaznik, kod) not in INTERESUJACE_WSKAZNIKI:
        continue

    url = f"https://api.gios.gov.pl/pjp-api/v1/rest/data/getData/{id_stanowiska}?size={size}&sort={sort}"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        dane = response.json()
        metadata = {
            "kod_stacji": KOD_STACJI,
            "id_stacji": ID_STACJI,
            "id_stanowiska": id_stanowiska,
            "kod": kod,
            "wskaznik": wskaznik,
        }

        payload = {
            "metadata": metadata,
            "data": dane
        }

        wszystkie_dane.append(payload)
        print(f"Zebrano dane: {id_stanowiska}_{kod}")

    except requests.RequestException as e:
        print(f"Błąd dla stanowiska {id_stanowiska}: {e}")
        bledne_id.append(id_stanowiska)

timestamp_utc = round_down_hour(datetime.now(timezone.utc)).strftime("%Y-%m-%d %H:%M:%S")

final_payload = {
    "timestamp": timestamp_utc,
    "data": wszystkie_dane
}

print(f"\nWysyłanie jednego pakietu do Kafki...")
producer.send(KAFKA_TOPIC, value=final_payload)
producer.flush()

if bledne_id:
    print("\nBłędy przy stanowiskach:")
    for id_ in bledne_id:
        print(f"- {id_}")

