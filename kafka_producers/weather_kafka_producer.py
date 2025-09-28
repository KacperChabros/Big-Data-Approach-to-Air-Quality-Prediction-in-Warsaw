import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.207742,
    "longitude": 20.906073,
    "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "rain", "snowfall", "surface_pressure", "cloud_cover", "wind_speed_10m", "wind_direction_10m"],
    "past_days": 1,
    "forecast_days": 1
}

responses = openmeteo.weather_api(url, params=params)
response = responses[0]

hourly = response.Hourly()
hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ),
    "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
    "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
    "dew_point_2m": hourly.Variables(2).ValuesAsNumpy(),
    "rain": hourly.Variables(3).ValuesAsNumpy(),
    "snowfall": hourly.Variables(4).ValuesAsNumpy(),
    "surface_pressure": hourly.Variables(5).ValuesAsNumpy(),
    "cloud_cover": hourly.Variables(6).ValuesAsNumpy(),
    "wind_speed_10m": hourly.Variables(7).ValuesAsNumpy(),
    "wind_direction_10m": hourly.Variables(8).ValuesAsNumpy()
}

hourly_dataframe = pd.DataFrame(data=hourly_data)

hourly_dataframe['date'] = hourly_dataframe['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

producer = KafkaProducer(
    bootstrap_servers='namenode:9092,datanode1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
timestamp = now.strftime('%Y-%m-%d %H:%M:%S')

payload = {
    "timestamp": timestamp,
    "data": hourly_dataframe.to_dict(orient='records')
}

producer.send('OpenMeteo', value=payload)

producer.flush()
producer.close()

print("Dane pogodowe zostały wysłane do Kafki.")

