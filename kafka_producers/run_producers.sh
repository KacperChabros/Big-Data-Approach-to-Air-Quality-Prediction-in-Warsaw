#!/bin/bash
source /home/hadoop/venvs/open_meteo/bin/activate

python3 /home/hadoop/venvs/open_meteo/src/weather_kafka_producer.py
python3 /home/hadoop/venvs/open_meteo/src/tomtom_kafka_producer.py
python3 /home/hadoop/venvs/open_meteo/src/gios_kafka_producer.py
