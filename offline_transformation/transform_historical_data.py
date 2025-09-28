from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, from_utc_timestamp, input_file_name, regexp_extract, expr, split, explode
)

spark = SparkSession.builder.appName("AirQualityTransform").getOrCreate()

custom_columns = [
    "time", "temperature", "humidity", "dew_point", "rain",
    "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir"
]

meteo_df = spark.read.csv(
    "hdfs://namenode:9000/user/hadoop/dane_weather/historical_data_open_meteo_2019_2025_cleaned.csv",
    header=True,
    inferSchema=True
)

meteo_df = meteo_df.selectExpr([
    "time",
    "`temperature_2m (°C)` as temperature",
    "`relative_humidity_2m (%)` as humidity",
    "`dew_point_2m (°C)` as dew_point",
    "`rain (mm)` as rain",
    "`snowfall (cm)` as snowfall",
    "`surface_pressure (hPa)` as pressure",
    "`cloud_cover (%)` as cloud_cover",
    "`wind_speed_10m (km/h)` as wind_speed",
    "`wind_direction_10m (°)` as wind_dir"
])


meteo_df = meteo_df.withColumn("datetime_utc", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))

meteo_df = meteo_df.drop("time")


gios_raw = spark.read.option("multiLine", True).json("hdfs://namenode:9000/user/hadoop/dane_gios/gios_archive_data//*/*/*/*.json")

valid_station_codes = [
    "MzWarChrosci-PM10-1g",
    "MzWarChrosci-PM2.5-1g",
    "MzWarChrosci-NO2-1g",
    "MzWarChrosci-O3-1g",
    "MzWarChrosci-SO2-1g"
]


gios_df = gios_raw.select(
    (to_timestamp(col("Data")) - expr("INTERVAL 1 HOUR")).alias("datetime_utc"),
    col("Kod stanowiska"),
    col("Wartość")
)


gios_filtered = gios_df.filter(col("Kod stanowiska").isin(valid_station_codes))

gios_pivoted = gios_filtered.groupBy("datetime_utc").pivot("Kod stanowiska").agg({"Wartość": "max"})

rename_map = {
    'MzWarChrosci-PM10-1g': 'pm10',
    'MzWarChrosci-PM2.5-1g': 'pm25',
    'MzWarChrosci-NO2-1g': 'no2',
    'MzWarChrosci-O3-1g': 'o3',
    'MzWarChrosci-SO2-1g': 'so2',
}

for old_name, new_name in rename_map.items():
    if old_name in gios_pivoted.columns:
        gios_pivoted = gios_pivoted.withColumnRenamed(old_name, new_name)


joined_df = meteo_df.join(
    gios_pivoted,
    on="datetime_utc",
    how="inner"
)

joined_df.show()

joined_df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/hadoop/dane_przetworzone/initial_transform_meteo_gios.parquet")

spark.stop()

