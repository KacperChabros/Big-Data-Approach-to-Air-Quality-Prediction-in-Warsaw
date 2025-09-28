from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, input_file_name, regexp_extract,
    expr, explode, when, lit, struct, concat_ws, array
)

spark = SparkSession.builder.appName("InitialOnlineFullTrainingTransform").getOrCreate()

meteo_df = spark.read.csv(
    "hdfs://namenode:9000/user/hadoop/online_data_final_11_30/online_data_open_meteo_11_30.csv",
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

gios_raw = spark.read.option("multiLine", True).json(
    "hdfs://namenode:9000/user/hadoop/online_data_final_11_30/gios/dane_stanowiska*/MzWarChrosci_10955/*.json"
)

gios_with_filename = gios_raw.withColumn("filename", input_file_name())
gios_with_pollutant = gios_with_filename.withColumn(
    "pollutant",
    regexp_extract(col("filename"), r"_([a-zA-Z0-9\.]+)\.json", 1)
)

gios_with_pollutant = gios_with_pollutant.withColumn("pollutant", when(col("pollutant") == "pm2.5", "pm25")
    .when(col("pollutant") == "pm10", "pm10")
    .when(col("pollutant") == "no2", "no2")
    .when(col("pollutant") == "o3", "o3")
    .when(col("pollutant") == "so2", "so2")
    .otherwise(col("pollutant")))

gios_exploded = gios_with_pollutant.select(explode(col("Lista danych pomiarowych")).alias("measurement"), col("pollutant"))

gios_flat = gios_exploded.select(
    to_timestamp(col("measurement.Data"), "yyyy-MM-dd HH:mm:ss").alias("datetime_local"),
    col("measurement.Wartość").alias("value"),
    col("pollutant")
)

gios_flat = gios_flat.withColumn("datetime_utc", col("datetime_local") - expr("INTERVAL 2 HOURS"))

gios_pivoted = gios_flat.groupBy("datetime_utc").pivot("pollutant", ["pm10", "pm25", "no2", "o3", "so2"]).agg({"value": "max"})

joined_df = meteo_df.join(gios_pivoted, on="datetime_utc", how="inner")



source_path = "hdfs://namenode:9000/user/hadoop/online_data_final_11_30/tomtom/filtered_data/*/*/segment_*.json"
tt_raw = spark.read.option("multiLine", True).json(source_path)
tt = tt_raw.withColumn("file_path", input_file_name())

tt = tt.withColumn("date", regexp_extract("file_path", r"/(\d{4}-\d{2}-\d{2})/", 1))
tt = tt.withColumn("hour", regexp_extract("file_path", r"/(\d{2})/segment_", 1))
tt = tt.withColumn("lat", regexp_extract("file_path", r"segment_(\d+\.\d+)_", 1))
tt = tt.withColumn("long", regexp_extract("file_path", r"_(\d+\.\d+)_z\d{2}\.json", 1))
tt = tt.withColumn("datetime_utc", to_timestamp(concat_ws(" ", col("date"), col("hour")), "yyyy-MM-dd HH"))

tt = tt.withColumn("frc", col("flowSegmentData.frc")) \
       .withColumn("currentSpeed", col("flowSegmentData.currentSpeed")) \
       .withColumn("freeFlowSpeed", col("flowSegmentData.freeFlowSpeed")) \
       .withColumn("roadClosure", col("flowSegmentData.roadClosure")) \
       .withColumn("latlong", concat_ws("_", col("lat"), col("long")))


tt_pivot = tt.withColumn("frc_struct", struct(
                            col("frc").cast("string").alias("value"),
                            concat_ws("_", lit("frc"), col("latlong")).alias("column_name"))) \
             .withColumn("currentSpeed_struct", struct(
                            col("currentSpeed").cast("string").alias("value"),
                            concat_ws("_", lit("currentSpeed"), col("latlong")).alias("column_name"))) \
             .withColumn("freeFlowSpeed_struct", struct(
                            col("freeFlowSpeed").cast("string").alias("value"),
                            concat_ws("_", lit("freeFlowSpeed"), col("latlong")).alias("column_name"))) \
             .withColumn("roadClosure_struct", struct(
                            col("roadClosure").cast("string").alias("value"),
                            concat_ws("_", lit("roadClosure"), col("latlong")).alias("column_name")))

tt_exploded = tt_pivot.select(
    "datetime_utc",
    explode(array(
        col("frc_struct"),
        col("currentSpeed_struct"),
        col("freeFlowSpeed_struct"),
        col("roadClosure_struct")
    )).alias("kv")
)


tt_kv = tt_exploded.select(
    "datetime_utc",
    col("kv.column_name"),
    col("kv.value")
)

tt_wide = tt_kv.groupBy("datetime_utc").pivot("column_name").agg({"value": "first"})



joined_all_df = joined_df.join(tt_wide, on="datetime_utc", how="inner")

joined_all_df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/hadoop/dane_przetworzone/online_full_initial_training_dataset_final.parquet")

spark.stop()

