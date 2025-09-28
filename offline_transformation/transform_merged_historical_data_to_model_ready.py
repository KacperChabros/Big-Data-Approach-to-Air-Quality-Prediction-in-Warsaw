from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, hour, dayofweek, month, year, sum, when, avg, lag, lead, unix_timestamp, expr, round

spark = SparkSession.builder \
    .appName("TransformHistoricalToModelReady") \
    .getOrCreate()

input_path = "hdfs:///user/hadoop/dane_przetworzone/initial_transform_meteo_gios.parquet"
output_path = "hdfs:///user/hadoop/dane_przetworzone/meteo_gios_model_ready.parquet"

df = spark.read.parquet(input_path)

df_model_ready = df \
    .withColumn("hour", hour(col("datetime_utc"))) \
    .withColumn("day_of_week", dayofweek(col("datetime_utc"))) \
    .withColumn("month", month(col("datetime_utc"))) \
    .withColumn("year", year(col("datetime_utc")))


df_model_ready = df_model_ready.orderBy("datetime_utc")

window_spec = Window.orderBy("datetime_utc")

for pollutant in ["pm10", "pm25", "no2", "o3", "so2"]:
    lag_col = f"{pollutant}_lag"
    lead_col = f"{pollutant}_lead"

    df_model_ready = df_model_ready.withColumn(lag_col, lag(col(pollutant)).over(window_spec))
    df_model_ready = df_model_ready.withColumn(lead_col, lead(col(pollutant)).over(window_spec))
    df_model_ready = df_model_ready.withColumn(
        pollutant,
        when(
            col(pollutant).isNull(),
            (col(lag_col) + col(lead_col)) / 2
        ).otherwise(col(pollutant))
    )
    df_model_ready = df_model_ready.drop(lag_col, lead_col)


df_final = df_model_ready.dropna()

df_final = df_final.orderBy("datetime_utc")


df_final = df_final.withColumn("ts", unix_timestamp(col("datetime_utc")))

window_spec = Window.orderBy("datetime_utc")

for pollutant in ["pm10", "pm25", "no2", "o3", "so2"]:
    lag1_val = lag(col(pollutant), 1).over(window_spec)
    lag1_ts = lag(col("ts"), 1).over(window_spec)
    
    lead_val = lead(col(pollutant), 1).over(window_spec)
    lead_ts = lead(col("ts"), 1).over(window_spec)

    df_final = df_final \
        .withColumn(
            f"{pollutant}_t-1",
            when(col("ts") - lag1_ts == 3600, lag1_val).otherwise(None)
        ) \
        .withColumn(
            f"{pollutant}_t+1",
            when(lead_ts - col("ts") == 3600, lead_val).otherwise(None)
        )
        
df_final = df_final.drop("ts")

df_final = df_final.dropna()

df_final.show()

df_final.write.mode("overwrite").parquet(output_path)

spark.stop()
