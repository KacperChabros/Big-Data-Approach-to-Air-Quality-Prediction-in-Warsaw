from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, sum, when,
    lag, lead, unix_timestamp
)

spark = SparkSession.builder \
    .appName("PrepareOnlineDataWithPollutantInterpolation") \
    .getOrCreate()

input_path = "hdfs:///user/hadoop/dane_przetworzone/online_full_initial_training_dataset_final.parquet"
output_path = "hdfs:///user/hadoop/dane_przetworzone/online_model_ready_all_data_final.parquet"

df = spark.read.parquet(input_path)


for old_name in df.columns:
    new_name = old_name.replace('.', '-')
    if new_name != old_name:
        df = df.withColumnRenamed(old_name, new_name)


cols_to_drop = [c for c in df.columns if c.startswith("frc_") or c.startswith("roadClosure_")]
df = df.drop(*cols_to_drop)


df = df \
    .withColumn("hour", hour(col("datetime_utc"))) \
    .withColumn("day_of_week", dayofweek(col("datetime_utc"))) \
    .withColumn("month", month(col("datetime_utc"))) \
    .withColumn("year", year(col("datetime_utc")))


df = df.orderBy("datetime_utc")
window_spec = Window.orderBy("datetime_utc")

for pollutant in ["pm10", "pm25", "no2", "o3", "so2"]:
    lag_col = f"{pollutant}_lag"
    lead_col = f"{pollutant}_lead"

    df = df.withColumn(lag_col, lag(col(pollutant)).over(window_spec))
    df = df.withColumn(lead_col, lead(col(pollutant)).over(window_spec))

    df = df.withColumn(
        pollutant,
        when(col(pollutant).isNull(), (col(lag_col) + col(lead_col)) / 2).otherwise(col(pollutant))
    )

    df = df.drop(lag_col, lead_col)

df = df.withColumn("ts", unix_timestamp(col("datetime_utc")))

for pollutant in ["pm10", "pm25", "no2", "o3", "so2"]:
    lag_val = lag(col(pollutant), 1).over(window_spec)
    lag_ts = lag(col("ts"), 1).over(window_spec)
    lead_val = lead(col(pollutant), 1).over(window_spec)
    lead_ts = lead(col("ts"), 1).over(window_spec)

    df = df.withColumn(
        f"{pollutant}_t-1",
        when(col("ts") - lag_ts == 3600, lag_val)
    ).withColumn(
        f"{pollutant}_t+1",
        when(lead_ts - col("ts") == 3600, lead_val)
    )

df = df.drop("ts")

df_cleaned = df.dropna()

cols = df_cleaned.columns

current_cols = [c for c in cols if c.startswith('currentSpeed_')]
freeflow_cols = [c for c in cols if c.startswith('freeFlowSpeed_')]

def suffix(colname, prefix):
    return colname[len(prefix):]  # e.g. '52-1977_20-9222'

freeflow_map = {suffix(c, 'freeFlowSpeed_'): c for c in freeflow_cols}

for c in current_cols:
    suf = suffix(c, 'currentSpeed_')
    free_col = freeflow_map.get(suf)
    if free_col:
        new_col_name = f'trafficJamRatio_{suf}'
        df_cleaned = df_cleaned.withColumn(new_col_name,
                           when(col(free_col) != 0, col(c) / col(free_col))
                           .otherwise(None))

df_cleaned = df_cleaned.drop(*current_cols).drop(*freeflow_cols)

row_count = df_cleaned.count()
col_count = len(df_cleaned.columns)
print(f"Final shape: ({row_count}, {col_count})")

df_cleaned.write.mode("overwrite").parquet(output_path)

spark.stop()

