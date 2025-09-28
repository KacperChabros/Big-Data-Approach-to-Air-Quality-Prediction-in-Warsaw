from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel


spark = SparkSession.builder.appName("OfflinePredOnOnlineDataset").getOrCreate()

online_df = spark.read.parquet("hdfs:///user/hadoop/dane_przetworzone/online_model_ready_all_data_final.parquet")

feature_cols = [
    "temperature", "humidity", "dew_point", "rain", "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir",
    "hour", "day_of_week", "month", "year",
    "pm10", "pm25", "no2", "o3", "so2",
    "pm10_t-1", "pm25_t-1", "no2_t-1", "o3_t-1", "so2_t-1"
]

models_paths = {
    "pm10": "hdfs:///user/hadoop/modele/offline_cv/offline_rf_pm10",
    "pm25": "hdfs:///user/hadoop/modele/offline_cv/offline_rf_pm25",
    "no2": "hdfs:///user/hadoop/modele/offline_cv/offline_rf_no2",
    "o3": "hdfs:///user/hadoop/modele/offline_cv/offline_rf_o3",
    "so2": "hdfs:///user/hadoop/modele/offline_cv/offline_rf_so2"
}


models = {pollutant: PipelineModel.load(path) for pollutant, path in models_paths.items()}

for pollutant, model in models.items():
    df_features = online_df.select("datetime_utc", *feature_cols).dropna()

    pred_df = model.transform(df_features).select("datetime_utc", col("prediction").alias(f"offline_model_pred_{pollutant}"))

    online_df = online_df.join(pred_df, on="datetime_utc", how="left")

    online_df = online_df.withColumn(
        f"res_{pollutant}",
        col(f"offline_model_pred_{pollutant}") - col(f"{pollutant}_t+1")
    )


online_df.write.mode("overwrite").parquet("hdfs:///user/hadoop/dane_przetworzone/online_training_dataset_with_offline_preds_final.parquet")

