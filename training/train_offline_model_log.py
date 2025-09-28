from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import expm1, col, monotonically_increasing_id

spark = SparkSession.builder.appName("PredictOfflineLogSO2_t+1").getOrCreate()

df = spark.read.parquet("hdfs:///user/hadoop/dane_przetworzone/meteo_gios_model_ready_log.parquet")

feature_cols = [
    "temperature", "humidity", "dew_point", "rain", "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir", 
    
    "hour", "day_of_week", "month", "year",
    
    "log_pm10", "log_pm25", "log_no2", "log_o3", "log_so2",

    "log_pm10_t-1", "log_pm25_t-1", "log_no2_t-1", "log_o3_t-1", "log_so2_t-1"
]

target_column = "log_so2_t+1"
target_column_to_evaluate = target_column.replace("log_", "", 1)

df_clean = df.dropna(subset=feature_cols + [target_column])

df_sorted = df_clean.orderBy(col("datetime_utc"))
total_count = df_sorted.count()
train_count = int(total_count * 0.8)

df_with_id = df_sorted.withColumn("row_id", monotonically_increasing_id())

train_data = df_with_id.filter(col("row_id") < train_count)
test_data = df_with_id.filter(col("row_id") >= train_count)

train_data = train_data.drop("row_id")
test_data = test_data.drop("row_id")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)

rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol=target_column, numTrees=200, maxDepth=5)

pipeline = Pipeline(stages=[assembler, scaler, rf])

model = pipeline.fit(train_data)

predictions = model.transform(test_data)
predictions = predictions.withColumn("prediction_original_scale", expm1(col("prediction")))

evaluator = RegressionEvaluator(labelCol=target_column_to_evaluate, predictionCol="prediction_original_scale", metricName="rmse")
rmse = evaluator.evaluate(predictions)

evaluator_r2 = RegressionEvaluator(labelCol=target_column_to_evaluate, predictionCol="prediction_original_scale", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)

print("----------------------------------------------------------------------------------")
print(f"RMSE: {rmse}")
print(f"R2: {r2}")

predictions.select(
    *feature_cols,
    target_column,
    "prediction"
).show(10, truncate=False)

rf_model = model.stages[-1]

importances = rf_model.featureImportances

importances_list = importances.toArray().tolist()

features_importance = list(zip(feature_cols, importances_list))
features_importance_sorted = sorted(features_importance, key=lambda x: x[1], reverse=True)

print("-------------------------------Feature importances:--------------------------")
for feat, imp in features_importance_sorted:
    print(f"{feat}: {imp}")

model.write().overwrite().save("hdfs:///user/hadoop/modele/offline/offline_rf_so2_log")
