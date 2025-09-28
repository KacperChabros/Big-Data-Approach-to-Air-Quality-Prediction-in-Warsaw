from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import monotonically_increasing_id, col, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("PredictOfflinePM10_t+1").getOrCreate()

df = spark.read.parquet("hdfs:///user/hadoop/dane_przetworzone/meteo_gios_model_ready_log.parquet")

feature_cols = [
    "temperature", "humidity", "dew_point", "rain", "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir", 
    
    "hour", "day_of_week", "month", "year",
    
    "pm10", "pm25", "no2", "o3", "so2",

    "pm10_t-1", "pm25_t-1", "no2_t-1", "o3_t-1", "so2_t-1"
]

target_column = "pm10_t+1"
max_depth = 10
num_trees = 200

df_clean = df.dropna(subset=feature_cols + [target_column])
df_clean = df_clean.withColumn("datetime_utc", to_timestamp("datetime_utc", "yyyy-MM-dd HH:mm"))

df_sorted = df_clean.orderBy(col("datetime_utc"))
total_count = df_sorted.count()
train_count = int(total_count * 0.8)

windowSpec = Window.orderBy("datetime_utc")
df_sorted = df_sorted.withColumn("row_num", row_number().over(windowSpec))

train_data = df_sorted.filter(col("row_num") <= train_count)
test_data = df_sorted.filter(col("row_num") > train_count)

train_data = train_data.drop("row_id")
test_data = test_data.drop("row_id")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)

rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol=target_column, numTrees=num_trees, maxDepth=max_depth, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, rf])

model = pipeline.fit(train_data)

predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol=target_column, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

evaluator_r2 = RegressionEvaluator(labelCol=target_column, predictionCol="prediction", metricName="r2")
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

model.write().overwrite().save("hdfs:///user/hadoop/modele/offline/offline_rf_pm10")
