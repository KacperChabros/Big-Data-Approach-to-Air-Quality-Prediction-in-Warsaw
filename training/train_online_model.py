from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("OnlineResidualModel_PM25").getOrCreate()

df = spark.read.parquet("hdfs:///user/hadoop/dane_przetworzone/online_training_dataset_with_offline_preds.parquet")

base_feature_cols = [
    "temperature", "humidity", "dew_point", "rain", "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir",
    "hour", "day_of_week", "month", "year",
    "pm10", "pm25", "no2", "o3", "so2",
    "pm10_t-1", "pm25_t-1", "no2_t-1", "o3_t-1", "so2_t-1"
]

traffic_features = [col for col in df.columns if col.startswith("trafficJamRatio_")]

feature_cols = base_feature_cols + traffic_features

target_column = "res_pm25"

df_clean = df.dropna(subset=feature_cols + [target_column])

train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True)
rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol=target_column, numTrees=200, maxDepth=5)

pipeline = Pipeline(stages=[assembler, scaler, rf])

model = pipeline.fit(train_data)

predictions = model.transform(test_data)

evaluator_rmse = RegressionEvaluator(labelCol=target_column, predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol=target_column, predictionCol="prediction", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print("------------------------------------------------------------------------")
print(f"RMSE ({target_column}): {rmse}")
print(f"R2 ({target_column}): {r2}")

predictions.select(
    *feature_cols,
    target_column,
    "prediction"
).show(10, truncate=False)

rf_model = model.stages[-1]
importances_list = rf_model.featureImportances.toArray().tolist()

features_importance = list(zip(feature_cols, importances_list))
features_importance_sorted = sorted(features_importance, key=lambda x: x[1], reverse=True)

print("-------------------------------Feature Importances (res_pm10)--------------------------")
for feat, imp in features_importance_sorted:
    print(f"{feat}: {imp}")

model.write().overwrite().save("hdfs:///user/hadoop/modele/online/online_rf_res_pm25")
