from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    from_json, col, expr, to_timestamp,
    year as spark_year, month as spark_month, dayofweek as spark_dayofweek, hour as spark_hour,
    lit, udf, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, MapType,
    DoubleType, LongType, IntegerType, BooleanType, TimestampType
)
from datetime import datetime, timedelta, timezone
from pyspark.ml import PipelineModel

KAFKA_BOOTSTRAP_SERVERS = "namenode:9092,datanode1:9092"
TOPIC_OPENMETEO = "OpenMeteo"
TOPIC_TOMTOM = "TomTom"
TOPIC_GIOS = "GIOS"
CHECKPOINT_LOCATION = "hdfs://namenode:9000/tmp/spark_checkpoints/air_quality_consumer_processed"

TOMTOM_COORDS_PATH = "/home/hadoop/venvs/open_meteo/src/points.txt"

OFFLINE_MODELS_BASE_PATH = "hdfs://namenode:9000/user/hadoop/modele/offline_cv"
ONLINE_MODELS_BASE_PATH = "hdfs://namenode:9000/user/hadoop/modele/online_cv"

POLLUTANTS = ["pm10", "pm25", "no2", "o3", "so2"]

OFFLINE_MODEL_PATHS = {p: f"{OFFLINE_MODELS_BASE_PATH}/offline_rf_{p}" for p in POLLUTANTS}
ONLINE_MODEL_PATHS = {p: f"{ONLINE_MODELS_BASE_PATH}/online_rf_{p}" for p in POLLUTANTS}


MONGO_URI = "mongodb://namenode:27017"
MONGO_DATABASE = "air_quality_db"
MONGO_COLLECTION = "predictions"

loaded_offline_models = {}
loaded_online_models = {}

openmeteo_data_schema = StructType([
    StructField("date", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("dew_point_2m", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("surface_pressure", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True)
])

openmeteo_kafka_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", ArrayType(openmeteo_data_schema), True)
])

tomtom_flow_segment_data_schema = StructType([
    StructField("frc", StringType(), True),
    StructField("currentSpeed", IntegerType(), True),
    StructField("freeFlowSpeed", IntegerType(), True),
    StructField("currentTravelTime", IntegerType(), True),
    StructField("freeFlowTravelTime", IntegerType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("roadClosure", BooleanType(), True),
    StructField("coordinates", StructType([
        StructField("coordinate", ArrayType(StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True)
        ])), True)
    ]), True),
    StructField("length", IntegerType(), True)
])
tomtom_segment_schema = StructType([
    StructField("flowSegmentData", tomtom_flow_segment_data_schema, True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("zoom", IntegerType(), True)
])
tomtom_kafka_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("segments", ArrayType(tomtom_segment_schema), True)
])

gios_measurement_values_schema = StructType([
    StructField("Kod stanowiska", StringType(), True),
    StructField("Data", StringType(), True),
    StructField("Wartość", DoubleType(), True)
])
gios_sensor_api_response_schema = StructType([
    StructField("Lista danych pomiarowych", ArrayType(gios_measurement_values_schema), True),
])
gios_metadata_schema = StructType([
    StructField("kod_stacji", StringType(), True),
    StructField("id_stacji", LongType(), True),
    StructField("id_stanowiska", LongType(), True),
    StructField("kod", StringType(), True),
    StructField("wskaznik", StringType(), True)
])
gios_payload_entry_schema = StructType([
    StructField("metadata", gios_metadata_schema, True),
    StructField("data", gios_sensor_api_response_schema, True)
])
gios_kafka_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("data", ArrayType(gios_payload_entry_schema), True)
])


tomtom_segment_coords_tuples_detailed = []
try:
    with open(TOMTOM_COORDS_PATH, "r") as f:
        for line in f:
            if line.strip():
                lat_str, lon_str = line.strip().split(",")
                
                unrounded_lat_f = float(lat_str.strip())
                unrounded_lon_f = float(lon_str.strip())
                
                rounded_lat_f = round(unrounded_lat_f, 4)
                rounded_lon_f = round(unrounded_lon_f, 4)
                
                tomtom_segment_coords_tuples_detailed.append(
                    ((unrounded_lat_f, unrounded_lon_f), (rounded_lat_f, rounded_lon_f))
                )


    if not tomtom_segment_coords_tuples_detailed:
        print(f"WARNING: Nie wczytano żadnych koordynatów z {TOMTOM_COORDS_PATH}. Cechy TomTom nie będą działać.")
    else:
        print(f"INFO: Wczytano {len(tomtom_segment_coords_tuples_detailed)} koordynatów TomTom.")
except FileNotFoundError:
    print(f"ERROR: Plik z koordynatami TomTom {TOMTOM_COORDS_PATH} nie został znaleziony. Przerywam.")
    exit()
except Exception as e:
    print(f"ERROR: Wystąpił błąd podczas wczytywania koordynatów TomTom z {TOMTOM_COORDS_PATH}: {e}")
    exit()


OFFLINE_MODEL_FEATURE_COLS = [
    "temperature", "humidity", "dew_point", "rain", "snowfall", "pressure", "cloud_cover", "wind_speed", "wind_dir",
    "hour", "day_of_week", "month", "year",
    "pm10", "pm25", "no2", "o3", "so2",
    "pm10_t-1", "pm25_t-1", "no2_t-1", "o3_t-1", "so2_t-1"
]

ONLINE_MODEL_BASE_FEATURE_COLS = [ 
    "temperature", "humidity", "dew_point", "rain", "pressure", "cloud_cover", "wind_speed", "wind_dir",
    "hour", "day_of_week",
    "pm10", "pm25", "no2", "o3", "so2",
    "pm10_t-1", "pm25_t-1", "no2_t-1", "o3_t-1", "so2_t-1"
]


traffic_feature_names = []

for _, (r_lat_f, r_lon_f) in tomtom_segment_coords_tuples_detailed:
    lat_str_formatted = f"{r_lat_f:.4f}" # e.g. 52.2072 -> "52.2072"
    lon_str_formatted = f"{r_lon_f:.4f}" # e.g. 20.9073 -> "20.9073"
    
    formatted_lat_for_name = lat_str_formatted.replace('.', '-') # "52.2072" -> "52-2072"
    formatted_lon_for_name = lon_str_formatted.replace('.', '-') # "20.9073" -> "20-9073"
    
    traffic_feature_names.append(f"trafficJamRatio_{formatted_lat_for_name}_{formatted_lon_for_name}")

excluded_traffic_cols = [
    'trafficJamRatio_52-1882_20-9135', 'trafficJamRatio_52-1934_20-9244', 'trafficJamRatio_52-2014_20-9366',
    'trafficJamRatio_52-2037_20-9065', 'trafficJamRatio_52-2051_20-9105', 'trafficJamRatio_52-2072_20-9073',
    'trafficJamRatio_52-2077_20-9040', 'trafficJamRatio_52-2081_20-9073', 'trafficJamRatio_52-2089_20-9057',
    'trafficJamRatio_52-2089_20-9071', 'trafficJamRatio_52-2096_20-9078'
]

traffic_feature_names_filtered = [traffic_col for traffic_col in traffic_feature_names if traffic_col not in excluded_traffic_cols]


ONLINE_MODEL_FEATURE_COLS = ONLINE_MODEL_BASE_FEATURE_COLS + traffic_feature_names_filtered


ALL_MODEL_COLS = OFFLINE_MODEL_FEATURE_COLS.copy()
for model_col in ONLINE_MODEL_FEATURE_COLS:
    if model_col not in ALL_MODEL_COLS:
        ALL_MODEL_COLS.append(model_col)

def extract_openmeteo_features(openmeteo_payload_data, target_dt_utc):
    features = {
        "temperature": None, "humidity": None, "dew_point": None, "rain": None,
        "snowfall": None, "pressure": None, "cloud_cover": None, "wind_speed": None, "wind_dir": None
    }
    
    if not openmeteo_payload_data:
        return features

    for record in openmeteo_payload_data:
        if record is None or not hasattr(record, 'date'):
            print(f"DEBUG (OpenMeteo): Rekord jest None lub nie ma atrybutu 'date': {record}")
            continue
        record_dt_str = record.date
        if not record_dt_str:
            continue
        try:
            record_dt_utc = datetime.strptime(record_dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if record_dt_utc == target_dt_utc:
                features["temperature"] = record.temperature_2m if hasattr(record, 'temperature_2m') else None
                features["humidity"] = record.relative_humidity_2m if hasattr(record, 'relative_humidity_2m') else None
                features["dew_point"] = record.dew_point_2m if hasattr(record, 'dew_point_2m') else None
                features["rain"] = record.rain if hasattr(record, 'rain') else None
                features["snowfall"] = record.snowfall if hasattr(record, 'snowfall') else None
                features["pressure"] = record.surface_pressure if hasattr(record, 'surface_pressure') else None
                features["cloud_cover"] = record.cloud_cover if hasattr(record, 'cloud_cover') else None
                features["wind_speed"] = record.wind_speed_10m if hasattr(record, 'wind_speed_10m') else None
                features["wind_dir"] = record.wind_direction_10m if hasattr(record, 'wind_direction_10m') else None
                break
        except ValueError as e:
            print(f"DEBUG (OpenMeteo): Błąd parsowania daty '{record_dt_str}': {e}")
            continue
        except AttributeError as ae:
            print(f"DEBUG (OpenMeteo): AttributeError przy dostępie do pola w rekordzie {record}: {ae}")
            continue
        except Exception as ex:
            print(f"DEBUG (OpenMeteo): Nieoczekiwany błąd przy przetwarzaniu rekordu {record}: {ex}")
            continue
    return features

def extract_gios_features(gios_payload_data, target_dt_utc):
    features = {}
    standard_pollutant_codes = ["pm10", "pm25", "no2", "o3", "so2"]
    for p_code in standard_pollutant_codes:
        features[p_code] = None
        features[f"{p_code}_t-1"] = None

    if not gios_payload_data:
        return features

    target_dt_minus_1h_utc = target_dt_utc - timedelta(hours=1)

    for sensor_entry in gios_payload_data:
        if sensor_entry is None: continue

        metadata = sensor_entry.metadata if hasattr(sensor_entry, 'metadata') else None
        sensor_api_data = sensor_entry.data if hasattr(sensor_entry, 'data') else None

        if not metadata or not sensor_api_data:
            print(f"DEBUG (GIOS): Brak metadanych lub danych API w sensor_entry: {sensor_entry}")
            continue

        original_pollutant_code = metadata.kod if hasattr(metadata, 'kod') else None
        if not original_pollutant_code:
            continue

        current_pollutant_std_code = original_pollutant_code
        if original_pollutant_code == "pm2.5":
            current_pollutant_std_code = "pm25"

        if current_pollutant_std_code not in standard_pollutant_codes:
            continue
        
        pollutant_code = current_pollutant_std_code

        measurements_list_row = getattr(sensor_api_data, 'Lista danych pomiarowych', None)
        if measurements_list_row is None:
            try:
                measurements_list_row = sensor_api_data['Lista danych pomiarowych']
            except (KeyError, AttributeError):
                print(f"DEBUG (GIOS): Nie można uzyskać dostępu do 'Lista danych pomiarowych' w {sensor_api_data}")
                continue

        if not measurements_list_row:
            continue


        val_t = None
        val_t_minus_1 = None

        best_match_t_value = None
        min_diff_t = timedelta.max
        
        best_match_t_minus_1_value = None
        min_diff_t_minus_1 = timedelta.max
            
        for measurement_row in measurements_list_row:
            if measurement_row is None: continue

            m_dt_str = getattr(measurement_row, 'Data', None)
            m_value = getattr(measurement_row, 'Wartość', None)

            if m_dt_str is None or m_value is None:
                continue
            
            try:
                m_dt_local_naive = datetime.strptime(m_dt_str, "%Y-%m-%d %H:%M:%S")
                m_dt_utc_intermediate = m_dt_local_naive - timedelta(hours=2) 
                m_dt_utc = m_dt_utc_intermediate.replace(tzinfo=timezone.utc)

                if m_dt_utc <= target_dt_utc:
                    diff = target_dt_utc - m_dt_utc
                    if diff < min_diff_t:
                        min_diff_t = diff
                        best_match_t_value = m_value
                
                if m_dt_utc <= target_dt_minus_1h_utc:
                    diff_m1 = target_dt_minus_1h_utc - m_dt_utc
                    if diff_m1 < min_diff_t_minus_1:
                        min_diff_t_minus_1 = diff_m1
                        best_match_t_minus_1_value = m_value
                        
            except ValueError as e:
                continue
            except AttributeError as ae:
                print(f"DEBUG (GIOS): AttributeError przy dostępie do pola w measurement_row {measurement_row}: {ae}")
                continue
        
        if best_match_t_value is not None and min_diff_t < timedelta(hours=1):
             features[pollutant_code] = best_match_t_value
        if best_match_t_minus_1_value is not None and min_diff_t_minus_1 < timedelta(hours=1):
            features[f"{pollutant_code}_t-1"] = best_match_t_minus_1_value

    return features


def extract_tomtom_features(tomtom_payload_segments, detailed_coords_list):
    """
    Ekstrahuje cechy TomTom (trafficJamRatio) na podstawie danych z API i listy oczekiwanych koordynatów.

    Args:
        tomtom_payload_segments (list): Lista obiektów Row, każdy reprezentujący segment z API TomTom.
        detailed_coords_list (list): Lista krotek, gdzie każda krotka zawiera:
                                     ((niezaokr_lat_f, niezaokr_lon_f), (zaokr_4_lat_f, zaokr_4_lon_f)).
    Returns:
        dict: Słownik z cechami trafficJamRatio, gdzie klucze pasują do `traffic_feature_names`.
    """
    features = {}
    for full_feature_name in traffic_feature_names: 
        features[full_feature_name] = None

    if not tomtom_payload_segments:
        return features

    for segment_record in tomtom_payload_segments:
        if segment_record is None:
            continue

        fsd = segment_record.flowSegmentData if hasattr(segment_record, 'flowSegmentData') else None
        if not fsd:
            continue

        current_speed_val = fsd.currentSpeed if hasattr(fsd, 'currentSpeed') else None
        free_flow_speed_val = fsd.freeFlowSpeed if hasattr(fsd, 'freeFlowSpeed') else None
        
        lat_api_val = segment_record.lat if hasattr(segment_record, 'lat') else None
        lon_api_val = segment_record.lon if hasattr(segment_record, 'lon') else None

        if all(v is not None for v in [current_speed_val, free_flow_speed_val, lat_api_val, lon_api_val]):
            try:
                lat_f_api = float(lat_api_val)
                lon_f_api = float(lon_api_val)
                current_speed_f = float(current_speed_val)
                free_flow_speed_f = float(free_flow_speed_val)
            except (ValueError, TypeError) as e:
                continue

            matched_rounded_lat_for_name = None
            matched_rounded_lon_for_name = None

            for (unrounded_lat_from_file, unrounded_lon_from_file), \
                (rounded_lat_from_file, rounded_lon_from_file) in detailed_coords_list:
                
                if abs(lat_f_api - unrounded_lat_from_file) < 0.0000001 and \
                   abs(lon_f_api - unrounded_lon_from_file) < 0.0000001:
                    matched_rounded_lat_for_name = rounded_lat_from_file
                    matched_rounded_lon_for_name = rounded_lon_from_file
                    break
            
            if matched_rounded_lat_for_name is not None and matched_rounded_lon_for_name is not None:
                lat_str_formatted_for_key = f"{matched_rounded_lat_for_name:.4f}" # e.g. "52.2072"
                lon_str_formatted_for_key = f"{matched_rounded_lon_for_name:.4f}" # e.g. "20.9073"
                
                key_part_lat = lat_str_formatted_for_key.replace('.', '-') # e.g. "52-2072"
                key_part_lon = lon_str_formatted_for_key.replace('.', '-') # e.g. "20-9073"
                
                feature_name_key = f"trafficJamRatio_{key_part_lat}_{key_part_lon}"
                
                if feature_name_key in features:
                    if free_flow_speed_f != 0:
                        features[feature_name_key] = current_speed_f / free_flow_speed_f
                    else:
                        features[feature_name_key] = None 

    return features

def air_quality_index_for_pollutant(value, pollutant):
    if value is None:
        return None
    
    thresholds = {
        "PM10": [
            (0, 20, "Bardzo dobry"),
            (20.1, 50, "Dobry"),
            (50.1, 80, "Umiarkowany"),
            (80.1, 110, "Dostateczny"),
            (110.1, 150, "Zły"),
            (150.1, float('inf'), "Bardzo zły")
        ],
        "PM2_5": [
            (0, 13, "Bardzo dobry"),
            (13.1, 35, "Dobry"),
            (35.1, 55, "Umiarkowany"),
            (55.1, 75, "Dostateczny"),
            (75.1, 110, "Zły"),
            (110.1, float('inf'), "Bardzo zły")
        ],
        "O3": [
            (0, 70, "Bardzo dobry"),
            (70.1, 120, "Dobry"),
            (120.1, 150, "Umiarkowany"),
            (150.1, 180, "Dostateczny"),
            (180.1, 240, "Zły"),
            (240.1, float('inf'), "Bardzo zły")
        ],
        "NO2": [
            (0, 40, "Bardzo dobry"),
            (40.1, 100, "Dobry"),
            (100.1, 150, "Umiarkowany"),
            (150.1, 230, "Dostateczny"),
            (230.1, 400, "Zły"),
            (400.1, float('inf'), "Bardzo zły")
        ],
        "SO2": [
            (0, 50, "Bardzo dobry"),
            (50.1, 100, "Dobry"),
            (100.1, 200, "Umiarkowany"),
            (200.1, 350, "Dostateczny"),
            (350.1, 500, "Zły"),
            (500.1, float('inf'), "Bardzo zły")
        ],
    }

    for low, high, category in thresholds.get(pollutant, []):
        if low <= value <= high:
            return category
    return None

index_ranking = {
    "Brak indeksu": 0,
    "Bardzo dobry": 1,
    "Dobry": 2,
    "Umiarkowany": 3,
    "Dostateczny": 4,
    "Zły": 5,
    "Bardzo zły": 6
}

def calculate_final_air_quality(pm10, pm25, o3, no2, so2):
    values = {
        "PM10": pm10,
        "PM2_5": pm25,
        "O3": o3,
        "NO2": no2,
        "SO2": so2
    }

    indices = []
    for pollutant, val in values.items():
        idx = air_quality_index_for_pollutant(val, pollutant)
        if idx is not None:
            indices.append(idx)
    
    if not indices:
        return "Brak indeksu"

    worst_index = max(indices, key=lambda x: index_ranking.get(x, 0))
    return worst_index


spark_session_global = None

def process_joined_data(batch_df, epoch_id):
    global spark_session_global
    
    print(f"--- Przetwarzanie batcha ID: {epoch_id} ---")
    if batch_df.rdd.isEmpty():
        print("INFO: Brak kompletnych danych (z 3 tematów) w tym batchu.")
        return

    processed_rows_list = []
    
    print("INFO: COLLECTING INFO")
    collected_batch = batch_df.collect()
    print("INFO: INFO HAS BEEN COLLECTED")

    for row_spark in collected_batch:
        
        naive_target_event_time_ts = row_spark.matched_event_time
        if naive_target_event_time_ts is None:
            print("WARN: matched_event_time jest None, pomijam wiersz.")
            continue

        if naive_target_event_time_ts.tzinfo is None:
            target_event_time_ts = naive_target_event_time_ts.replace(tzinfo=timezone.utc)
        else:
            target_event_time_ts = naive_target_event_time_ts.astimezone(timezone.utc)

        print(f"DEBUG (Target Time): target_event_time_ts: {target_event_time_ts}, tzinfo: {target_event_time_ts.tzinfo}")
        
        print(f"INFO: Przetwarzanie danych dla timestampu: {target_event_time_ts}")

        time_features = {
            "hour": target_event_time_ts.hour,
            "day_of_week": target_event_time_ts.isoweekday(),
            "month": target_event_time_ts.month,
            "year": target_event_time_ts.year
        }
        openmeteo_features = extract_openmeteo_features(row_spark.openmeteo_data, target_event_time_ts)
        gios_features = extract_gios_features(row_spark.gios_data, target_event_time_ts)
        tomtom_features = extract_tomtom_features(row_spark.tomtom_data, tomtom_segment_coords_tuples_detailed)

        current_row_all_features = {"datetime_utc": target_event_time_ts}
        current_row_all_features.update(time_features)
        current_row_all_features.update(openmeteo_features)
        current_row_all_features.update(gios_features)
        current_row_all_features.update(tomtom_features)

        final_feature_dict_for_row = {"datetime_utc": target_event_time_ts}
        all_model_cols = list(set(ALL_MODEL_COLS))
        
        for col_name in all_model_cols:
            final_feature_dict_for_row[col_name] = current_row_all_features.get(col_name)
        
        processed_rows_list.append(Row(**final_feature_dict_for_row))

    if processed_rows_list:
        schema_fields = [StructField("datetime_utc", TimestampType(), True)]
        for col_name in list(set(ALL_MODEL_COLS)):
            if col_name in ["hour", "day_of_week", "month", "year"]:
                schema_fields.append(StructField(col_name, IntegerType(), True))
            else:
                schema_fields.append(StructField(col_name, DoubleType(), True))
        processed_data_schema = StructType(schema_fields)
        
        processed_df = spark_session_global.createDataFrame(processed_rows_list, schema=processed_data_schema)

        df_with_predictions = processed_df 
        
        cols_for_offline_input = ["datetime_utc"] + OFFLINE_MODEL_FEATURE_COLS

        actual_cols_for_offline_input = [c for c in cols_for_offline_input if c in processed_df.columns]
        
        offline_input_df = processed_df.select(*actual_cols_for_offline_input).dropna(subset=OFFLINE_MODEL_FEATURE_COLS)
        offline_input_df.show(truncate=False, vertical=True)

        if not offline_input_df.rdd.isEmpty():
            print(f"INFO: Przygotowano {offline_input_df.count()} wiersz(y) dla modelu OFFLINE.")
            for pollutant, model in loaded_offline_models.items():
                if model:
                    print(f"INFO (Batch {epoch_id}): Predykcja offline dla {pollutant}...")
                    try:
                        predictions_df = model.transform(offline_input_df)

                        current_preds = predictions_df.select(
                            col("datetime_utc"),
                            col("prediction").alias(f"offline_model_pred_{pollutant}")
                        )
                        print(f"--------------{pollutant}-")
                        current_preds.show(truncate=False, vertical=True)
                        df_with_predictions = df_with_predictions.join(current_preds, on="datetime_utc", how="left")
                        print(f"INFO (Batch {epoch_id}): Dodano predykcje offline dla {pollutant}.")

                    except Exception as e:
                        print(f"ERROR (Batch {epoch_id}): Błąd podczas predykcji offline dla {pollutant}: {e}")
                        df_with_predictions = df_with_predictions.withColumn(f"offline_model_pred_{pollutant}", lit(None).cast(DoubleType()))
                else:
                    print(f"WARN (Batch {epoch_id}): Model offline dla {pollutant} nie został załadowany, pomijam predykcję.")
                    df_with_predictions = df_with_predictions.withColumn(f"offline_model_pred_{pollutant}", lit(None).cast(DoubleType()))
        else:
            print("WARN: Brak kompletnych danych dla modelu OFFLINE po przetworzeniu i dropna.")
            for pollutant in POLLUTANTS:
                 if f"offline_model_pred_{pollutant}" not in df_with_predictions.columns:
                    df_with_predictions = df_with_predictions.withColumn(f"offline_model_pred_{pollutant}", lit(None).cast(DoubleType()))

        print(f"--- (Batch {epoch_id}) DataFrame z predykcjami OFFLINE ---")
        df_with_predictions.show(1, truncate=False, vertical=True)

        cols_for_online_input = ["datetime_utc"] + ONLINE_MODEL_FEATURE_COLS
        actual_cols_for_online_input = [c for c in cols_for_online_input if c in df_with_predictions.columns]

        online_input_df = df_with_predictions.select(*actual_cols_for_online_input).dropna(subset=ONLINE_MODEL_FEATURE_COLS)
        online_input_df.show(truncate=False, vertical=True)

        if not online_input_df.rdd.isEmpty():
            print(f"INFO: Przygotowano {online_input_df.count()} wiersz(y) dla modelu ONLINE.")
            for pollutant, model in loaded_online_models.items():
                if model:
                    print(f"INFO (Batch {epoch_id}): Predykcja online (residuum) dla {pollutant}...")
                    try:
                        predictions_df = model.transform(online_input_df)
                        current_preds = predictions_df.select(
                            col("datetime_utc"),
                            col("prediction").alias(f"online_model_pred_res_{pollutant}")
                        )
                        df_with_predictions = df_with_predictions.join(current_preds, on="datetime_utc", how="left")
                        print(f"INFO (Batch {epoch_id}): Dodano predykcje online (residuum) dla {pollutant}.")
                    except Exception as e:
                        print(f"ERROR (Batch {epoch_id}): Błąd podczas predykcji online dla {pollutant}: {e}")
                        df_with_predictions = df_with_predictions.withColumn(f"online_model_pred_res_{pollutant}", lit(None).cast(DoubleType()))
                else:
                    print(f"WARN (Batch {epoch_id}): Model online dla {pollutant} nie został załadowany, pomijam predykcję.")
                    df_with_predictions = df_with_predictions.withColumn(f"online_model_pred_res_{pollutant}", lit(None).cast(DoubleType()))

            for pollutant in POLLUTANTS:
                offline_col = f"offline_model_pred_{pollutant}"
                online_col = f"online_model_pred_res_{pollutant}"
                final_col = f"final_pred_{pollutant}"

                if offline_col in df_with_predictions.columns and online_col in df_with_predictions.columns:
                    df_with_predictions = df_with_predictions.withColumn(
                        final_col,
                        col(offline_col) - col(online_col)
                    )
                else:
                    print(f"WARN: Nie znaleziono kolumn {offline_col} i/lub {online_col}, pomijam final_pred dla {pollutant}.")

            final_air_quality_udf = udf(calculate_final_air_quality, StringType())

            df_with_predictions = df_with_predictions.withColumn(
                "offline_air_index",
                final_air_quality_udf(
                    col("offline_model_pred_pm10"),
                    col("offline_model_pred_pm25"),
                    col("offline_model_pred_o3"),
                    col("offline_model_pred_no2"),
                    col("offline_model_pred_so2")
                )
            )

            df_with_predictions = df_with_predictions.withColumn(
                "final_air_index",
                final_air_quality_udf(
                    col("final_pred_pm10"),
                    col("final_pred_pm25"),
                    col("final_pred_o3"),
                    col("final_pred_no2"),
                    col("final_pred_so2")
                )
            )

            print(f"--- (Batch {epoch_id}) DataFrame z WSZYSTKIMI predykcjami (Offline + Online Residua) ---")
            df_with_predictions.show(1, truncate=False, vertical=True)

            if not df_with_predictions.rdd.isEmpty():
                print(f"INFO (Batch {epoch_id}): Próba zapisu {df_with_predictions.count()} wierszy do MongoDB...")
                try:
                    (df_with_predictions.write.format("mongodb")
                        .mode("append")
                        .option("uri", MONGO_URI)
                        .option("database", MONGO_DATABASE)
                        .option("collection", MONGO_COLLECTION)
                        .save())
                    print(f"INFO (Batch {epoch_id}): Pomyślnie zapisano dane do MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION}")
                except Exception as e:
                    print(f"ERROR (Batch {epoch_id}): Błąd podczas zapisu do MongoDB: {e}")
            else:
                print(f"INFO (Batch {epoch_id}): Brak danych do zapisu do MongoDB (df_with_predictions jest puste).")

        else:
            print("WARN: Brak kompletnych danych dla modelu ONLINE po przetworzeniu i dropna.")
            for pollutant in POLLUTANTS:
                if f"online_model_pred_res_{pollutant}" not in df_with_predictions.columns:
                    df_with_predictions = df_with_predictions.withColumn(f"online_model_pred_res_{pollutant}", lit(None).cast(DoubleType()))
    else:
        print("INFO: Brak wierszy do przetworzenia w tym batchu (po pętli collect).")


def main():
    global spark_session_global
    spark_session_global = (SparkSession.builder
             .appName("AirQualityKafkaConsumerV2")
             .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
             .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}")
             .getOrCreate())
    spark_session_global.sparkContext.setLogLevel("WARN")

    print("INFO: Ładowanie modeli OFFLINE...")
    for pollutant, path in OFFLINE_MODEL_PATHS.items():
        try:
            loaded_offline_models[pollutant] = PipelineModel.load(path)
            print(f"INFO: Załadowano model offline dla {pollutant} z {path}")
        except Exception as e:
            print(f"ERROR: Nie udało się załadować modelu offline dla {pollutant} z {path}: {e}")
            loaded_offline_models[pollutant] = None 

    print("INFO: Ładowanie modeli ONLINE...")
    for pollutant, path in ONLINE_MODEL_PATHS.items():
        try:
            loaded_online_models[pollutant] = PipelineModel.load(path)
            print(f"INFO: Załadowano model online dla {pollutant} z {path}")
        except Exception as e:
            print(f"ERROR: Nie udało się załadować modelu online dla {pollutant} z {path}: {e}")
            loaded_online_models[pollutant] = None

    def read_kafka_stream_generic(topic_name, schema, data_field_name, stream_alias):
        return (spark_session_global.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic_name)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(from_json(col("value").cast("string"), schema).alias("payload"))
                .select("payload.*")
                .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .select(
                    col("event_time"),
                    col("timestamp").alias(f"{stream_alias}_orig_timestamp_str"),
                    col(data_field_name).alias(f"{stream_alias}_data")
                 ))

    openmeteo_stream_df = read_kafka_stream_generic(TOPIC_OPENMETEO, openmeteo_kafka_schema, "data", "openmeteo")
    tomtom_stream_df = read_kafka_stream_generic(TOPIC_TOMTOM, tomtom_kafka_schema, "segments", "tomtom")
    gios_stream_df = read_kafka_stream_generic(TOPIC_GIOS, gios_kafka_schema, "data", "gios")
    
    openmeteo_raw = openmeteo_stream_df.withColumn("event_date", to_date(col("event_time")))
    tomtom_raw = tomtom_stream_df.withColumn("event_date", to_date(col("event_time")))
    gios_raw = gios_stream_df.withColumn("event_date", to_date(col("event_time")))
    

    openmeteo_query = (openmeteo_raw.writeStream
        .format("json")
        .option("path", "/user/hadoop/bronze/openmeteo_raw")
        .option("checkpointLocation", "/user/hadoop/checkpoints/openmeteo_raw")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())

    tomtom_query = (tomtom_raw.writeStream
        .format("json")
        .option("path", "/user/hadoop/bronze/tomtom_raw")
        .option("checkpointLocation", "/user/hadoop/checkpoints/tomtom_raw")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())

    gios_query = (gios_raw.writeStream
        .format("json")
        .option("path", "/user/hadoop/bronze/gios_raw")
        .option("checkpointLocation", "/user/hadoop/checkpoints/gios_raw")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())


    watermark_delay = "10 minutes"
    openmeteo_with_watermark = openmeteo_stream_df.withWatermark("event_time", watermark_delay)
    tomtom_with_watermark = tomtom_stream_df.withWatermark("event_time", watermark_delay)
    gios_with_watermark = gios_stream_df.withWatermark("event_time", watermark_delay)

    joined_df = (openmeteo_with_watermark.alias("om")
                 .join(
                     tomtom_with_watermark.alias("tt"),
                     expr("om.event_time = tt.event_time"),
                     "inner"
                 )
                 .join(
                     gios_with_watermark.alias("gs"),
                     expr("om.event_time = gs.event_time"),
                     "inner"
                 )
                 .select(
                     col("om.event_time").alias("matched_event_time"),
                     col("om.openmeteo_data"),
                     col("tt.tomtom_data"),
                     col("gs.gios_data")
                 ))

    query = (joined_df.writeStream
             .foreachBatch(process_joined_data)
             .outputMode("append")
             .trigger(processingTime="30 seconds")
             .start())

    print("INFO: Konsument Spark Streaming (v2) uruchomiony. Oczekiwanie na dane z Kafki...")
    query.awaitTermination()
    openmeteo_query.awaitTermination()
    tomtom_query.awaitTermination()
    gios_query.awaitTermination()

if __name__ == "__main__":
    main()
