import json
import pickle
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, count, mean, call_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from io import BytesIO

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'sensor_data_stream'

spark = SparkSession.builder \
    .appName("RealTimeMLProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("is_critical", IntegerType(), True),
])

X_mock = [[t, h] for t in range(20, 36) for h in range(40, 71)]
y_mock = [1 if t > 30 and h > 60 else 0 for t, h in X_mock]
X_train_mock, _, y_train_mock, _ = train_test_split(X_mock, y_mock, test_size=0.5, random_state=42)

mock_model = LogisticRegression().fit(X_train_mock, y_train_mock)
model_bytes = pickle.dumps(mock_model)

def predict_critical_alert(temperature, humidity):
    if temperature is not None and humidity is not None:
        if temperature > 30 and humidity > 60:
            return 1
    return 0
    
spark.udf.register("predict_alert_udf", predict_critical_alert, IntegerType())

kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

parsed_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.*"),
    to_timestamp(col("data.timestamp")).alias("event_time")
)

windowed_avg_df = parsed_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "2 minutes"),
        col("sensor_id")
    ) \
    .agg(
        col("sensor_id").alias("agg_sensor_id"),
        count("temperature").alias("readings_count"),
        mean("temperature").alias("avg_temperature_5min"),
        mean("humidity").alias("avg_humidity_5min")
    ) \
    .select("window.start", "window.end", "agg_sensor_id", "readings_count", "avg_temperature_5min", "avg_humidity_5min")

ml_predictions_df = parsed_df.withColumn(
    "ml_alert_prediction",
    call_udf("predict_alert_udf", col("temperature"), col("humidity"))
)

query_agg = windowed_avg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("WindowedAggregationOutput") \
    .start()

query_ml = ml_predictions_df.selectExpr(
        "CAST(sensor_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", "ml_alert_topic") \
    .option("checkpointLocation", "/tmp/spark/ml_alert_checkpoint") \
    .queryName("MLAlertsToKafka") \
    .start()


print("Starting Spark Structured Streaming. Press Ctrl+C to stop.")
query_agg.awaitTermination()
query_ml.awaitTermination()