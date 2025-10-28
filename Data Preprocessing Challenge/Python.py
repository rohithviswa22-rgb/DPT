from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mean, stddev, count, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("DataPreprocessingChallenge").getOrCreate()

data = [
    (1, "ProductA", 15.5, "2023-01-01", "High", "100"),
    (2, "ProductB", 20.0, "2023-01-02", "Medium", "150"),
    (3, "ProductA", None, "2023-01-01", "High", "100"),
    (4, "ProductC", 10.0, "2023-01-03", "Low", "80"),
    (5, "ProductB", 25.0, "2023-01-04", None, "120"),
    (6, "ProductD", 50.0, "2023-01-05", "Critical", "ERROR")
]
columns = ["id", "product_name", "price", "date", "priority", "sales_qty"]
raw_df = spark.createDataFrame(data, columns)

processed_df = raw_df.withColumn(
    "sales_qty",
    when(col("sales_qty").cast("int").isNotNull(), col("sales_qty").cast("int")).otherwise(None)
)

processed_df = processed_df.dropDuplicates()

price_mean = processed_df.agg(mean("price")).collect()[0][0]
processed_df = processed_df.na.fill({"price": price_mean})

processed_df = processed_df.na.fill({"priority": "Unknown"})

processed_df = processed_df.na.drop(subset=["sales_qty"])

processed_df = processed_df.withColumn(
    "transaction_month",
    col("date").substr(6, 2).cast("int")
)

processed_df = processed_df.withColumn(
    "is_high_priority",
    when(col("priority") == "High", 1).otherwise(0)
)

feature_cols = ["price", "sales_qty"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)

scaling_pipeline = Pipeline(stages=[assembler, scaler])
scaler_model = scaling_pipeline.fit(processed_df)
scaled_df = scaler_model.transform(processed_df)

print(f"Original Row Count: {raw_df.count()}")
print(f"Final Processed Row Count: {scaled_df.count()}")
print("--- Final Processed Data Schema and Sample ---")
scaled_df.printSchema()
scaled_df.show(truncate=False)
