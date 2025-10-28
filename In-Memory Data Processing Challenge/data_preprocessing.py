from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import time

spark = SparkSession.builder \
    .appName("InMemoryProcessingChallenge") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DATA_SIZE = 10_000_000

print(f"Generating large DataFrame with {DATA_SIZE} rows...")

data_df = spark.range(DATA_SIZE).select(
    col("id"),
    (rand() * 100).alias("value_a"),
    (rand() * 100).alias("value_b"),
    (rand() * 10).cast("int").alias("category")
)

def run_analysis(df, description):
    start_time = time.time()
    
    result = df.withColumn("combined_value", col("value_a") + col("value_b")) \
               .groupBy("category") \
               .agg({"combined_value": "avg", "id": "count"}) \
               .collect() 
               
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n--- Analysis Run: {description} ---")
    print(f"Time taken: {duration:.4f} seconds")
    
    return duration

time_no_cache_1 = run_analysis(data_df, "Run 1 (No Cache)")

time_no_cache_2 = run_analysis(data_df, "Run 2 (No Cache - Recompute)")


print("\n--- Applying In-Memory Caching (df.cache()) ---")
data_df.cache().count()

time_with_cache_1 = run_analysis(data_df, "Run 3 (With Cache - First Access)")

time_with_cache_2 = run_analysis(data_df, "Run 4 (With Cache - Memory Read)")


print("\n=======================================================")
print("  IN-MEMORY PROCESSING PERFORMANCE IMPROVEMENT")
print("=======================================================")
print(f"Non-Cached Recompute Time: {time_no_cache_2:.4f} seconds")
print(f"Cached Memory Read Time:   {time_with_cache_2:.4f} seconds")

if time_no_cache_2 > 0 and time_with_cache_2 > 0:
    speed_up = time_no_cache_2 / time_with_cache_2
    print(f"\nPerformance Speed Up (Run 2 vs Run 4): {speed_up:.2f}x")
else:
    print("\nError: Cannot calculate speed-up (Time is zero or negative).")

data_df.unpersist()
spark.stop()