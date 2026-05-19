import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, mean, min, max, stddev, sum, count, desc

# Windows
os.environ['HADOOP_HOME'] = r"D:\University\DataEngineering\ZIJIAN_LIANG_exam\hadoop"
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r"\bin;" + os.environ['PATH']

def main():
    # 1. Spark Session
    spark = SparkSession.builder \
        .appName("AeroSense_Analytics") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Output directory
    os.makedirs("./outputs/analytics", exist_ok=True)

    print(" Loading Curated Data Lake")
    # read Curated data
    curated_df = spark.read.parquet("./tmp/datalake/curated/domain=iot")
    curated_df.createOrReplaceTempView("curated_sensor_data")

    # Query 1: Top 5 hours with the highest number of anomalies
    print("\n--- Query 1: Top 5 Hours with Highest Anomalies ---")
    q1_df = curated_df.filter(col("is_anomaly") == True) \
        .withColumn("hour", hour("event_time")) \
        .groupBy("year", "month", "day", "hour") \
        .count() \
        .orderBy(desc("count")) \
        .limit(5)
    
    q1_df.show()
    # save into CSV
    q1_df.toPandas().to_csv("./outputs/analytics/q1_top_anomaly_hours.csv", index=False)

    # Query 2: Global statistics for each sensor type
    print("\n--- Query 2: Global Statistics per Sensor Type ---")
    q2_df = curated_df.groupBy("sensor").agg(
        mean("value").alias("mean_val"),
        min("value").alias("min_val"),
        max("value").alias("max_val"),
        stddev("value").alias("stddev_val"),
        ((sum(col("is_anomaly").cast("int")) / count("*")) * 100).alias("anomaly_rate_pct")
    )
    
    q2_df.show()
    q2_df.toPandas().to_csv("./outputs/analytics/q2_sensor_global_stats.csv", index=False)

    # Query 3: Daily evolution of mean and anomalies for Temperature
    print("\n--- Query 3: Daily Evolution for Temperature ---")
    q3_df = curated_df.filter(col("sensor") == "temperature") \
        .groupBy("year", "month", "day") \
        .agg(
            mean("value").alias("daily_mean"),
            sum(col("is_anomaly").cast("int")).alias("daily_anomalies")
        ).orderBy("year", "month", "day")
    
    q3_df.show()
    q3_df.toPandas().to_csv("./outputs/analytics/q3_temperature_daily_evolution.csv", index=False)

    # Query 4: Partition Pruning Demonstration
    print("\n--- Query 4: Partition Pruning Demonstration ---")
    
    # Experiment A: Using standard equality lookup
    start_time = time.time()
    pruned_count = spark.sql("SELECT COUNT(*) FROM curated_sensor_data WHERE sensor = 'temperature'").collect()[0][0]
    pruned_duration = time.time() - start_time
    
    # Experiment B: Wrapping partition columns with functions to intentionally disrupt partition pruning.
    start_time = time.time()
    unpruned_count = spark.sql("SELECT COUNT(*) FROM curated_sensor_data WHERE lower(sensor) = 'temperature'").collect()[0][0]
    unpruned_duration = time.time() - start_time

    speedup = unpruned_duration / pruned_duration if pruned_duration > 0 else float('inf')

    print(f"Count Result (Pruned): {pruned_count}")
    print(f"Count Result (Unpruned): {unpruned_count}")
    print(f"Execution Time WITH Pruning:    {pruned_duration:.4f} seconds")
    print(f"Execution Time WITHOUT Pruning: {unpruned_duration:.4f} seconds")
    print(f" Speedup Factor: {speedup:.2f}x")

    # Simply write the results to a txt file instead of a CSV file.
    with open("./outputs/analytics/q4_pruning_demo.txt", "w") as f:
        f.write(f"Pruned Time: {pruned_duration:.4f}s\n")
        f.write(f"Unpruned Time: {unpruned_duration:.4f}s\n")
        f.write(f"Speedup: {speedup:.2f}x\n")

if __name__ == "__main__":
    main()