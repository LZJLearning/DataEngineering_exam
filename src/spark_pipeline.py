import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from pyspark.sql.functions import (
    col, from_json, current_timestamp, year, month, dayofmonth, hour,
    when, window, avg, min as spark_min, max as spark_max, count, sum as spark_sum
)

# Windows environment repair
os.environ['HADOOP_HOME'] = r"D:\University\DataEngineering\ZIJIAN_LIANG_exam\hadoop"
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r"\bin;" + os.environ['PATH']

def main():
    # 1. initialization Spark Session
    spark = SparkSession.builder \
        .spark = SparkSession.builder \
        .appName("AeroSense_IoT_Pipeline") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Data Lake Root Directory
    LAKE_ROOT = "./tmp/datalake"
    CHECKPOINT_ROOT = "./tmp/checkpoints"

    # 2. JSON Schema 
    schema = StructType([
        StructField("sensor", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("anomaly", BooleanType(), True)
    ])

    # 3. read Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor-events") \
        .option("startingOffsets", "earliest") \
        .load()

    # Zone 1: Raw Zone
    raw_zone_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as raw_json") \
        .withColumn("ingest_time", current_timestamp()) \
        .withColumn("year", year("ingest_time")) \
        .withColumn("month", month("ingest_time")) \
        .withColumn("day", dayofmonth("ingest_time")) \
        .withColumn("hour", hour("ingest_time"))

    raw_query = raw_zone_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .partitionBy("year", "month", "day", "hour") \
        .option("path", f"{LAKE_ROOT}/raw/source=kafka/topic=sensor-events") \
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw") \
        .start()

    # Zone 2: Curated Zone
    parsed_df = raw_kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    event_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    cleaned_df = event_df.filter(
        ((col("sensor") == "temperature") & (col("value").between(-50, 100))) |
        ((col("sensor") == "humidity") & (col("value").between(0, 100))) |
        ((col("sensor") == "pressure") & (col("value").between(800, 1200)))
    )

    curated_df = cleaned_df.withColumn(
        "is_anomaly",
        when((col("sensor") == "temperature") & (col("value") > 35.0), True)
        .when((col("sensor") == "humidity") & (col("value") > 90.0), True)
        .when((col("sensor") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0)), True)
        .otherwise(False)
    ).withColumn("year", year("event_time")) \
     .withColumn("month", month("event_time")) \
     .withColumn("day", dayofmonth("event_time"))

    curated_query = curated_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .partitionBy("sensor", "year", "month", "day") \
        .option("path", f"{LAKE_ROOT}/curated/domain=iot") \
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/curated") \
        .start()

    # Zone 3: Consumption Zone
    consumption_df = curated_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window("event_time", "5 minutes"), "sensor", "year", "month") \
        .agg(
            spark_min("value").alias("min_val"),
            spark_max("value").alias("max_val"),
            avg("value").alias("avg_val"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count")
        )

    # use Append
    consumption_query = consumption_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .partitionBy("sensor", "year", "month") \
        .option("path", f"{LAKE_ROOT}/consumption/use_case=sensor_averages") \
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/consumption") \
        .start()

    print("Spark Streaming Pipeline is running")
    
    # stop
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()