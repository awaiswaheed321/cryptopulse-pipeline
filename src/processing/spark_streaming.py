import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, sum as spark_sum, max as spark_max, min as spark_min, when 
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv


from src.processing.schemas import get_binance_schema
from src.processing.sinks import write_to_cassandra

load_dotenv()
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'crypto_trades')

def create_spark_session():
    builder: SparkSession.Builder = SparkSession.builder # type: ignore
    return (
        builder
        .appName("CryptoPulse_RealTime_Aggregator")
        .master("local[*]")
        .getOrCreate()
    )

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Engine initialized. Reading from Kafka...")

    # READ THE STATIC CSV DATA FROM HDFS
    # HDFS_NAMENODE env var allows the start script to pass the container IP when
    # running Spark on the host (where Docker's internal DNS doesn't resolve).
    hdfs_namenode = os.environ.get('HDFS_NAMENODE', 'namenode:9000')
    static_metadata_df = spark.read \
        .option("header", "true") \
        .csv(f"hdfs://{hdfs_namenode}/user/data/crypto_metadata.csv")

    # READ FROM KAFKA
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # PARSE THE JSON (Using our external schema)
    schema = get_binance_schema()
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # TYPE CASTING
    cleaned_df = parsed_df \
        .withColumn("price", col("p").cast(DoubleType())) \
        .withColumn("quantity", col("q").cast(DoubleType())) \
        .withColumn("trade_timestamp", (col("T") / 1000).cast("timestamp"))

    # WINDOWED AGGREGATION
    aggregated_df = cleaned_df \
        .withWatermark("trade_timestamp", "1 minute") \
        .groupBy(window(col("trade_timestamp"), "1 minute"), col("s").alias("symbol")) \
        .agg(
            avg("price").alias("average_price"),
            spark_sum("quantity").alias("total_volume"),
            spark_max("price").alias("high_price"), # Track the highest price in this minute
            spark_min("price").alias("low_price") ,  # Track the lowest price in this minute
        )
    
    final_df = aggregated_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # ANOMALY DETECTION (Streaming-Safe Math)
    # Calculate the % swing between the highest and lowest price within the 60 seconds
    anomaly_df = final_df \
        .withColumn("price_swing_pct", ((col("high_price") - col("low_price")) / col("low_price")) * 100) \
        .withColumn("is_anomaly", 
            when(col("price_swing_pct") > 1.5, True) # If price swings > 1.5% in 60s, flag True
            .otherwise(False)
        )
    
    # THE STREAM-STATIC JOIN (Spark SQL)
    anomaly_df.createOrReplaceTempView("aggregates")
    static_metadata_df.createOrReplaceTempView("metadata")
    enriched_df = spark.sql("""
        SELECT
            a.window_start,
            a.window_end,
            a.symbol,
            a.average_price,
            a.total_volume,
            a.high_price,
            a.low_price,
            a.price_swing_pct,
            a.is_anomaly,
            m.asset_name,
            m.category
        FROM aggregates a
        LEFT JOIN metadata m ON a.symbol = m.symbol
    """)

    # OUTPUT TO CASSANDRA (Using our external sink)
    query = enriched_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/spark_checkpoints_crypto") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()