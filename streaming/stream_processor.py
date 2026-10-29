"""
Reads trades from Kafka, aggregates them into 1-minute candles per symbol,
and writes Parquet files to Azure Data Lake Storage Gen2, partitioned by trade_date.
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")
STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
CONTAINER = os.getenv("AZURE_CONTAINER", "lake")
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

ACCOUNT_HOST = f"{STORAGE_ACCOUNT}.dfs.core.windows.net"
LAKE_ROOT = f"abfss://{CONTAINER}@{ACCOUNT_HOST}"
OUTPUT_PATH = f"{LAKE_ROOT}/trades_1min"
CHECKPOINT_PATH = f"{LAKE_ROOT}/checkpoints/trades_1min"

spark = (
    SparkSession.builder
    .appName("crypto-trades-1min")
    # Log in to the data lake as the service principal (OAuth client credentials)
    .config(f"spark.hadoop.fs.azure.account.auth.type.{ACCOUNT_HOST}", "OAuth")
    .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{ACCOUNT_HOST}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{ACCOUNT_HOST}", CLIENT_ID)
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{ACCOUNT_HOST}", CLIENT_SECRET)
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{ACCOUNT_HOST}",
            f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")   # small data -> few partitions
    # Standard Parquet timestamps, which Synapse serverless reads as DATETIME2
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Shape of each Kafka message value (what producer.py sends)
trade_schema = StructType([
    StructField("trade_id", LongType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_time", LongType()),
    StructField("side", StringType()),
])

# 1. Read from Kafka (each row has binary key/value columns)
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 2. Parse JSON into columns and add event time
trades = (
    raw.select(F.from_json(F.col("value").cast("string"), trade_schema).alias("t"))
    .select("t.*")
    .where(F.col("symbol").isNotNull())
    .withColumn("event_time", (F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("notional", F.col("price") * F.col("quantity"))
)

# 3. Aggregate into 1-minute windows per symbol
candles = (
    trades
    .withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
    .agg(
        F.min_by("price", "event_time").alias("open_price"),
        F.max("price").alias("high_price"),
        F.min("price").alias("low_price"),
        F.max_by("price", "event_time").alias("close_price"),
        F.sum("quantity").alias("volume"),
        F.sum("notional").alias("quote_volume"),
        F.count("*").alias("trade_count"),
        F.sum(F.when(F.col("side") == "buy", F.col("notional")).otherwise(0.0)).alias("buy_quote_volume"),
        F.sum(F.when(F.col("side") == "sell", F.col("notional")).otherwise(0.0)).alias("sell_quote_volume"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "open_price", "high_price", "low_price", "close_price",
        "volume", "quote_volume", "trade_count",
        "buy_quote_volume", "sell_quote_volume",
        F.to_date(F.col("window.start")).alias("trade_date"),
    )
    # One file per micro-batch instead of up to 4: Synapse serverless reads
    # fewer, larger files faster.
    .coalesce(1)
)

# 4. Write finished windows to the data lake
query = (
    candles.writeStream
    .format("parquet")
    .outputMode("append")              # only write a window once it's final
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("trade_date")         # one folder per day
    .trigger(processingTime="1 minute")
    .start()
)

# Log level is WARN (Spark's INFO is very noisy), so print one line per finished batch
# to make progress visible in `docker compose logs spark`.
last_batch = None
while query.isActive:
    progress = query.lastProgress
    if progress and progress["batchId"] != last_batch:
        last_batch = progress["batchId"]
        # (the file sink doesn't report rows written, so only input is shown)
        print(f"batch {progress['batchId']}: {progress['numInputRows']} trades in, "
              f"watermark {progress.get('eventTime', {}).get('watermark', '-')}", flush=True)
    time.sleep(10)

query.awaitTermination()   # re-raises the error if the query stopped because of one
