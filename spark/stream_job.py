"""Spark Structured Streaming job: Kafka -> 5-second OHLC/VWAP -> (step 4) console.

Reads raw aggregated trades from Kafka, parses them with an explicit schema and
folds them into one row per symbol per 5-second window: trade count, volume,
VWAP and the four OHLC prices.

Step 5 replaces the console sink with MongoDB + output/live_ohlc.jsonl.

Runs inside the container defined as the `spark` service, so it talks to the
broker over the INTERNAL listener (kafka:9092).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import max_by
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import min_by
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import timestamp_millis, window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance.trades.raw")

WINDOW_DURATION = "5 seconds"
# How long to keep a window open for late arrivals. The producer polls every
# 2 seconds, so 30 seconds is generous; it also caps how much state Spark has
# to keep in memory.
WATERMARK_DELAY = "30 seconds"

# Explicit schema instead of inference: schema inference is not available for
# streaming sources, and pinning the types here means a malformed record turns
# into nulls rather than breaking the query.
TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_id", LongType()),
    StructField("price", DoubleType()),
    StructField("qty", DoubleType()),
    StructField("quote_qty", DoubleType()),
    StructField("trade_time", LongType()),
    StructField("is_buyer_maker", BooleanType()),
    StructField("ingest_time", LongType()),
])

spark = (
    SparkSession.builder
    .appName("binance-stream")
    .getOrCreate()
)
# Spark's INFO logging drowns out the streaming output; warnings still show.
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    # Read the topic from the beginning. This option only applies to a query
    # that has no checkpoint yet; once step 5 adds a checkpoint the stream
    # resumes from its stored offsets instead.
    .option("startingOffsets", "earliest")
    .load()
)

trades = (
    raw
    .select(from_json(col("value").cast("string"), TRADE_SCHEMA).alias("data"))
    # from_json is PERMISSIVE by default: an unparseable record yields nulls
    # instead of failing the batch. Drop those rather than aggregating them.
    .filter(col("data.trade_id").isNotNull())
    .select(
        col("data.symbol").alias("symbol"),
        col("data.trade_id").alias("trade_id"),
        col("data.price").alias("price"),
        col("data.qty").alias("qty"),
        col("data.quote_qty").alias("quote_qty"),
        # Epoch milliseconds -> real timestamp, the event time of the stream
        timestamp_millis(col("data.trade_time")).alias("trade_time"),
    )
)

ohlc = (
    trades
    .withWatermark("trade_time", WATERMARK_DELAY)
    .groupBy(window(col("trade_time"), WINDOW_DURATION), col("symbol"))
    .agg(
        count("*").alias("trade_count"),
        spark_sum("qty").alias("volume"),
        # quote_qty is price * qty, already computed by the producer
        spark_sum("quote_qty").alias("quote_volume"),
        spark_max("price").alias("high"),
        spark_min("price").alias("low"),
        # Open and close prices. Ordering by trade_id rather than by
        # trade_time: ids increase strictly per symbol, while several trades
        # can share the same millisecond, which would make the pick ambiguous.
        min_by("price", "trade_id").alias("first_price"),
        max_by("price", "trade_id").alias("last_price"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("trade_count"),
        col("volume"),
        col("quote_volume"),
        # Volume weighted average price: what one unit really traded at across
        # the window, unlike a plain average of the prices.
        (col("quote_volume") / col("volume")).alias("vwap"),
        col("high"),
        col("low"),
        col("first_price"),
        col("last_price"),
    )
)

query = (
    ohlc.writeStream
    .format("console")
    # append needs the watermark: a window is emitted once only after the
    # watermark has passed its end, at which point the row is final.
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
