"""Spark Structured Streaming job: Kafka -> 5-second OHLC/VWAP -> Mongo + JSONL.

Reads raw aggregated trades from Kafka, parses them with an explicit schema and
folds them into one row per symbol per 5-second window: trade count, volume,
VWAP and the four OHLC prices. Each finished window is written to MongoDB and
appended to a JSON Lines file.

The JSON Lines file keeps only the most recent JSONL_MAX_LINES rows. It is a
live tail for watching the stream by eye, not the record of truth - MongoDB is.
Left unbounded it grows at roughly 560 KB/hour, about 400 MB a month, for data
that is already stored properly next door. Trimming happens in place once the
file overshoots the limit by JSONL_TRIM_SLACK, not on every batch: rewriting
megabytes every five seconds would cost more than the file is worth. A reader
tailing the file will see it jump at that moment, which is the accepted
trade-off.

Runs inside the container defined as the `spark` service, so it talks to the
broker over the INTERNAL listener (kafka:9092).
"""

import json
import os
from collections import deque

from pymongo import MongoClient, UpdateOne
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

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "realtime")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "trade_ohlc")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/app/output/live_ohlc.jsonl")
# How many lines the JSONL tail keeps. At three windows per five seconds this is
# a few hours of history in a couple of megabytes.
JSONL_MAX_LINES = int(os.environ.get("JSONL_MAX_LINES", "10000"))
# Allowed overshoot before a trim runs, so the rewrite happens every few
# thousand lines instead of every batch.
JSONL_TRIM_SLACK = 1000
# Where Spark stores the committed Kafka offsets and the window state. With it
# the query resumes where it left off instead of replaying the whole topic.
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/checkpoint/ohlc")

WINDOW_DURATION = "5 seconds"
# How long to keep a window open for late arrivals, and therefore the main
# component of end-to-end latency: a window is only emitted this long after it
# closes. The producer walks fromId sequentially and polls every 2 seconds, so
# records arrive in order and barely late; 10 seconds is five times that
# headroom. The one case it does not cover is a symbol whose polling backs off
# for longer than this while the other symbols keep advancing the watermark --
# those trades would be dropped from their window.
WATERMARK_DELAY = "10 seconds"

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
    # that has no checkpoint yet; with a checkpoint the stream resumes from its
    # stored offsets instead.
    .option("startingOffsets", "earliest")
    # Do not kill the query when a committed offset is gone from Kafka - skip to
    # the earliest one still available and carry on. Without this, anything that
    # truncates the broker's log while the checkpoint survives (retention
    # deleting old segments, or a wiped broker) fails the query permanently, and
    # with restart: unless-stopped that becomes a crash loop. Mongo upserts are
    # idempotent, so re-reading whatever is left costs nothing.
    .option("failOnDataLoss", "false")
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

# foreachBatch runs on the driver, so a single client here is enough and is
# reused across batches.
collection = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLLECTION]

# Line count of the JSONL tail, so a trim decision costs nothing per batch.
# Counted from the file once, on the first write after startup.
jsonl_lines = None


def append_to_tail(rows):
    """Append rows to the JSONL tail and trim it back when it overshoots."""
    global jsonl_lines
    if jsonl_lines is None:
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as handle:
                jsonl_lines = sum(1 for _ in handle)
        except FileNotFoundError:
            jsonl_lines = 0

    with open(OUTPUT_PATH, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str) + "\n")
    jsonl_lines += len(rows)

    if jsonl_lines <= JSONL_MAX_LINES + JSONL_TRIM_SLACK:
        return

    # deque with maxlen keeps only the tail in memory, whatever the file size.
    with open(OUTPUT_PATH, "r", encoding="utf-8") as handle:
        kept = deque(handle, maxlen=JSONL_MAX_LINES)
    # Rewritten in place rather than through a temp file and rename: this is a
    # throwaway tail, and a brief partial read is cheaper than the extra
    # failure modes a rename across a bind mount can introduce.
    with open(OUTPUT_PATH, "w", encoding="utf-8") as handle:
        handle.writelines(kept)
    jsonl_lines = len(kept)
    print(f"trimmed the JSONL tail to its last {jsonl_lines} lines", flush=True)


def write_batch(batch_df, batch_id):
    """Write one micro-batch to both sinks.

    A single foreachBatch instead of two separate queries: the topic would
    otherwise be read twice and the windows aggregated twice.
    """
    # Safe to collect: the aggregation already reduced each batch to a handful
    # of rows, at most one per symbol per window.
    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    if not rows:
        return

    # Mongo, idempotent. foreachBatch is at-least-once: after a failure Spark
    # may replay a batch. Keying on symbol + window start turns that into an
    # overwrite of the same document rather than a duplicate.
    collection.bulk_write(
        [
            UpdateOne(
                {"_id": f"{row['symbol']}|{row['window_start'].isoformat()}"},
                {"$set": row},
                upsert=True,
            )
            for row in rows
        ],
        ordered=False,
    )

    # JSON Lines tail, bounded. Deliberately not writeStream.format("json"): the
    # built-in sink spreads output over many part-*.json files, while what is
    # wanted here is one readable file that grows live. Note this file is the
    # one place a replayed batch does show up twice; Mongo stays correct.
    append_to_tail(rows)

    print(f"batch {batch_id}: wrote {len(rows)} windows", flush=True)


query = (
    ohlc.writeStream
    # Names the query in the Spark UI's Structured Streaming tab
    .queryName("binance-ohlc")
    .foreachBatch(write_batch)
    # append needs the watermark: a window is emitted once only after the
    # watermark has passed its end, at which point the row is final.
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
