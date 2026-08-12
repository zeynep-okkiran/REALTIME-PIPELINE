"""Spark Structured Streaming job: Kafka -> (step 3) console.

Step 3 deliberately does no transformation. It reads the raw Kafka records,
casts key/value to string and prints them, so that the Spark <-> Kafka
connection is proven before any parsing or aggregation logic is added on top.

Runs inside the container defined as the `spark` service, so it talks to the
broker over the INTERNAL listener (kafka:9092).
"""

import os

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance.trades.raw")

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

records = raw.selectExpr(
    "CAST(key AS STRING) AS symbol",
    "CAST(value AS STRING) AS payload",
    "partition",
    "offset",
    "timestamp AS kafka_time",
)

query = (
    records.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 5)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
