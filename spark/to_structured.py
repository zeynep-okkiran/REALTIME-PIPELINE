"""Batch job: MongoDB documents -> structured, schema-enforced Parquet.

MongoDB is schemaless. The documents written by stream_job.py look regular, but
nothing in Mongo guarantees that: a document with a string in `vwap` would be
accepted. That makes the collection semi-structured.

This job closes that gap. It reads the collection, forces every field through an
explicit schema, derives one computed column, checks the values against the
constraints the data is supposed to satisfy, and writes Parquet - a columnar
format that carries its schema inside the file, so anything reading it back gets
the types without being told.

Run it on demand, separately from the streaming job:

    docker compose run --rm --no-deps spark \
        /opt/spark/bin/spark-submit /app/to_structured.py
"""

import csv
import os

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "realtime")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "trade_ohlc")
STRUCTURED_DIR = os.environ.get("STRUCTURED_DIR", "/app/output/structured")

# The data contract. Every column has one type and one meaning, and the order
# here is the order the rows are built in below - nothing is inferred.
#
# column          type       unit / meaning
# symbol          string     trading pair, e.g. BTCUSDT
# window_start    timestamp  inclusive start of the 5-second window (UTC)
# window_end      timestamp  exclusive end of the window (UTC)
# trade_count     long       number of aggregated trades in the window
# volume          double     base asset traded, e.g. BTC
# quote_volume    double     quote asset traded, e.g. USDT
# vwap            double     quote_volume / volume
# high / low      double     highest / lowest trade price
# first_price     double     price of the earliest trade (open)
# last_price      double     price of the latest trade (close)
STRUCTURED_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("window_start", TimestampType(), nullable=False),
    StructField("window_end", TimestampType(), nullable=False),
    StructField("trade_count", LongType(), nullable=False),
    StructField("volume", DoubleType(), nullable=False),
    StructField("quote_volume", DoubleType(), nullable=False),
    StructField("vwap", DoubleType(), nullable=False),
    StructField("high", DoubleType(), nullable=False),
    StructField("low", DoubleType(), nullable=False),
    StructField("first_price", DoubleType(), nullable=False),
    StructField("last_price", DoubleType(), nullable=False),
])

COLUMNS = [field.name for field in STRUCTURED_SCHEMA.fields]

# What the numbers have to satisfy to be believable. Anything failing these is a
# bug upstream, not something to silently pass on.
CONSTRAINTS = {
    "trade_count > 0": "trade_count > 0",
    "volume > 0": "volume > 0",
    "vwap within [low, high]": "vwap >= low - 1e-9 AND vwap <= high + 1e-9",
    "open within [low, high]": "first_price >= low AND first_price <= high",
    "close within [low, high]": "last_price >= low AND last_price <= high",
    "high >= low": "high >= low",
    "window is 5 seconds": "unix_timestamp(window_end) - unix_timestamp(window_start) = 5",
}


def read_documents():
    """Pull the collection into plain tuples, in schema order.

    _id is dropped on purpose: it is a composite string built for idempotent
    upserts, which is a storage concern. The natural key of the structured table
    is (symbol, window_start).
    """
    collection = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLLECTION]
    documents = list(collection.find({}, {"_id": 0}))
    return [tuple(doc.get(name) for name in COLUMNS) for doc in documents]


def main():
    spark = SparkSession.builder.appName("to-structured").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    rows = read_documents()
    print(f"read {len(rows)} documents from {MONGO_DB}.{MONGO_COLLECTION}")
    if not rows:
        print("nothing to transform - run the pipeline first")
        return 1

    # Applying the schema is the transformation: from here on the types are
    # fixed, and a document that did not fit would have failed right here.
    df = spark.createDataFrame(rows, schema=STRUCTURED_SCHEMA)

    # One derived column, to show the structured table can carry computed
    # measures rather than only what Mongo happened to store.
    df = df.withColumn(
        "price_change_pct",
        spark_round(((col("last_price") - col("first_price")) / col("first_price")) * 100, 4),
    )

    print("\n--- enforced schema ---")
    df.printSchema()

    print("--- constraint checks ---")
    total = df.count()
    failed = 0
    for label, expression in CONSTRAINTS.items():
        violations = df.filter(f"NOT ({expression})").count()
        failed += violations
        print(f"  {label:<28} {violations} violation(s) of {total}")

    # Partitioned by symbol: each symbol becomes its own directory, so a reader
    # asking for one symbol never opens the others' files.
    #
    # repartition("symbol") first, so each symbol is written by a single task
    # and lands in one file. Without it every task writes its own fragment into
    # every partition directory - the "small files problem", which costs more in
    # metadata lookups than the data itself is worth.
    parquet_path = f"{STRUCTURED_DIR}/trade_ohlc.parquet"
    df.repartition("symbol").write.mode("overwrite").partitionBy("symbol").parquet(parquet_path)
    print(f"\nwrote Parquet to {parquet_path} (partitioned by symbol)")

    # A CSV as well, for reading by eye. Written from the driver rather than
    # through df.write.csv(): Spark's writer always produces a *directory*, with
    # a part-00000-<uuid> file, a _SUCCESS marker and a hidden .crc checksum per
    # file. None of that helps someone who just wants to open one spreadsheet.
    # Safe to collect at this scale - the aggregation already reduced the data to
    # a few thousand rows. A much larger table would want the Spark writer back.
    csv_path = f"{STRUCTURED_DIR}/trade_ohlc.csv"
    columns = df.columns
    with open(csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in df.collect():
            writer.writerow([row[name] for name in columns])
    print(f"wrote CSV to {csv_path}")

    # Read it back to prove the schema travelled with the file: nothing here
    # tells Spark what the types are.
    restored = spark.read.parquet(parquet_path)
    print("\n--- schema read back from the Parquet file itself ---")
    restored.printSchema()

    # SQL is the proof of being structured: a schemaless document store cannot
    # answer this without the reader knowing the shape in advance.
    restored.createOrReplaceTempView("trade_ohlc")

    print("--- per symbol summary ---")
    spark.sql("""
        SELECT symbol,
               COUNT(*)                        AS windows,
               SUM(trade_count)                AS trades,
               ROUND(AVG(vwap), 2)             AS avg_vwap,
               ROUND(MAX(high), 2)             AS highest,
               ROUND(MIN(low), 2)              AS lowest,
               ROUND(SUM(quote_volume), 2)     AS quote_volume
        FROM trade_ohlc
        GROUP BY symbol
        ORDER BY trades DESC
    """).show(truncate=False)

    print("--- most volatile windows ---")
    spark.sql("""
        SELECT symbol, window_start, trade_count, first_price, last_price, price_change_pct
        FROM trade_ohlc
        ORDER BY ABS(price_change_pct) DESC
        LIMIT 5
    """).show(truncate=False)

    print(f"done. {total} rows, {failed} constraint violation(s)")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
