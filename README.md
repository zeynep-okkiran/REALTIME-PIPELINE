# Real-Time Trade Pipeline

Streams live trades from the Binance public REST API, aggregates them into
5-second OHLC/VWAP windows with Spark Structured Streaming, and writes every
finished window to MongoDB and to a JSON Lines file.

Everything runs in containers. Moving the stack to another machine is
"copy the folder, run `docker compose up -d`".

```
Binance REST /api/v3/aggTrades
      │   producer.py - polls every 2s, fromId watermark, no duplicates
      ▼
Kafka  topic: binance.trades.raw    key=symbol, 3 partitions
      │
      ▼
Spark Structured Streaming
      │   5-second tumbling window on event time, 10s watermark
      │   trade_count, volume, quote_volume, VWAP, open/high/low/close
      │
      ├──► MongoDB  realtime.trade_ohlc   _id = symbol|window_start (idempotent upsert)
      │         │
      │         └──► to_structured.py — schema-enforced Parquet, run on demand
      └──► output/live_ohlc.jsonl         append-only, one JSON object per line
```

`CLAUDE.md` is the full reference: architecture, every design decision and the
reason behind it, measured numbers, and the traps met along the way.

## Requirements

Docker Desktop (or Docker Engine + Compose v2). Nothing else — Kafka, Spark,
MongoDB and Python all live inside the images.

## Quick start

```bash
cp .env.example .env          # Windows: Copy-Item .env.example .env
docker compose up -d --build
```

First run pulls ~2.5 GB of images and downloads the Spark Kafka connector into
`./.ivy`; later runs reuse both. After about a minute:

```bash
# Windows PowerShell
Get-Content output\live_ohlc.jsonl -Tail 5 -Wait

# Linux / macOS
tail -f output/live_ohlc.jsonl
```

Lines appear three at a time — one per symbol — every 5 seconds.

## What you get

| Where | What |
|---|---|
| `output/live_ohlc.jsonl` | One JSON object per finished window, appended live. A bounded tail — the newest `JSONL_MAX_LINES` rows, since MongoDB is the record of truth |
| MongoDB `realtime.trade_ohlc` | The same windows, keyed for idempotent upsert |
| `output/structured/` | Parquet + CSV with an enforced schema, once you run the batch job below |
| http://localhost:8080 | Kafka UI — browse raw messages, partitions, broker health |
| http://localhost:4040 | Spark UI — the `binance-ohlc` query, batch durations, input rate |

One output row:

```json
{"window_start": "2026-08-14 12:45:15", "window_end": "2026-08-14 12:45:20",
 "symbol": "BTCUSDT", "trade_count": 105, "volume": 2.25352,
 "quote_volume": 141439.2327852, "vwap": 62763.69093027798,
 "high": 62774.0, "low": 62761.9,
 "first_price": 62761.91, "last_price": 62773.99}
```

`first_price`, `high`, `low` and `last_price` are the OHLC candle for that
window. `vwap` is the volume weighted average price: total quote volume divided
by total volume, so a large trade moves it more than a small one does.

## Configuration

All of it lives in `.env`:

| Variable | Default | Meaning |
|---|---|---|
| `SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | Symbols to poll, comma separated |
| `POLL_INTERVAL_SEC` | `2` | Seconds between Binance polls |
| `KAFKA_TOPIC` | `binance.trades.raw` | Topic name |
| `KAFKA_PARTITIONS` | `3` | Partition count, applied at topic creation |
| `KAFKA_EXTERNAL_PORT` | `29092` | Broker port for clients on the host |
| `MONGO_PORT` | `27018` | Host port for Mongo (27017 is left alone) |
| `MONGO_DB` / `MONGO_COLLECTION` | `realtime` / `trade_ohlc` | Write target |
| `JSONL_MAX_LINES` | `10000` | Lines the JSONL tail keeps before older ones are trimmed |
| `KAFKA_UI_PORT` / `SPARK_UI_PORT` | `8080` / `4040` | Web UIs |

`.env` is not tracked. Adding a variable means adding it to `.env.example` too.

## Commands

```bash
docker compose up -d                 # start
docker compose ps                    # service status
docker compose logs -f spark         # follow the Spark job
docker compose logs -f producer      # follow the Binance poller
docker compose restart spark         # pick up an edit to spark/stream_job.py
docker compose stop                  # stop, keep all data
docker compose down                  # remove containers, keep volumes
docker compose down -v               # remove volumes too: the Kafka log, Mongo
                                     # data and the Spark checkpoint all go
```

`spark/stream_job.py` is bind-mounted, so editing it needs only a restart, not
a rebuild. `producer.py` is baked into its image and needs
`docker compose up -d --build producer`.

### Mongo to structured Parquet

MongoDB is schemaless, so what the pipeline writes there is still
semi-structured. A separate batch job forces it through an explicit schema,
checks it against the constraints the data should satisfy, and writes Parquet
partitioned by symbol:

```bash
docker compose run --rm --no-deps spark \
    /opt/spark/bin/spark-submit /app/to_structured.py
```

Output lands in `output/structured/` as Parquet and CSV, and the job exits
non-zero if any constraint is violated. `CLAUDE.md` walks one real API call
through every format on the way there.

### Starting over

```bash
docker compose down -v
rm -rf output                        # Windows: Remove-Item output -Recurse
docker compose up -d
```

## Layout

```
docker-compose.yml     six services: kafka, kafka-init, producer, spark, mongo, kafka-ui
.env / .env.example    all configuration
CLAUDE.md              full reference: architecture, every design decision and why,
                       measured numbers, known traps, environment facts
producer/
  producer.py          Binance REST -> Kafka
  requirements.txt     requests, confluent-kafka
  Dockerfile
spark/
  stream_job.py        Kafka -> 5s OHLC/VWAP -> Mongo + JSONL (streaming)
  to_structured.py     Mongo -> schema-enforced Parquet (batch, on demand)
  Dockerfile           apache/spark plus pymongo
output/                live_ohlc.jsonl and structured/ (gitignored)
```

State lives in three named volumes, not in the repo: `kafka-data` (the broker's
log), `mongo-data` and `spark-checkpoint`. They are deleted together by
`down -v` and by nothing else — the checkpoint records Kafka offsets, so
removing one without the other leaves Spark asking for offsets that no longer
exist.

## Design notes

**No trade is published twice.** The producer keeps a `fromId` watermark per
symbol and asks Binance only for what comes after it. On start it reads the
single latest trade to seed that watermark, so a restart resumes live instead
of replaying history.

**Records are keyed by symbol.** All trades of one symbol land on the same
partition, which is what preserves their order end to end.

**Windows are cut on event time, not arrival time.** A trade carries the
timestamp Binance gave it, so a record delayed in transit still lands in the
window it belongs to. The 10-second watermark is how long a window waits for
late arrivals before it is finalised — and the largest single component of
end-to-end latency, which measures at about 26 seconds.

**Mongo writes are idempotent.** `foreachBatch` is at-least-once, so a replayed
batch would write the same window again; keying documents on
`symbol|window_start` turns that into an overwrite. The JSONL file is the one
place a replay does show up twice, by design — it is a live tail, not a record
of truth.

**The checkpoint is a named volume, not a bind mount.** Spark rewrites
offset and state files on every micro-batch, and doing that across the Windows
filesystem boundary pushed batches from 5s to 8-13s.

**Spark runs in a container, not on the host.** PySpark checkpointing on
Windows wants `winutils.exe`/`hadoop.dll`, which would defeat the portability
goal.

## Notes for Linux

Both application images run unprivileged — Spark as uid 185, the producer as
uid 1000 — so nothing in the bind-mounted `output/` ends up owned by root. If
you want the files owned by your own user instead:

```bash
sudo chown -R "$USER" output
```

Ports 8080, 4040, 27018 and 29092 must be free, or change them in `.env`.
