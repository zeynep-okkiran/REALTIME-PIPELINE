# CLAUDE.md — Real-Time Trade Pipeline

This is the project's single reference document. Someone starting from zero — or a
fresh Claude session — should finish this page knowing what the system is, how it
works, why each decision was made the way it was, and where the traps are.

`README.md` is the short outward-facing introduction. The detail lives here.

---

## 1. What this project is

A **real-time data pipeline** that pulls live trade data from the Binance public REST
API, moves it through Kafka, folds it into 5-second OHLC/VWAP windows with Spark
Structured Streaming, and writes every finished window to MongoDB and to a JSON Lines
file.

Every component runs in a container. Moving the stack to another machine is "copy the
folder, run `docker compose up -d`".

**Status: the six-step plan is complete.** The system runs and starts with one command.

---

## 2. Working agreement (important)

These are the user's explicit requests, not assumptions:

| Rule | Detail |
|---|---|
| **Go one step at a time** | Finish a step, verify it, wait for the user's confirmation |
| **The user makes every commit** | Never run `git commit`. Prepare the change, hand over the command, let the user run it |
| **Chat in Turkish, comments in English** | Code, YAML and `.env` comments are always English; conversation is Turkish |
| **Do not create unnecessary files** | Extend what exists rather than adding new files. Single-doc policy: this file plus `README.md` |
| **Do not claim without measuring** | Never say "it got faster" — measure it and give the number. Every performance decision here came from a measurement |

### Architectural decisions the user made

- **Source:** Binance public REST API — **no WebSocket or SSE**, plain REST GET
- **Stream engine:** Apache Spark Structured Streaming (PySpark)
- **NoSQL store:** MongoDB
- **Infrastructure:** Docker Compose, for portability
- **Goal:** the stack will later be **moved to a Linux machine**

---

## 3. Architecture

```
BINANCE REST   GET /api/v3/aggTrades?symbol=X&fromId=N
      │        producer.py — polls every 2s, fromId watermark, never a duplicate
      ▼
KAFKA          topic: binance.trades.raw   key=symbol, 3 partitions
      │        (KRaft mode, no Zookeeper, single node)
      ▼
SPARK          readStream kafka → from_json with an explicit schema
      │        → 5s tumbling window on event time + 10s watermark
      │        → trade_count, volume, quote_volume, VWAP, OHLC
      │
      ├──► MONGODB  realtime.trade_ohlc   _id = symbol|window_start (idempotent upsert)
      └──► output/live_ohlc.jsonl         append-only, one JSON object per line
                    │
                    └──► to_structured.py (separate batch job)
                         Mongo → explicit StructType → Parquet (structured)
```

### Services

| Service | Image | Port | Role |
|---|---|---|---|
| `rtp-kafka` | `apache/kafka:3.9.1` | 29092 | The message log |
| `rtp-kafka-init` | `apache/kafka:3.9.1` | — | Creates the topic idempotently, then exits (one-shot) |
| `rtp-producer` | `rtp-producer:1.0` (built) | — | Binance → Kafka |
| `rtp-spark` | `rtp-spark:4.0.0` (built) | 4040 | Kafka → aggregation → Mongo + file |
| `rtp-mongo` | `mongo:8` | 27018 | Durable store for results |
| `rtp-kafka-ui` | `kafbat/kafka-ui` | 8080 | Browse topics and messages (debug only) |

**Volumes:** `kafka-data` (the broker's log), `mongo-data` (Mongo's data),
`spark-checkpoint` (Spark offsets and state). All three are removed together by `down -v`
and by nothing else — the checkpoint references Kafka offsets, so wiping one without the
other breaks the stream.

---

## 4. File map

```
docker-compose.yml     six services, all orchestration
.env                   ALL configuration (gitignored)
.env.example           tracked template — a new variable goes in both files
.gitignore
CLAUDE.md              this file
README.md              outward-facing introduction
producer/
  producer.py          Binance REST → Kafka (290 lines)
  requirements.txt     requests, confluent-kafka
  Dockerfile           python:3.12-slim, uid 1000
spark/
  stream_job.py        Kafka → 5s OHLC/VWAP → Mongo + JSONL (streaming)
  to_structured.py     Mongo → Parquet (batch, run on demand)
  Dockerfile           apache/spark:4.0.0 + pymongo, /checkpoint owned by uid 185
output/                live_ohlc.jsonl and structured/ (gitignored)
```

**How code edits propagate:**
- `spark/` is bind-mounted at `/app`, so editing `stream_job.py` needs only
  `docker compose restart spark` — no rebuild
- `producer.py` is baked into its image, so it needs
  `docker compose up -d --build producer`

---

## 5. Running it

```bash
docker compose up -d                  # start everything
docker compose ps                     # status
docker compose logs -f spark          # follow Spark
docker compose logs -f producer       # follow the producer
docker compose restart spark          # pick up an edit to stream_job.py
docker compose stop                   # stop, keep all data
docker compose down                   # remove containers, keep volumes
docker compose down -v                # remove volumes too (Mongo data + checkpoint go)
```

**Watch the live output:**
```powershell
Get-Content output\live_ohlc.jsonl -Tail 5 -Wait     # Windows
tail -f output/live_ohlc.jsonl                        # Linux/macOS
```

**Structured transform (Mongo → Parquet):**
```bash
docker compose run --rm --no-deps spark \
    /opt/spark/bin/spark-submit /app/to_structured.py
```
Runs in its own short-lived container and does not disturb the streaming job. Exits
non-zero if any constraint is violated.

**Web UIs:** `localhost:8080` (Kafka UI) · `localhost:4040` (Spark UI, only while Spark
is running)

**Start over:**
```bash
docker compose down -v && rm -rf output && docker compose up -d
```

---

## 6. Data formats — the full lineage

The same data through five formats. The example below is **real**: SOLUSDT, the
2026-08-17 11:46:50–11:46:55 window, four trades.

### ① Raw Binance response

```
GET https://api.binance.com/api/v3/aggTrades?symbol=SOLUSDT&fromId=666655295&limit=4
```
```json
[{"a":666655295,"p":"75.90000000","q":"0.09700000","f":2021980492,"l":2021980492,"T":1786967211622,"m":false,"M":true},
 {"a":666655296,"p":"75.89000000","q":"0.07300000","f":2021980493,"l":2021980493,"T":1786967212771,"m":true, "M":true},
 {"a":666655297,"p":"75.89000000","q":"0.26300000","f":2021980494,"l":2021980494,"T":1786967213997,"m":true, "M":true},
 {"a":666655298,"p":"75.90000000","q":"1.00600000","f":2021980495,"l":2021980495,"T":1786967214373,"m":false,"M":true}]
```

Single-letter field names (bandwidth), price and quantity as **strings** (to avoid
decimal precision loss in transit), and **no `symbol` field** — it was in the request.

`a`=aggTrade id · `p`=price · `q`=qty · `f`/`l`=first/last trade id · `T`=timestamp ·
`m`=is buyer maker · `M`=best price match

### ② What the producer writes to Kafka

```json
{"symbol":"SOLUSDT","trade_id":666655295,"price":75.9,"qty":0.097,
 "quote_qty":7.3623,"trade_time":1786967211622,"is_buyer_maker":false,
 "ingest_time":1786967213095}
```

| Change | Why |
|---|---|
| `a`→`trade_id`, `p`→`price`, `q`→`qty`, `T`→`trade_time` | Readability |
| `"75.90000000"` → `75.9` | String → number, so Spark can sum it |
| `symbol` added | Kafka's partition choice keys on it |
| `quote_qty` added | `price × qty`, computed once here for VWAP |
| `ingest_time` added | To measure latency |
| `f`, `l`, `M` dropped | Unused |

**The two timestamps matter:** `trade_time` is when the trade happened at Binance (event
time), `ingest_time` is when the producer saw it. Spark windows on **`trade_time`**.

### ③ Kafka: bytes

Kafka does not care about content. `key=SOLUSDT` (bytes), `value` is the UTF-8 bytes of
that JSON. "This is JSON" is a **convention**, not something Kafka knows.

### ④ Spark: typed columns → aggregation → Mongo document

```javascript
{
  _id: "SOLUSDT|2026-08-17T11:46:50",
  symbol: "SOLUSDT",
  window_start: ISODate("2026-08-17T11:46:50.000Z"),
  window_end:   ISODate("2026-08-17T11:46:55.000Z"),
  trade_count: 4,   volume: 1.439,   quote_volume: 109.21674000000002,
  vwap: 75.89766504517027,
  high: 75.9, low: 75.89, first_price: 75.9, last_price: 75.9
}
```

### ⑤ Structured table (Parquet)

```
symbol=SOLUSDT | window_start=2026-08-17T11:46:50Z | window_end=...11:46:55Z
trade_count=4 | volume=1.439 | quote_volume=109.21674000000002
vwap=75.89766504517027 | high=75.9 | low=75.89
first_price=75.9 | last_price=75.9 | price_change_pct=0.0
```

### Verified by hand — every value matched

| Field | Computed by hand | System |
|---|---|---|
| `trade_count` | 4 | 4 ✅ |
| `volume` | 0.097+0.073+0.263+1.006 = 1.439 | 1.439 ✅ |
| `quote_volume` | 7.3623+5.53997+19.95907+76.3554 = 109.21674 | 109.21674 ✅ |
| `vwap` | 109.21674 / 1.439 = 75.8976650451703 | 75.89766504517027 ✅ |
| `high` / `low` | 75.9 / 75.89 | 75.9 / 75.89 ✅ |
| `first_price` / `last_price` | lowest/highest `trade_id` → 75.9 / 75.9 | 75.9 / 75.9 ✅ |

**Why VWAP is not a plain average:** the plain average of the four prices is 75.895. But
70% of the volume sat in the last trade (1.006 units) and went through at 75.90. VWAP
weights that and says **75.8977** — the number that reflects where the money actually
traded.

---

## 7. The structured transform: why and how

**The problem:** MongoDB is **schemaless**. The `trade_ohlc` documents look regular, but
Mongo guarantees nothing — a client writing a string into `vwap` would be accepted. So
the pipeline's output is technically **semi-structured**.

**The fix:** `spark/to_structured.py` reads Mongo, **forces** every field through an
explicit `StructType`, checks the constraints, and writes **Parquet**. Parquet carries
its schema inside the file, so whatever reads it back gets the types without being told.

### Data contract

| Column | Type | Meaning |
|---|---|---|
| `symbol` | string | Trading pair |
| `window_start` / `window_end` | timestamp | Window bounds (UTC, start inclusive, end exclusive) |
| `trade_count` | long | Trades in the window |
| `volume` | double | Base asset volume (BTC) |
| `quote_volume` | double | Quote asset volume (USDT) |
| `vwap` | double | `quote_volume / volume` |
| `high` / `low` | double | Highest / lowest price |
| `first_price` / `last_price` | double | Open / close (the O and C of OHLC) |
| `price_change_pct` | double | **Derived:** `(last-first)/first × 100` |

**Natural key:** `(symbol, window_start)`. Mongo's `_id` is deliberately **dropped** — it
is a storage concern, a composite string built for idempotent upserts.

**Constraints checked:** `trade_count > 0` · `volume > 0` · `low ≤ vwap ≤ high` ·
`low ≤ first_price ≤ high` · `low ≤ last_price ≤ high` · `high ≥ low` ·
`window_end - window_start = 5s`

### Measured result (2760 rows)

- **Zero violations across all seven constraints**
- Storage: JSONL 746 KB → CSV 379 KB → **Parquet 147 KB** (80% smaller)
- Partitioned by symbol, **one file per symbol** — thanks to `repartition("symbol")`;
  without it every task drops a fragment into every directory, the "small files problem"
- SQL works, which is the proof of being structured:

```
+-------+-------+------+--------+--------+--------+
|symbol |windows|trades|avg_vwap|highest |lowest  |
+-------+-------+------+--------+--------+--------+
|BTCUSDT|964    |27348 |63656.07|63781.69|63512.01|
|ETHUSDT|955    |17360 |1906.06 |1909.49 |1901.78 |
|SOLUSDT|841    |7808  |75.78   |75.95   |75.58   |
+-------+-------+------+--------+--------+--------+
```

---

## 8. Design decisions and the reasons behind them

The most valuable section. Every "why is it done this way?" is answered here.

### Source

**`/api/v3/aggTrades`, not `/api/v3/trades`.** Same REST family, identical `fromId`
logic. But `trades` carries a high rate-limit weight: polling three symbols frequently
overran the 1200 weight/min ceiling. `aggTrades` is much lighter, so we poll more often
and get a livelier stream. Measured usage: **~360 of 1200 per minute**.

**Seeding the watermark publishes nothing.** The first call uses `limit=1` purely to read
the latest `trade_id` and start the watermark there. No burst of stale data at startup —
the stream begins live. This is also why a restart produces no duplicates.

**Paging exists but is bounded.** If a poll comes back full (1000 records) the symbol is
fetched again in the same cycle, at most five pages, so one busy symbol cannot burn the
minute's weight budget.

**`confluent-kafka`, not `kafka-python`.** kafka-python has known Python 3.13
incompatibilities. confluent-kafka 2.15.0's cp313 wheel installed cleanly on both Windows
and Linux.

**`enable.idempotence=True`.** librdkafka's internal retry after a network hiccup cannot
duplicate a record. A single `Coordinator load in progress: retrying` warning at startup
is normal — the broker has only just come up.

**One symbol's failure never kills the loop.** Beyond the specific handlers for rate limits
and network errors, the per-symbol call is wrapped in a broad `except Exception` that logs
the traceback and moves on. This looks like the anti-pattern it usually is, but here the
alternative is worse: an unhandled error kills the process, `restart: unless-stopped`
brings it back, and the producer reseeds its watermark from the *latest* trade — so every
trade between the crash and the restart is silently lost. Skipping one symbol for one cycle
leaves its watermark untouched, so the next cycle resumes exactly where this one stopped.
This is the only place in the pipeline where data could go missing without a trace;
everywhere else a checkpoint, an idempotent upsert or `failOnDataLoss` covers it.

### Kafka

**KRaft mode, no Zookeeper**, single node — simplicity.

**Two listeners are mandatory:** `kafka:9092` (between containers, INTERNAL) and
`localhost:29092` (scripts on the host, EXTERNAL). With one listener the broker advertises
its own address, so a host client would try to resolve `kafka:9092` and hang.
⚠️ `KAFKA_BOOTSTRAP=localhost:29092` in `.env` is **only for running from the host**; the
producer and spark services in compose override it explicitly with `kafka:9092`.

**Produced with `key=symbol`.** All trades of one symbol land on the same partition, so
their order is preserved. Out-of-order price data would be a disaster.

**The partition spread is uneven and that is fine.** Three symbols, three partitions, but
the murmur2 hash collides: BTCUSDT + ETHUSDT → p0, SOLUSDT → p2, p1 empty. The guarantee
we need — same symbol, same partition — holds. It evens out as symbols are added.

**The `kafka-init` service.** Creates the topic with `--if-not-exists` on every `up`, so
the topic is there after a `down -v` without a manual step. `auto.create.topics` is off so
the partition count stays ours.

**Kafka's log is persisted, and it has to be.** `KAFKA_LOG_DIRS=/var/lib/kafka/data` on the
`kafka-data` volume. The broker originally had no volume, on the reasoning that a live
stream's history need not survive — but the *Spark checkpoint* does survive, and it
remembers Kafka offsets. A plain `docker compose down` then wiped the broker while the
checkpoint lived on, so Spark asked for offset ~66000 in a broker that had restarted at 0,
raised `KafkaIllegalStateException: Some data may have been lost`, and — with
`restart: unless-stopped` — crash-looped 35 times. **Two stores whose contents reference
each other must share a lifetime.** Now both are named volumes and both go only on
`down -v`. No custom image was needed: the apache/kafka image already owns
`/var/lib/kafka/data` as `appuser`, and a fresh named volume inherits that ownership.

**`failOnDataLoss=false` on the Kafka source.** The safety net for the same class of
problem: if a committed offset is gone, skip to the earliest one still available instead of
failing the query forever. Retention deleting old segments while Spark is down would
otherwise be fatal too. Mongo upserts are idempotent, so re-reading what remains costs
nothing.

**`kafbat/kafka-ui`, not `provectuslabs/kafka-ui`.** The Provectus repo was archived; the
project continues under kafbat.

### Spark

**Runs in a container, not on the host.** PySpark checkpointing on Windows wants
`winutils.exe`/`hadoop.dll`, which also defeats the portability goal.

**Explicit `StructType`, not schema inference.** Inference does not work on streaming
sources anyway. `from_json` in PERMISSIVE mode turns a malformed record into nulls; the
`trade_id IS NOT NULL` filter drops those instead of failing the batch.

**Windows are cut on event time.** `timestamp_millis(trade_time)` converts to a real
timestamp, so a record delayed in transit still lands in **the window it belongs to**.

**`min_by`/`max_by`, not `first()`/`last()`.** In streaming, `first()`/`last()` give no
ordering guarantee within a group. The ordering key is `trade_id`, not `trade_time`:
several trades can share a millisecond (observed in the data), while `trade_id` strictly
increases per symbol.

**`local[4]`, not `local[2]`.** With two threads the micro-batch consistently overran the
5-second trigger (5.4–5.6s, "batch is falling behind"). At four, one warning remained
across 36 batches.

**`spark.sql.shuffle.partitions=8`.** The default is **200** and it had never been set —
every micro-batch opened a 200-task stage to produce three rows. Found by profiling in the
Spark UI. At 8: **200 → 8 tasks**, stage time **~6000 ms → ~360 ms**.

**Watermark of 10 seconds** (it started at 30). The producer walks `fromId` sequentially,
so records barely ever arrive late; 30s was needlessly generous. At 10s, **no record was
dropped** across 55 windows (Kafka 557 = Spark 557).
⚠️ The one case it does not cover: a symbol whose polling backs off for longer than this
while the other symbols keep advancing the watermark — those trades fall out of their
window.

### Sinks

**One `foreachBatch`, not two queries.** Two queries would read the topic twice and
aggregate the windows twice.

**`foreachBatch` + `pymongo`, not `mongo-spark-connector`.** One less dependency and a
pile of version-compatibility risk avoided; the code stays in one place.

**`_id = symbol|window_start`.** `foreachBatch` is at-least-once; this key turns it into
effectively-once, because a replay *overwrites* the same document.

**JSONL is written through `foreachBatch`, not `writeStream.format("json")`.** The
built-in JSON sink spreads output over dozens of `part-00000-*.json` files; what is wanted
is one readable file that grows live. **The JSONL does gain duplicate lines on a replay —
deliberately.** It is a live tail, not the record of truth.

**The JSONL tail is bounded to `JSONL_MAX_LINES` (default 10000).** Left unbounded it grew
at a measured **560 KB/hour** — about 400 MB a month — for windows that MongoDB already
stores properly. Trimming keeps the newest lines and runs only once the file overshoots by
`JSONL_TRIM_SLACK` (1000), so the rewrite happens every few thousand lines instead of every
batch; rewriting megabytes every five seconds would cost more than the file is worth. A
process tailing the file sees it jump at that moment — the accepted trade-off. Verified by
temporarily setting the limit to 500: a 4100-line file was trimmed to exactly 500 and the
lines kept were the most recent ones.

**Why 10000, and what the limit actually bounds.** Measured at 275 bytes per line and
36 lines per minute (3 symbols × 12 windows), so the default works out to **2.63 MB** of
file and **4.6 hours** of history, with a trim roughly every **28 minutes** — a few
milliseconds of work each time, and 0.6% of the Spark driver's 434 MB held in the deque
while it runs.

The important part: **the limit fixes the size, not the duration.** How much history that
size buys depends on the symbol count and the window length, neither of which the limit
knows about:

| Symbols | 10000 lines covers |
|---|---|
| 3 (current) | 4.6 hours |
| 10 | 1.4 hours |
| 30 | 28 minutes |

So adding symbols keeps the file at 2.63 MB — healthy — while quietly shrinking the window
of history. Shortening `WINDOW_DURATION` has the same effect: 1-second windows produce five
times the lines. If either changes materially, raise `JSONL_MAX_LINES` to match. For
reference: 1000 lines is too short to tail comfortably, 50000 is ~13 MB and about a day,
and anything past that is MongoDB's job, not this file's.

**The structured CSV is written from the driver, not with `df.write.csv()`.** Spark's CSV
writer always produces a *directory* — `part-00000-<uuid>.csv`, a `_SUCCESS` marker and a
hidden `.crc` checksum per file. None of that helps someone who wants to open one
spreadsheet, so `to_structured.py` writes the file itself with Python's `csv` module. Safe
at this scale, since the aggregation already reduced the data to a few thousand rows; a
much larger table would want the Spark writer back. Parquet keeps Spark's layout, because
a directory with `_SUCCESS` is what a Parquet dataset is supposed to look like.

**The checkpoint is a named volume (`spark-checkpoint`), not a bind mount.** With
`./checkpoint` bind-mounted, batches took **8–13s instead of 5s** — Spark rewrites
offset/state files on every micro-batch and doing that across the Windows filesystem
boundary is expensive. Moving it to `/tmp` and measuring again brought batches back to
exactly 5.0s, which confirmed the cause. `output/` stays a bind mount because seeing that
file is the whole point.

**The checkpoint lives at `/checkpoint`, not `/app/checkpoint`.** Two reasons: a fresh
named volume is owned by `root`, which blocked uid 185 from writing (`mkdir ... failed`),
and `/app` is itself a bind mount so nesting a second mount under it is fragile. Creating
`/checkpoint` as uid 185 in the Dockerfile makes the volume inherit that ownership on
first use.

### Operations

**`restart: unless-stopped`** on the five long-lived services; `kafka-init` stays
`restart: "no"` because it is a one-shot job. On the night of 13 Aug `rtp-spark` died
(heartbeat timeout, exit 56, not OOM — the container froze, most likely with the host
sleeping) and stayed down 19 hours because nothing restarted it. The checkpoint meant no
data was lost. After the policy was added the same crash happened again and **Docker
brought it back on its own** (`RestartCount=1`), without producing a single duplicate line
in the JSONL.

**`.env` is gitignored, `.env.example` is tracked.** A new variable goes in **both**. The
initial commit still contains `.env` and was deliberately left alone — it holds no secrets
(topic name, ports, symbol list), so history was not rewritten.

**No `user:` override.** The intent was to keep root-owned files out of the bind mount,
but both images already run unprivileged (Spark uid 185, producer uid 1000), so the
problem never arises. On Linux, `sudo chown -R "$USER" output` is enough.

---

## 9. Measured facts

| Measurement | Value |
|---|---|
| End-to-end latency (window closes → result written) | 23.3s at best, **26.3s average**, 29.1s at worst |
| Binance weight usage | ~360 of 1200 per minute |
| Micro-batch interval | exactly 5.0s (shows as 10s on batches that emit nothing) |
| Aggregation stage | 8 tasks, ~360 ms |
| Producer → Kafka delay | ~2–3s, from the poll interval |
| Parquet saving | **80% smaller** than the JSONL |

**Where the latency comes from:** the 10s watermark plus Spark's micro-batch mechanics.
A window can only close once an event past the watermark has been *seen*, the watermark
computed in one batch only takes effect in the *next*, and the trigger interval and batch
time add on top. Lowering the watermark cuts latency one for one but does not reach the
floor — Spark's floor is roughly two to three trigger intervals. If milliseconds are
needed, the tool is **Flink**, not Spark.

### Verification tests that were run

- Producer restarted twice → **zero duplicate `trade_id`s**
- Spark's own trade count vs the real count in Kafka → **1232 = 1232**, difference 0
- Across 121 windows, VWAP and open/close always within `[low, high]` → **zero outliers**
- No window was ever emitted twice → append mode is correct
- Checkpoint reset and the whole topic reprocessed → JSONL **+842** lines, Mongo **+304**
  documents; Mongo's 842 documents match the JSONL's 842 unique windows exactly
  (idempotency)
- Clean install (`down -v` → `up -d`) → 6/6 services in 8.1s, zero failed jobs
- Structured transform → 2760 rows, **zero violations** across seven constraints

---

## 10. Known limits and traps

All of these were hit for real, and all of them cost time:

**A Compose `command` given as a string gets split into words.** `sh -c` then treats only
the first word as the script. Fix: make `command` a **single-element list**. Watch YAML's
folded `>` block too — indented continuation lines are not folded.

**The console sink *prints* at most `numRows` rows per batch and silently drops the
rest** (`only showing top 20 rows`). Windows emitted ≠ rows printed. Counting log lines to
verify totals gives a wrong answer because of this.

**`docker kill` / `docker stop` disable the restart policy** — Docker treats them as a
manual stop. The policy **cannot** be tested with those commands.

**A container's PID 1 is protected from SIGKILL sent from inside its own namespace.**
`kill -9 1` from within does nothing.

**A fresh named volume is owned by `root`.** An unprivileged user cannot write to it. Fix:
create the mount point with the right uid in the Dockerfile.

**Frequent small writes on a Windows bind mount are very expensive.** The Spark checkpoint
fell victim to this (5s → 8-13s).

**`kafka-get-offsets.sh` wants `--topic-partitions topic:N`, not `--partition`.**

**The Spark UI's streaming statistics page 400s after a restart.**
`HTTP ERROR 400 Failed to find streaming query <uuid>` on
`/StreamingQuery/statistics/` means the page was loaded before the driver
restarted. Spark keeps two identifiers: `queryId` is stored in the checkpoint and
survives restarts, while `runId` is regenerated on every run — and that page keys
on `runId`. Not a fault: reload `localhost:4040` and click the query again. Never
bookmark the statistics URL, only the UI root.

**Parquet does not preserve `nullable=false` on read-back.** Types travel, the *not null*
constraint does not — Spark marks every column `nullable=true`. If it must be enforced,
use a relational database (`NOT NULL`) or supply an explicit schema on read. `symbol` also
**moves to the end** of the schema, because it is a partition column stored in the
directory name.

**`to_structured.py` is a batch job.** It produces a snapshot and replaces the previous
one with `mode("overwrite")`. Continuous freshness would need scheduled runs.

**MongoDB is the one store that still grows without bound**, now that the JSONL tail is
capped. Measured at 241 bytes per document and 36 documents per minute with three symbols:
roughly **12 MB a day, 4.5 GB a year**. That is intended — Mongo is the record of truth and
nothing should silently delete from it — but a long-running deployment needs a plan:
a TTL index on `window_start`, periodic archival to Parquet (which `to_structured.py`
already produces), or rolling the collection by month. The only index is the default one on
`_id`; since `_id` is `symbol|window_start`, prefix queries by symbol already use it.

**Mongo does not enforce a schema** — the `$jsonSchema` validator was deliberately not
added. A wrongly typed document can get into Mongo; `to_structured.py` catches it on read.

**VS Code's Python extension interrupts the terminal at startup and cancels whatever was
typed with Ctrl+C.** Wait for `(.venv)` to appear on the prompt before typing. `git add .`
silently lost to this several times; `git commit -a` avoids the step entirely.

---

## 11. Environment facts

| | |
|---|---|
| OS | Windows 11 Home (no Hyper-V → WSL2 backend mandatory) |
| Docker | Desktop 4.86.0, CLI 29.7.2, Compose 5.3.1 |
| WSL | 2.7.11.0, kernel 6.18.33.2-2 |
| Python (host) | 3.13.2 — the `.venv` was only for running the producer from the host, no longer needed |
| **Native MongoDB 8.2 (port 27017)** | Installed and running on this machine but **unused by this project**. That is why the compose Mongo maps to 27018 |
| Java (host) | Only JRE 8 32-bit — would have been insufficient, but Kafka and Spark run in containers so it never mattered |
| Repo | `https://github.com/zeynep-okkiran/REALTIME-PIPELINE` |

**The only thing installed was Docker Desktop + WSL2.** No JDK, no native Kafka, no native
Spark, no extra WSL distro — everything else came from images. A direct consequence of the
portability decision.

---

## 12. Build history

Six steps, each one stopping for the user's commit before the next began:

| Step | What was done | Commit |
|---|---|---|
| 0 | Docker Desktop + WSL2 install (needed a reboot and `wsl --update`) | — |
| 1 | `docker-compose.yml`, `.env`, `.gitignore`; kafka/mongo/kafka-ui up, topic with 3 partitions | `463a176` |
| 2 | `producer/producer.py` + `requirements.txt`, run from the host | `3b189df` |
| 3 | `spark/stream_job.py` skeleton (console sink), `spark` service | `3b189df` |
| 4 | Schema + 5-second windowed OHLC/VWAP aggregation | `0062ed6` |
| — | Watermark 30 → 10 seconds | `1453ba7` |
| 5 | `foreachBatch` → Mongo + JSONL, checkpoint, `spark/Dockerfile` | `8be2f16` |
| — | Spark UI port (4040) + `queryName` | `b52a6b0` |
| 6 | `producer/Dockerfile` + service, restart policies, shuffle=8, `README.md` | — |
| + | `to_structured.py` — Mongo → Parquet structured transform | — |

**Risks that were closed:** Binance rate limit (aggTrades + backoff) · Spark 4.0 ↔
connector compatibility (no problem, no fallback to 3.5 needed) · Kafka listener
configuration (two listeners from the start) · 27017 port clash (moved to 27018) · slow
jar downloads (`.ivy` cache, zero re-downloads on restart) · duplicate Mongo rows on
replay (idempotent upsert)

---

## 13. Possible next steps (not done)

Ideas on record; none of them are needed:

- **mongo-express** — browse MongoDB from the browser (`localhost:8081`). Offered, not
  requested.
- **Tiered aggregation** — 1-minute and 1-hour windows layered on top of the 5-second
  ones. The standard pyramid in real systems.
- **Avro + Schema Registry** instead of JSON. 60-80% smaller and schema-enforced, but it
  makes the data unreadable by eye. JSON was a deliberate choice: being able to click a
  message in Kafka UI and read it has real teaching value.
- **`Decimal` instead of `float`** for prices. Fine for crypto as it is, but in a real
  financial system float arithmetic on money loses cents over time.
- **Spark History Server** — the Spark UI only exists while a job runs; this keeps the
  record of finished ones.
- **PostgreSQL** — Parquet was chosen as the structured target. A relational table
  (`NOT NULL`, `CHECK`, `PRIMARY KEY`) is the alternative if constraints must be enforced
  by the store.
