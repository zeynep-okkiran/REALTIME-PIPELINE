"""Binance aggTrades -> Kafka producer.

Polls the Binance public REST API for aggregated trades and publishes them to a
Kafka topic. Every symbol keeps its own `fromId` watermark, so a trade is never
published twice no matter how often the loop runs.

Configuration comes from environment variables; values missing from the
environment are filled in from the project's .env file. That way the exact same
file runs on the host (step 2, KAFKA_BOOTSTRAP=localhost:29092) and inside a
container (step 6, KAFKA_BOOTSTRAP=kafka:9092).
"""

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import requests
from confluent_kafka import Producer

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from a .env file without adding a dependency.

    Real environment variables win: only keys that are not already set are
    filled in. In a container the values are injected by Compose and this file
    does not even have to exist.
    """
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


load_env_file(PROJECT_ROOT / ".env")

SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", "BTCUSDT").split(",") if s.strip()]
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "2"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance.trades.raw")

BINANCE_URL = "https://api.binance.com/api/v3/aggTrades"
REQUEST_TIMEOUT_SEC = 10

# Binance caps aggTrades at 1000 records per call. When a poll comes back full
# there may be more waiting, so the symbol is fetched again in the same cycle --
# but at most MAX_PAGES_PER_CYCLE times, to bound how much weight one busy
# symbol can burn in a single pass.
PAGE_LIMIT = 1000
MAX_PAGES_PER_CYCLE = 5

# The REST weight budget is 1200 per minute per IP. Slow down before hitting it
# rather than waiting for the 429 that leads to an IP ban (418).
WEIGHT_LIMIT_1M = 1200
WEIGHT_SOFT_LIMIT = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")


class RateLimited(Exception):
    """Raised on HTTP 429 (rate limit) and 418 (IP banned after repeated 429)."""

    def __init__(self, status: int, retry_after: float):
        super().__init__(f"HTTP {status}, retry after {retry_after:.0f}s")
        self.status = status
        self.retry_after = retry_after


# --------------------------------------------------------------------------
# Binance REST
# --------------------------------------------------------------------------


def fetch_agg_trades(session: requests.Session, symbol: str, from_id: int | None, limit: int):
    """Return (records, used_weight) for one aggTrades call.

    `from_id` is inclusive on the Binance side, so callers pass watermark + 1.
    """
    params = {"symbol": symbol, "limit": limit}
    if from_id is not None:
        params["fromId"] = from_id

    response = session.get(BINANCE_URL, params=params, timeout=REQUEST_TIMEOUT_SEC)

    used_weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
    used_weight = int(used_weight) if used_weight and used_weight.isdigit() else None

    if response.status_code in (429, 418):
        retry_after = response.headers.get("Retry-After")
        raise RateLimited(
            response.status_code,
            float(retry_after) if retry_after and retry_after.isdigit() else 60.0,
        )

    response.raise_for_status()
    return response.json(), used_weight


def normalize(symbol: str, raw: dict, ingest_time_ms: int) -> dict:
    """Map a Binance aggTrade onto the flat schema the Spark job will read.

    Binance sends price and quantity as strings; they become floats here so the
    downstream aggregation (VWAP, OHLC) does not have to cast every field.
    Timestamps stay as epoch milliseconds -- unambiguous, and a single cast in
    Spark turns them into a real timestamp.
    """
    price = float(raw["p"])
    qty = float(raw["q"])
    return {
        "symbol": symbol,
        "trade_id": raw["a"],          # aggregate trade id, also the watermark
        "price": price,
        "qty": qty,
        "quote_qty": price * qty,
        "trade_time": raw["T"],        # epoch ms, event time
        "is_buyer_maker": raw["m"],
        "ingest_time": ingest_time_ms,  # epoch ms, when the producer saw it
    }


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------


def on_delivery(err, msg):
    """Delivery report callback; only failures are worth a line in the log."""
    if err is not None:
        log.error("delivery failed for %s: %s", msg.key(), err)


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "binance-producer",
            # Idempotent producer: an internal retry after a network hiccup
            # cannot duplicate a record. Implies acks=all.
            "enable.idempotence": True,
            # Small batching window; the poll loop is the real pacing mechanism.
            "linger.ms": 50,
        }
    )


def publish(producer: Producer, record: dict) -> None:
    """Produce one record keyed by symbol.

    The key matters: all trades of a symbol land on the same partition, so their
    order is preserved end to end.
    """
    payload = json.dumps(record).encode("utf-8")
    key = record["symbol"].encode("utf-8")
    try:
        producer.produce(KAFKA_TOPIC, key=key, value=payload, callback=on_delivery)
    except BufferError:
        # Local queue is full: drain it, then retry once.
        producer.flush(5)
        producer.produce(KAFKA_TOPIC, key=key, value=payload, callback=on_delivery)


# --------------------------------------------------------------------------
# Poll loop
# --------------------------------------------------------------------------

running = True


def request_stop(signum, _frame):
    global running
    running = False
    log.info("signal %s received, shutting down after this cycle", signum)


def poll_symbol(session: requests.Session, producer: Producer, symbol: str, watermarks: dict) -> tuple[int, int | None]:
    """Fetch and publish everything new for one symbol. Returns (count, weight)."""
    if symbol not in watermarks:
        # First sight of this symbol: read the single latest aggTrade only to
        # seed the watermark. It is not published -- the stream should start
        # live rather than replaying history at startup.
        rows, weight = fetch_agg_trades(session, symbol, None, limit=1)
        if rows:
            watermarks[symbol] = rows[-1]["a"]
            log.info("%s watermark seeded at trade_id=%s", symbol, watermarks[symbol])
        return 0, weight

    sent = 0
    weight = None
    for _ in range(MAX_PAGES_PER_CYCLE):
        rows, weight = fetch_agg_trades(session, symbol, watermarks[symbol] + 1, PAGE_LIMIT)
        if not rows:
            break

        ingest_time_ms = int(time.time() * 1000)
        for raw in rows:
            publish(producer, normalize(symbol, raw, ingest_time_ms))

        watermarks[symbol] = rows[-1]["a"]
        sent += len(rows)

        # A short page means the symbol is caught up for this cycle.
        if len(rows) < PAGE_LIMIT:
            break
    else:
        log.warning("%s still behind after %d pages", symbol, MAX_PAGES_PER_CYCLE)

    return sent, weight


def main() -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    log.info("symbols=%s topic=%s bootstrap=%s interval=%.1fs",
             ",".join(SYMBOLS), KAFKA_TOPIC, KAFKA_BOOTSTRAP, POLL_INTERVAL_SEC)

    session = requests.Session()
    producer = build_producer()
    watermarks: dict[str, int] = {}
    total = 0
    backoff = POLL_INTERVAL_SEC

    while running:
        cycle_start = time.monotonic()
        counts = {}
        weight = None

        for symbol in SYMBOLS:
            if not running:
                break
            try:
                counts[symbol], symbol_weight = poll_symbol(session, producer, symbol, watermarks)
                weight = symbol_weight if symbol_weight is not None else weight
                backoff = POLL_INTERVAL_SEC
            except RateLimited as exc:
                log.warning("%s rate limited (%s); sleeping %.0fs", symbol, exc, exc.retry_after)
                time.sleep(exc.retry_after)
            except requests.RequestException as exc:
                # Network trouble: exponential backoff, capped at a minute. The
                # watermark is untouched, so nothing is lost or duplicated.
                log.warning("%s request failed: %s; retrying in %.0fs", symbol, exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception:
                # Deliberately broad. Anything unexpected here - a Binance
                # response missing a field, a BufferError that survived the
                # flush - would otherwise kill the process, and a restart
                # reseeds the watermark from the latest trade, so every trade
                # between the crash and the restart is silently lost. Skipping
                # one symbol for one cycle costs nothing by comparison: its
                # watermark is untouched, so the next cycle picks up exactly
                # where this one stopped. The traceback is logged in full.
                log.exception("%s failed unexpectedly; skipping this cycle", symbol)

        total += sum(counts.values())
        producer.poll(0)  # serve delivery callbacks

        if counts:
            log.info(
                "%s | weight=%s/%s | total=%d",
                " ".join(f"{s}={n}" for s, n in counts.items()),
                weight if weight is not None else "?",
                WEIGHT_LIMIT_1M,
                total,
            )

        if weight is not None and weight >= WEIGHT_SOFT_LIMIT:
            log.warning("weight %d near the %d limit; pausing 20s", weight, WEIGHT_LIMIT_1M)
            time.sleep(20)

        elapsed = time.monotonic() - cycle_start
        if running and elapsed < POLL_INTERVAL_SEC:
            time.sleep(POLL_INTERVAL_SEC - elapsed)

    log.info("flushing %d queued messages", len(producer))
    producer.flush(30)
    log.info("stopped after publishing %d trades", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
