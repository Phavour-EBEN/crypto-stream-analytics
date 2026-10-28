"""
Reads live trades from the Binance WebSocket and publishes them to Kafka.

Each Kafka message:
  key   = symbol (e.g. "BTCUSDT")
  value = JSON {trade_id, symbol, price, quantity, trade_time, side}
"""
import asyncio
import json
import logging
import os
import random

import websockets
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# --- Settings from environment variables ---
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",") if s.strip()]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_trades")
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision")

WS_URL = f"{WS_BASE}/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 100,          # wait up to 100ms to batch messages together
    "acks": "all",             # broker confirms the message is stored
})


def delivery_report(err, msg):
    """Called once per message to report whether Kafka stored it."""
    if err is not None:
        log.error("Delivery failed for %s: %s", msg.key(), err)


def to_record(d: dict) -> dict:
    """Turn a raw Binance trade into a clean record."""
    return {
        "trade_id": d["t"],
        "symbol": d["s"],
        "price": float(d["p"]),
        "quantity": float(d["q"]),
        "trade_time": d["T"],                    # milliseconds since epoch
        "side": "sell" if d["m"] else "buy",     # buyer is maker -> seller was aggressive
    }


def send(record: dict) -> None:
    """Queue one record for Kafka, keyed by symbol so each coin stays in one partition."""
    try:
        producer.produce(
            KAFKA_TOPIC,
            key=record["symbol"],
            value=json.dumps(record),
            callback=delivery_report,
        )
    except BufferError:
        # Local queue is full (Kafka slow or down): wait for it to drain, then retry once
        producer.poll(1)
        producer.produce(KAFKA_TOPIC, key=record["symbol"], value=json.dumps(record),
                         callback=delivery_report)
    producer.poll(0)    # handle delivery callbacks


async def stream_trades():
    backoff = 1
    sent = 0
    while True:
        try:
            log.info("Connecting to %s", WS_URL)
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                log.info("Connected. Streaming %d symbols.", len(SYMBOLS))
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", {})
                    if data.get("e") != "trade":
                        continue
                    try:
                        record = to_record(data)
                    except (KeyError, ValueError) as e:
                        log.warning("Skipping malformed message: %s", e)
                        continue

                    send(record)
                    sent += 1
                    if sent % 1000 == 0:
                        log.info("Sent %d trades", sent)
        except Exception as e:
            log.warning("Connection problem: %s. Reconnecting in ~%ds", e, backoff)

        producer.flush(5)
        await asyncio.sleep(backoff + random.random())
        backoff = min(backoff * 2, 60)  # exponential backoff, capped at 60s


if __name__ == "__main__":
    try:
        asyncio.run(stream_trades())
    except KeyboardInterrupt:
        log.info("Stopping, flushing remaining messages...")
        producer.flush(10)
