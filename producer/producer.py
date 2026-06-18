"""
Simulated engine telemetry producer for Confluent Cloud — the engines of Sodor.

Emits one JSON message per engine every INTERVAL_SECONDS to TOPIC.
Messages are keyed by engine_id so all events for a given engine land on the
same partition, preserving per-engine ordering.

Config comes from environment variables (see .env.example).
Start here with plain JSON; once the loop works end-to-end, switch to Avro
+ Schema Registry (the harder, more interesting version).
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

TOPIC = os.environ.get("TOPIC", "sodor-telemetry")
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "2"))
NUM_ENGINES = int(os.environ.get("NUM_ENGINES", "5"))

conf = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "client.id": "sodor-telemetry-producer",
}

producer = Producer(conf)

# The engines of the North Western Railway, in number order. We take the
# first NUM_ENGINES of these; anything beyond the roster falls back to a
# numbered engine id.
SODOR_ENGINES = [
    "Thomas", "Edward", "Henry", "Gordon", "James",
    "Percy", "Toby", "Duck", "Donald", "Douglas",
    "Oliver", "Emily", "BoCo", "Daisy", "Bill", "Ben",
]


def engine_name(i):
    return SODOR_ENGINES[i] if i < len(SODOR_ENGINES) else f"engine-{i + 1:03d}"


# Seed each engine near the Island of Sodor (off the north-west coast of England).
engines = {}
for i in range(NUM_ENGINES):
    engine_id = engine_name(i)
    engines[engine_id] = {
        "lat": 54.25 + random.uniform(-0.2, 0.2),
        "lon": -4.5 + random.uniform(-0.2, 0.2),
        "heading": random.uniform(0, 360),
        "speed_kmh": random.uniform(60, 160),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  delivery FAILED for {msg.key()}: {err}")
    else:
        print(
            f"  {msg.key().decode()} -> {msg.topic()} "
            f"[p{msg.partition()}] @ offset {msg.offset()}"
        )


def step(state):
    """Nudge the engine's position and telemetry a little each tick."""
    state["speed_kmh"] = max(0.0, state["speed_kmh"] + random.uniform(-5, 5))
    state["lat"] += random.uniform(-0.001, 0.001)
    state["lon"] += random.uniform(-0.001, 0.001)
    return state


def build_event(engine_id, state):
    return {
        "event_id": str(uuid.uuid4()),
        "engine_id": engine_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(state["lat"], 6),
        "longitude": round(state["lon"], 6),
        "speed_kmh": round(state["speed_kmh"], 1),
        "heading": round(state["heading"], 1),
        "door_status": random.choice(["open", "closed"]),
        "car_temperature_c": round(random.uniform(18, 26), 1),
    }


def main():
    print(
        f"Producing to '{TOPIC}' every {INTERVAL_SECONDS}s "
        f"for {NUM_ENGINES} engines. Ctrl-C to stop."
    )
    try:
        while True:
            for engine_id, state in engines.items():
                engines[engine_id] = step(state)
                event = build_event(engine_id, engines[engine_id])
                producer.produce(
                    TOPIC,
                    key=engine_id,
                    value=json.dumps(event),
                    callback=delivery_report,
                )
            producer.poll(0)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nFlushing...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
