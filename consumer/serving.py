"""
Folded serving routes for the Sodor live-tap container (build-order step 9).

These three routes used to be the step-7 serving Lambda (infra/lambda/handler.py);
they fold into THIS FastAPI app as the Lambda + API Gateway are retired, so one
always-on container serves everything:

  POST /produce        produce one tick of Avro telemetry (all engines) to Kafka,
                       rate-limited process-wide (see MIN_PRODUCE_INTERVAL_S)
  GET  /latest         live view: latest row per engine, read from Snowflake
  GET  /query?name=…   one of three canned, server-side analytics queries

Two deliberate changes from the Lambda original:

  * Config is ENVIRONMENT-ONLY — no boto3, no in-app Secrets Manager fetch. Locally
    the values come from consumer/.env; in the cloud the ECS task definition injects
    the Secrets Manager values as env vars (see infra/ecs.tf). This keeps the image
    free of AWS SDK + IAM-at-runtime concerns; the platform handles secret delivery.
  * confluent-kafka (librdkafka) and snowflake-connector-python are blocking,
    synchronous clients, so every route hops the work OFF the event loop with
    `asyncio.to_thread` — an in-flight Snowflake query or Kafka flush must never
    stall the SSE stream the same server is pushing on /stream.

Credentials this module reads from the environment:

  Snowflake (reads)   SNOWFLAKE_{ACCOUNT,USER,PRIVATE_KEY,WAREHOUSE,DATABASE,
                      SCHEMA,ROLE}
  Produce (Kafka)     BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC (shared with
                      the live consumer — same cluster) + the producer service
                      account's own keys PRODUCE_{KAFKA_API_KEY,KAFKA_API_SECRET,
                      SCHEMA_REGISTRY_API_KEY,SCHEMA_REGISTRY_API_SECRET}.

The produce path authenticates as the *producer* service account (WRITE), NOT the
read-only app-live identity the live consumer uses — that one has no topic WRITE
ACL, by design.
"""
import asyncio
import json
import math
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from cryptography.hazmat.primitives import serialization
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

HERE = Path(__file__).parent
SCHEMA_FILE = HERE / "telemetry.avsc"
NUM_ENGINES = int(os.environ.get("NUM_ENGINES", "5"))

# Minimum wall-clock gap between actual produce ticks, process-wide. /produce is
# public and unauthenticated, and every call writes NUM_ENGINES Avro messages to
# Confluent Cloud (which the Snowflake sink then bills to ingest) — so a scripted
# flood is a direct cost lever. Because the simulation is a single shared global
# (one set of engines for all viewers), coalescing to at most one tick per interval
# loses nothing: every viewer watches the same trains over SSE regardless of who
# triggered the tick. The 1s default sits well under the page's 2s cadence, so a
# real visitor never trips it, while any flood is capped to ~NUM_ENGINES msg/s.
MIN_PRODUCE_INTERVAL_S = float(os.environ.get("MIN_PRODUCE_INTERVAL_SECONDS", "1.0"))

router = APIRouter()


# ─── Snowflake reads ─────────────────────────────────────────────────────────
#
# All query logic lives in Snowflake VIEWS (defined in ../snowflake); this module
# holds NO business SQL, only a server-side whitelist of view names and runs
# `SELECT * FROM <view>`. That keeps the classic-vs-streaming RECORD_CONTENT seam
# entirely inside Snowflake: schematizing the table (step 10) changes the views in
# one place and this code never does.

LATEST_VIEW = "SODOR_RAILWAY.TELEMETRY.TELEMETRY_LATEST"

# Canned analytics — ?name=… maps to a view. The value is a fixed identifier from
# this map (no raw SQL from the browser, no injection surface).
QUERY_VIEWS = {
    "speed_leaderboard": "SODOR_RAILWAY.TELEMETRY.SPEED_LEADERBOARD",
    "throughput_per_minute": "SODOR_RAILWAY.TELEMETRY.THROUGHPUT_PER_MINUTE",
    "doors_open_moving": "SODOR_RAILWAY.TELEMETRY.DOORS_OPEN_MOVING",
}

_conn = None
# Serialize Snowflake access: the cached connection is shared across the worker
# threads `asyncio.to_thread` runs queries on, and a single connection isn't safe
# for concurrent cursors. Demo traffic is light, so serializing reads is fine.
_sf_lock = threading.Lock()


def _private_key_der(pem_str):
    """Key-pair auth wants DER bytes; the secret stores the full PEM."""
    key = serialization.load_pem_private_key(pem_str.encode(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_private_key_der(os.environ["SNOWFLAKE_PRIVATE_KEY"]),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "SODOR_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SODOR_RAILWAY"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "TELEMETRY"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SODOR_CONNECTOR_ROLE"),
        client_session_keep_alive=True,
    )


def _query(sql):
    """Run a read query, reconnecting once if a cached connection went stale.

    Blocking — always call via `asyncio.to_thread`. Lock-guarded so concurrent
    requests don't share one connection's cursor state.
    """
    global _conn
    with _sf_lock:
        for attempt in range(2):
            try:
                if _conn is None or _conn.is_closed():
                    _conn = _connect()
                cur = _conn.cursor(snowflake.connector.DictCursor)
                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                finally:
                    cur.close()
                return [{k.lower(): v for k, v in row.items()} for row in rows]
            except snowflake.connector.errors.Error:
                _conn = None
                if attempt == 1:
                    raise


# ─── Kafka produce ───────────────────────────────────────────────────────────
#
# Reuses producer.py's logic. The Avro serializer runs read-only against the
# Schema Registry (auto.register off, use.latest.version on) — Terraform owns the
# schema; we serialize against whatever version is registered. Engine state is a
# module global so the long-lived process continues one random walk across ticks;
# the process start reseeds near the Island of Sodor.

SODOR_ENGINES = [
    "Thomas", "Edward", "Henry", "Gordon", "James",
    "Percy", "Toby", "Duck", "Donald", "Douglas",
    "Oliver", "Emily", "BoCo", "Daisy", "Bill", "Ben",
]


def _engine_name(i):
    return SODOR_ENGINES[i] if i < len(SODOR_ENGINES) else f"engine-{i + 1:03d}"


ENGINES = {
    _engine_name(i): {
        "lat": 54.25 + random.uniform(-0.2, 0.2),
        "lon": -4.5 + random.uniform(-0.2, 0.2),
        "heading": random.uniform(0, 360),
        "speed_kmh": random.uniform(60, 160),
    }
    for i in range(NUM_ENGINES)
}

_kafka = None
_kinesis = None  # cached (boto3 Kinesis client, stream_name); see _kinesis_client
# Guard the producer + ENGINES random walk: `asyncio.to_thread` can run two
# /produce calls (different viewers) on different threads at once, and an
# interleaved flush()/state mutation would corrupt the per-engine walk. Shared by
# BOTH backends — ENGINES is one global sim, so the Kinesis tick takes this lock too.
_produce_lock = threading.Lock()

# Monotonic timestamp of the last produce tick; read/written only under _produce_lock.
_last_produce_monotonic = None


class _Throttled(Exception):
    """Raised when /produce is called inside the cooldown window; the route turns it
    into a 429. Carries the seconds the caller should wait before retrying."""

    def __init__(self, retry_after):
        super().__init__("produce rate limited")
        self.retry_after = retry_after


def _enforce_rate_gate():
    """Process-wide cost gate, shared by both backends. MUST be called while
    holding _produce_lock. Coalesces calls that arrive inside the cooldown into a
    429 (via _Throttled) before touching any backend at all. The window is measured
    from the previous tick's START, so a slow flush/put doesn't stack extra delay on
    the next call. One cadence across Kafka + Kinesis is fine — the regulator only
    ever drives one source at a time."""
    global _last_produce_monotonic
    now = time.monotonic()
    if _last_produce_monotonic is not None:
        elapsed = now - _last_produce_monotonic
        if elapsed < MIN_PRODUCE_INTERVAL_S:
            raise _Throttled(MIN_PRODUCE_INTERVAL_S - elapsed)
    _last_produce_monotonic = now


def _kafka_clients():
    """Build (and cache) the producer + Avro serializer from the environment."""
    global _kafka
    if _kafka is None:
        producer = Producer(
            {
                "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": os.environ["PRODUCE_KAFKA_API_KEY"],
                "sasl.password": os.environ["PRODUCE_KAFKA_API_SECRET"],
                "client.id": "sodor-produce-live",
            }
        )
        sr = SchemaRegistryClient(
            {
                "url": os.environ["SCHEMA_REGISTRY_URL"],
                "basic.auth.user.info": (
                    f"{os.environ['PRODUCE_SCHEMA_REGISTRY_API_KEY']}:"
                    f"{os.environ['PRODUCE_SCHEMA_REGISTRY_API_SECRET']}"
                ),
            }
        )
        serializer = AvroSerializer(
            sr,
            SCHEMA_FILE.read_text(),
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        topic = os.environ.get("TOPIC", "sodor-telemetry")
        _kafka = (producer, serializer, StringSerializer("utf_8"), topic)
    return _kafka


def _kinesis_client():
    """Build (and cache) the boto3 Kinesis client + stream name from the env.

    Unlike the Kafka producer, there are NO keys here: boto3 authenticates from
    the ambient credential chain — the ECS task role on AWS (IAM-scoped to the
    stream ARN, see infra/kinesis.tf) and the dev's AWS_PROFILE locally. This is
    the one deliberate AWS-at-runtime departure; see notes/KINESIS.md."""
    global _kinesis
    if _kinesis is None:
        import boto3  # function-local: only the Kinesis path needs the SDK
        client = boto3.client("kinesis", region_name=os.environ.get("AWS_REGION"))
        _kinesis = (client, os.environ["KINESIS_STREAM_NAME"])
    return _kinesis


def _step(state):
    state["speed_kmh"] = max(0.0, state["speed_kmh"] + random.uniform(-5, 5))
    state["lat"] += random.uniform(-0.001, 0.001)
    state["lon"] += random.uniform(-0.001, 0.001)
    return state


def _build_event(engine_id, state):
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


def _produce_tick():
    """Produce one reading for every engine; flush so this tick's delivery errors
    are known before we respond. Rate-limited process-wide (see
    MIN_PRODUCE_INTERVAL_S) — raises _Throttled if called inside the cooldown.
    Blocking — call via `asyncio.to_thread`."""
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    with _produce_lock:
        _enforce_rate_gate()  # 429s inside the cooldown before touching Kafka

        producer, serialize, key_ser, topic = _kafka_clients()
        produced = 0
        for engine_id in ENGINES:
            ENGINES[engine_id] = _step(ENGINES[engine_id])
            event = _build_event(engine_id, ENGINES[engine_id])
            producer.produce(
                topic=topic,
                key=key_ser(engine_id),
                value=serialize(event, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=on_delivery,
            )
            produced += 1
        # A persistent process doesn't need the Lambda's freeze-driven flush, but
        # a synchronous /produce still flushes this tick so the response can report
        # delivery errors. Bounded so it can't wedge a worker thread.
        producer.flush(5)

    return produced, topic, errors


def produce_tick_kinesis():
    """Produce one reading for every engine to Kinesis as plain JSON — the
    swappable twin of _produce_tick. Reuses the SAME engine sim (ENGINES/_step/
    _build_event), so both backends emit the identical event dict; only the wire
    format differs (JSON here vs Avro on Kafka). Shares _produce_lock + the cost
    gate because ENGINES is one global random walk.

    PartitionKey = engine_id, the direct analogue of the Kafka message key: it
    routes a given engine's records to one shard, preserving per-engine order.
    Unlike Kafka there's no flush — put_record is synchronous and confirmed (shard
    id + sequence number) on return, so a failure is known immediately. Returns
    (produced, stream_name, errors) to mirror _produce_tick. Blocking — call via
    asyncio.to_thread."""
    errors = []
    with _produce_lock:
        _enforce_rate_gate()  # 429s inside the cooldown before touching Kinesis

        client, stream = _kinesis_client()
        produced = 0
        for engine_id in ENGINES:
            ENGINES[engine_id] = _step(ENGINES[engine_id])
            event = _build_event(engine_id, ENGINES[engine_id])
            try:
                client.put_record(
                    StreamName=stream,
                    Data=json.dumps(event).encode(),
                    PartitionKey=engine_id,
                )
                produced += 1
            except Exception as exc:  # one PutRecord failure shouldn't abort the tick
                errors.append(str(exc))

    return produced, stream, errors


# ─── Routes ──────────────────────────────────────────────────────────────────


@router.get("/latest")
async def latest():
    """Live view: latest reading per engine (returned as a bare JSON array)."""
    return await asyncio.to_thread(_query, f"SELECT * FROM {LATEST_VIEW}")


@router.get("/query")
async def query(name: str = Query(...)):
    view = QUERY_VIEWS.get(name)
    if not view:
        raise HTTPException(
            status_code=400,
            detail={"error": f"unknown query '{name}'", "available": list(QUERY_VIEWS)},
        )
    rows = await asyncio.to_thread(_query, f"SELECT * FROM {view}")
    return {"name": name, "rows": rows}


@router.post("/produce")
async def produce(source: str = Query("kafka")):
    """Emit one tick of telemetry to the SELECTED backend. ?source=kafka|kinesis;
    an unknown source falls back to Kafka. Same engine sim + cost gate either way."""
    tick = produce_tick_kinesis if source == "kinesis" else _produce_tick
    try:
        produced, target, errors = await asyncio.to_thread(tick)
    except _Throttled as exc:
        retry_after = max(1, math.ceil(exc.retry_after))
        return JSONResponse(
            status_code=429,
            content={"error": "rate limited", "retry_after_seconds": retry_after},
            headers={"Retry-After": str(retry_after)},
        )
    # Preserve the Kafka response's "topic" key; label the Kinesis target "stream".
    body = {"produced": produced, "source": source}
    body["stream" if source == "kinesis" else "topic"] = target
    if errors:
        body["errors"] = errors
        return JSONResponse(status_code=502, content=body)
    return body
