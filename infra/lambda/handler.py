"""
Serving-layer Lambda for the Sodor engine telemetry exercise — three routes,
one container image. This is what lets the whole demo run in the cloud with no
laptop (build-order step 7).

  POST /produce        produce one tick of Avro telemetry (all engines) to Kafka
  GET  /latest         live view: latest row per engine, read from Snowflake
  GET  /query?name=…   one of three canned, server-side analytics queries

Why a container image (not a zip + layer): this function needs BOTH
`confluent-kafka` (librdkafka C extension, for /produce) and
`snowflake-connector-python` (for the reads). Vendoring those manylinux wheels
into a zip is fiddly and brittle; a container image carries them cleanly. See
the Dockerfile and ../README.md.

Credentials never live in code or committed env files — they come from two AWS
Secrets Manager secrets (names in the SNOWFLAKE_SECRET_NAME / CONFLUENT_SECRET_NAME
env vars), each granted to this function's role least-privilege:

  snowflake secret  {account, user, private_key (full PEM), warehouse, database,
                     schema, role}
  confluent secret  {bootstrap_servers, kafka_api_key, kafka_api_secret,
                     schema_registry_url, schema_registry_api_key,
                     schema_registry_api_secret, topic}

A Lambda outside a VPC has default internet egress, so it can reach Confluent
Cloud (9092) and Snowflake (443) with no NAT gateway.
"""
import json
import os
import random
import uuid
from datetime import datetime, timezone

import boto3
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

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "telemetry.avsc")
NUM_ENGINES = int(os.environ.get("NUM_ENGINES", "5"))

_secrets = boto3.client("secretsmanager")


# ─── Secrets ─────────────────────────────────────────────────────────────────

def _secret(env_var):
    name = os.environ[env_var]
    return json.loads(_secrets.get_secret_value(SecretId=name)["SecretString"])


# ─── Snowflake reads ─────────────────────────────────────────────────────────
#
# All query logic lives in Snowflake VIEWS (defined in ../../snowflake), matching
# the TELEMETRY_LATEST precedent — this function holds NO business SQL, only a
# server-side whitelist of view names it may read, and runs `SELECT * FROM <view>`.
# That keeps the classic-vs-streaming RECORD_CONTENT seam entirely inside
# Snowflake: if step 6 schematizes the table, the views change in one place and
# this Lambda never does.
#
# The Lambda authenticates as the connector's key-pair user, which is granted
# SELECT on these views in ../../snowflake/analytics.tf. Reusing the connector
# identity is a deliberate demo simplification — production would use a separate
# read-only role.

# Live view: latest reading per engine.
LATEST_VIEW = "SODOR_RAILWAY.TELEMETRY.TELEMETRY_LATEST"

# Canned analytics — route ?name=… maps to a view. The value is a fixed
# identifier from this map (no raw SQL from the browser, no injection surface).
QUERY_VIEWS = {
    "speed_leaderboard": "SODOR_RAILWAY.TELEMETRY.SPEED_LEADERBOARD",
    "throughput_per_minute": "SODOR_RAILWAY.TELEMETRY.THROUGHPUT_PER_MINUTE",
    "doors_open_moving": "SODOR_RAILWAY.TELEMETRY.DOORS_OPEN_MOVING",
}

_conn = None


def _private_key_der(pem_str):
    """Key-pair auth wants DER bytes; the secret stores the full PEM."""
    key = serialization.load_pem_private_key(pem_str.encode(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    creds = _secret("SNOWFLAKE_SECRET_NAME")
    return snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        private_key=_private_key_der(creds["private_key"]),
        warehouse=creds.get("warehouse", "SODOR_WH"),
        database=creds.get("database", "SODOR_RAILWAY"),
        schema=creds.get("schema", "TELEMETRY"),
        role=creds.get("role", "SODOR_CONNECTOR_ROLE"),
        client_session_keep_alive=True,
    )


def _query(sql):
    """Run a read query, reconnecting once if a cached connection went stale."""
    global _conn
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
# module global so a warm container continues the random walk; a cold start
# reseeds near the Island of Sodor.

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


def _kafka_clients():
    """Build (and cache) the producer + Avro serializer from the secret."""
    global _kafka
    if _kafka is None:
        creds = _secret("CONFLUENT_SECRET_NAME")
        producer = Producer(
            {
                "bootstrap.servers": creds["bootstrap_servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": creds["kafka_api_key"],
                "sasl.password": creds["kafka_api_secret"],
                "client.id": "sodor-produce-lambda",
            }
        )
        sr = SchemaRegistryClient(
            {
                "url": creds["schema_registry_url"],
                "basic.auth.user.info": (
                    f"{creds['schema_registry_api_key']}:"
                    f"{creds['schema_registry_api_secret']}"
                ),
            }
        )
        with open(SCHEMA_FILE) as f:
            schema_str = f.read()
        serializer = AvroSerializer(
            sr,
            schema_str,
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        _kafka = (producer, serializer, StringSerializer("utf_8"), creds["topic"])
    return _kafka


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


# ─── Routes ──────────────────────────────────────────────────────────────────

def handle_latest():
    return _resp(200, _query(f"SELECT * FROM {LATEST_VIEW}"))


def handle_query(name):
    view = QUERY_VIEWS.get(name)
    if not view:
        return _resp(400, {"error": f"unknown query '{name}'",
                           "available": list(QUERY_VIEWS)})
    return _resp(200, {"name": name, "rows": _query(f"SELECT * FROM {view}")})


def handle_produce():
    producer, serialize, key_ser, topic = _kafka_clients()
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

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
    # Lambda freezes after the response, so flush synchronously — background
    # delivery can't be relied on once the container is suspended.
    producer.flush(10)

    body = {"produced": produced, "topic": topic}
    if errors:
        body["errors"] = errors
        return _resp(502, body)
    return _resp(200, body)


def _resp(status, body):
    # API Gateway owns the CORS headers — don't set them here (avoids duplicates).
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def handler(event, context):
    http = event.get("requestContext", {}).get("http", {})
    method = http.get("method", "GET")
    path = event.get("rawPath", "/")
    try:
        if method == "POST" and path.endswith("/produce"):
            return handle_produce()
        if method == "GET" and path.endswith("/latest"):
            return handle_latest()
        if method == "GET" and path.endswith("/query"):
            params = event.get("queryStringParameters") or {}
            return handle_query(params.get("name"))
        return _resp(404, {"error": f"no route for {method} {path}"})
    except Exception as exc:  # never leak a stack trace / secret to the caller
        print(f"ERROR handling {method} {path}: {exc!r}")
        return _resp(500, {"error": "internal error"})
