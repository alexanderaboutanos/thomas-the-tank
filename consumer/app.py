"""
FastAPI serving app for the Sodor live-tap (build-order step 9).

One always-on container that serves the live view straight from Kafka over SSE —
the production-faithful split (Kafka → live, Snowflake → analytics) that closes
step 6's freshness gap. The step-7 serving Lambda's routes (/produce, /latest,
/query) are now FOLDED into this same FastAPI server (see serving.py) as the
Lambda + API Gateway are retired — one container serves everything:

  GET /stream    Server-Sent Events: a snapshot-on-connect, then one event per
                 new engine reading, with a ~20s heartbeat for idle keep-alive.
  POST /produce  produce one tick of Avro telemetry to Kafka            (serving.py)
  GET /latest    live view: latest row per engine, from Snowflake       (serving.py)
  GET /query     one of three canned analytics queries, from Snowflake  (serving.py)
  GET /healthz   liveness probe for the ALB target group.
  GET /          a localhost test page (same-origin EventSource — no mixed-content).

Blocking work (the Kafka consumer, plus the folded Kafka/Snowflake calls) stays
OFF the event loop: the consumer runs in its own thread (see live.py) and the
folded routes hop to worker threads with `asyncio.to_thread` (see serving.py).
Config is environment-only (local: consumer/.env; cloud: ECS injects the Secrets
Manager values as env vars) — no boto3, no in-app fetch.
"""
import asyncio
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, StreamingResponse

import serving
from live import LiveState, run_consumer, run_kinesis_consumer

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sodor.app")

HERE = Path(__file__).parent
HEARTBEAT_S = float(os.environ.get("HEARTBEAT_SECONDS", "20"))


class Config:
    """Live consumer config, read from the environment (consumer/.env locally)."""

    def __init__(self):
        self.bootstrap_servers = os.environ["BOOTSTRAP_SERVERS"]
        self.topic = os.environ.get("TOPIC", "sodor-telemetry")
        self.group_id = os.environ.get("GROUP_ID", "sodor-live-consumer")
        self.kafka_api_key = os.environ["LIVE_KAFKA_API_KEY"]
        self.kafka_api_secret = os.environ["LIVE_KAFKA_API_SECRET"]
        self.schema_registry_url = os.environ["SCHEMA_REGISTRY_URL"]
        self.sr_api_key = os.environ["LIVE_SCHEMA_REGISTRY_API_KEY"]
        self.sr_api_secret = os.environ["LIVE_SCHEMA_REGISTRY_API_SECRET"]
        self.schema_str = (HERE / "telemetry.avsc").read_text()
        # Kinesis backend (the swappable second stream) — OPTIONAL. Absent both
        # vars, the app boots Kafka-only and the Kinesis consumer never starts.
        # boto3 reads credentials from the task role / AWS_PROFILE, not from here.
        self.aws_region = os.environ.get("AWS_REGION")
        self.kinesis_stream_name = os.environ.get("KINESIS_STREAM_NAME")


# One LiveState per backend; both consumers fill their own from boot, so the
# front-end lever flips between two already-warm streams instantly. STATES.get(...)
# falls back to Kafka for any unknown ?source=.
STATES = {"kafka": LiveState(), "kinesis": LiveState()}


@asynccontextmanager
async def lifespan(app):
    # Capture the loop BEFORE the consumer threads start so their first message can
    # always fan out (update() needs the loop for call_soon_threadsafe).
    loop = asyncio.get_running_loop()
    for st in STATES.values():
        st.set_loop(loop)
    cfg = Config()
    stop = threading.Event()  # one event stops both consumers on shutdown

    threads = [
        threading.Thread(
            target=run_consumer,
            args=(STATES["kafka"], cfg, stop),
            name="kafka-consumer",
            daemon=True,
        )
    ]
    # The Kinesis consumer only runs when the stream is configured. Absent
    # KINESIS_STREAM_NAME the app stays Kafka-only and the lever's Kinesis side is
    # simply empty — the existing Avro path is never disturbed.
    if cfg.kinesis_stream_name:
        threads.append(
            threading.Thread(
                target=run_kinesis_consumer,
                args=(STATES["kinesis"], cfg, stop),
                name="kinesis-consumer",
                daemon=True,
            )
        )
    else:
        log.info("KINESIS_STREAM_NAME unset — Kinesis consumer not started (Kafka-only)")

    for t in threads:
        t.start()
    log.info("live-tap consumer threads started: %s", [t.name for t in threads])
    try:
        yield
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
        log.info("live-tap consumer threads stopped")


app = FastAPI(title="Sodor live tap", lifespan=lifespan)

# Folded step-7 serving routes (/produce, /latest, /query) — see serving.py.
app.include_router(serving.router)


def _sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@app.get("/healthz")
async def healthz():
    # Kafka count keeps the probe's historical shape (Kafka is always running).
    return {"status": "ok", "engines": len(STATES["kafka"].snapshot())}


@app.get("/stream")
async def stream(request: Request, source: str = Query("kafka")):
    """SSE: snapshot-on-connect, then live per-engine updates + idle heartbeats.

    ?source=kafka|kinesis selects which backend's stream to tail; an unknown
    source falls back to Kafka. Both backends run from boot, so the flip is just
    a reconnect against an already-warm LiveState."""
    src_state = STATES.get(source, STATES["kafka"])

    async def gen():
        q = src_state.subscribe()
        try:
            # Snapshot-on-connect: hand a new client the full latest-per-engine
            # state at once, so it isn't blank until every engine next reports.
            yield _sse("snapshot", src_state.snapshot())
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=HEARTBEAT_S)
                    yield _sse("update", event)
                except asyncio.TimeoutError:
                    # Comment line: keeps the connection (and any proxy idle timer)
                    # alive when every engine is quiet.
                    yield ": heartbeat\n\n"
        finally:
            src_state.unsubscribe(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # don't let a proxy buffer the stream
        },
    )


@app.get("/internals")
async def internals(source: str = Query("kafka")):
    """Stream Internals: the active backend's live machinery, so the lever proves
    real backend-specific knowledge instead of "they look identical."

      kinesis -> stream, shard id, sequence number, iterator age (ms behind
                 latest), records/poll
      kafka   -> topic, group, partition, offset, messages seen

    The consumer thread keeps this fresh via LiveState.update_meta on every poll;
    this read-only handler just hands back the latest snapshot. The web panel polls
    it ~every 2s for the active source. Unknown sources fall back to Kafka."""
    return STATES.get(source, STATES["kafka"]).meta()


@app.get("/")
async def index():
    return FileResponse(HERE / "static" / "index.html")
