"""
FastAPI serving app for the Sodor live-tap (build-order step 9).

One always-on container that serves the live view straight from Kafka over SSE —
the production-faithful split (Kafka → live, Snowflake → analytics) that closes
step 6's freshness gap. The step-7 serving Lambda's routes (/produce, /latest,
/query) fold into THIS app on the same FastAPI server as the Lambda + API Gateway
are retired (see the build order). This file currently carries the live-tap core;
the folded routes land alongside in a later increment.

  GET /stream    Server-Sent Events: a snapshot-on-connect, then one event per
                 new engine reading, with a ~20s heartbeat for idle keep-alive.
  GET /healthz   liveness probe for the ALB target group.
  GET /          a localhost test page (same-origin EventSource — no mixed-content).

Blocking work (Kafka, and later Snowflake) stays OFF the event loop: the consumer
runs in its own thread (see live.py); folded blocking calls will use
`asyncio.to_thread`. Config is environment-only (local: consumer/.env; cloud: ECS
injects the Secrets Manager values as env vars) — no boto3, no in-app fetch.
"""
import asyncio
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse

from live import LiveState, run_consumer

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


state = LiveState()


@asynccontextmanager
async def lifespan(app):
    # Capture the loop BEFORE the consumer thread starts so its first message can
    # always fan out (update() needs the loop for call_soon_threadsafe).
    state.set_loop(asyncio.get_running_loop())
    cfg = Config()
    stop = threading.Event()
    thread = threading.Thread(
        target=run_consumer, args=(state, cfg, stop), name="kafka-consumer", daemon=True
    )
    thread.start()
    log.info("live-tap consumer thread started")
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)
        log.info("live-tap consumer thread stopped")


app = FastAPI(title="Sodor live tap", lifespan=lifespan)


def _sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "engines": len(state.snapshot())}


@app.get("/stream")
async def stream(request: Request):
    """SSE: snapshot-on-connect, then live per-engine updates + idle heartbeats."""

    async def gen():
        q = state.subscribe()
        try:
            # Snapshot-on-connect: hand a new client the full latest-per-engine
            # state at once, so it isn't blank until every engine next reports.
            yield _sse("snapshot", state.snapshot())
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
            state.unsubscribe(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # don't let a proxy buffer the stream
        },
    )


@app.get("/")
async def index():
    return FileResponse(HERE / "static" / "index.html")
