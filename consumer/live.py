"""
Live-tap state + Kafka consumer thread for the step-9 live view.

`confluent-kafka` is librdkafka under the hood — a blocking, synchronous client —
so the poll loop runs in its OWN background thread, never on the asyncio event
loop. It holds a latest-per-engine map in memory and fans each new reading out to
every connected SSE subscriber by hopping back onto the loop with
`call_soon_threadsafe`.

This is the production-faithful live path: the browser's live view is served
straight from Kafka (this consumer), independent of the Snowflake sink. The two
read the same topic under separate consumer groups — Kafka's fan-out made
physical, the whole point of step 9.
"""
import asyncio
import logging
import threading

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

log = logging.getLogger("sodor.live")


class LiveState:
    """Thread-safe latest-per-engine cache + SSE subscriber fan-out.

    `update()` is called from the consumer thread; `subscribe()`/`unsubscribe()`/
    `snapshot()` from the event loop. One lock guards both the map and the
    subscriber set.
    """

    def __init__(self):
        self._latest = {}             # engine_id -> event dict
        self._subscribers = set()     # set[asyncio.Queue], one per SSE client
        self._lock = threading.Lock()
        self._loop = None             # event loop, captured at startup

    def set_loop(self, loop):
        self._loop = loop

    def snapshot(self):
        """Latest reading for every engine seen so far (newest wins)."""
        with self._lock:
            return list(self._latest.values())

    def update(self, event):
        """Consumer-thread entry point: record a reading, fan it out to clients."""
        with self._lock:
            self._latest[event["engine_id"]] = event
            subscribers = list(self._subscribers)
        loop = self._loop
        if loop is None:  # consumer outran startup — state is still recorded
            return
        for q in subscribers:
            loop.call_soon_threadsafe(self._offer, q, event)

    @staticmethod
    def _offer(q, event):
        """Runs on the loop. Drop on a full queue — a slow client's loss; the
        snapshot-on-reconnect and heartbeat keep the live view self-healing."""
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def subscribe(self):
        q = asyncio.Queue(maxsize=256)
        with self._lock:
            self._subscribers.add(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            self._subscribers.discard(q)


def _build_consumer(cfg):
    return Consumer(
        {
            "bootstrap.servers": cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": cfg.kafka_api_key,
            "sasl.password": cfg.kafka_api_secret,
            "group.id": cfg.group_id,
            # Live tail: state lives in memory and we never replay history, so we
            # don't commit offsets. With nothing committed, every (re)start jumps
            # to the live edge (auto.offset.reset=latest). The cold-start gap —
            # engines reappear only as they next report — is the documented
            # trade-off (a log-compacted "…-latest" topic would close it; see the
            # step-9 reach note).
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )


def run_consumer(state, cfg, stop_event):
    """Poll loop — runs in a daemon thread. Deserializes Avro, updates state."""
    sr = SchemaRegistryClient(
        {
            "url": cfg.schema_registry_url,
            "basic.auth.user.info": f"{cfg.sr_api_key}:{cfg.sr_api_secret}",
        }
    )
    deserialize = AvroDeserializer(sr, cfg.schema_str)
    ctx = SerializationContext(cfg.topic, MessageField.VALUE)

    consumer = _build_consumer(cfg)
    consumer.subscribe([cfg.topic])
    log.info("live consumer subscribed to %s as group %s", cfg.topic, cfg.group_id)
    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Partition EOF is benign; surface anything else.
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("consumer error: %s", msg.error())
                continue
            try:
                event = deserialize(msg.value(), ctx)
            except Exception as exc:  # one poison record shouldn't wedge the loop
                log.warning("failed to deserialize message: %r", exc)
                continue
            if event:
                state.update(event)
    finally:
        consumer.close()
        log.info("live consumer closed")
