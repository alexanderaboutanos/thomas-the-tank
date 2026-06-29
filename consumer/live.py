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
import json
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
        self._meta = {}               # backend machinery readout (see update_meta)

    def set_loop(self, loop):
        self._loop = loop

    def snapshot(self):
        """Latest reading for every engine seen so far (newest wins)."""
        with self._lock:
            return list(self._latest.values())

    def update_meta(self, meta):
        """Consumer-thread entry point: publish this backend's live machinery —
        Kinesis shard/sequence/iterator-age, Kafka partition/offset — for the
        Stream Internals panel (served by GET /internals). The consumer thread
        REPLACES the dict each poll; readers get a consistent snapshot under lock."""
        with self._lock:
            self._meta = meta

    def meta(self):
        """Snapshot of the active backend's machinery (read by /internals)."""
        with self._lock:
            return dict(self._meta)

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
    messages_total = 0
    # The live tap reads all of the topic's partitions, each with its OWN
    # independent offset; keep the latest offset this consumer has reached per
    # partition so the panel shows every partition at once (one offset jumps around
    # only because messages round-robin across partitions — see notes/KINESIS.md).
    partition_offsets = {}        # partition -> latest offset consumed
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
                messages_total += 1
                partition_offsets[msg.partition()] = msg.offset()
                # Surface EVERY assigned partition, not just ones that have
                # delivered a message — so the panel reads a full 3-vs-3 against
                # Kinesis's 3 shards (which are always all enumerated). A partition
                # no engine keys into stays at a null offset, the exact twin of an
                # empty Kinesis shard's null sequence number. assignment() is the
                # local cached set, so this is cheap to re-check each message.
                for tp in consumer.assignment():
                    partition_offsets.setdefault(tp.partition, None)
                # Publish Kafka's machinery for the Stream Internals panel — each
                # partition's offset is the direct twin of a Kinesis shard's
                # sequence number (see update_meta and notes/KINESIS.md).
                state.update_meta(
                    {
                        "backend": "kafka",
                        "topic": cfg.topic,
                        "group_id": cfg.group_id,
                        "messages_total": messages_total,
                        "partitions": [
                            {"partition": p, "offset": partition_offsets[p]}
                            for p in sorted(partition_offsets)
                        ],
                    }
                )
    finally:
        consumer.close()
        log.info("live consumer closed")


def run_kinesis_consumer(state, cfg, stop_event):
    """Poll loop for the Kinesis backend — the swappable twin of run_consumer,
    running in its own daemon thread. Reads plain-JSON telemetry from the live
    edge of every shard and updates the SAME transport-agnostic state the Kafka
    path feeds, so LiveState / the SSE layer / the web table need zero change.

    DELIBERATE DEPARTURE: this is the one place the app touches AWS at runtime.
    boto3 resolves credentials from the ambient chain — the ECS task role on AWS
    (IAM-scoped to exactly this stream's ARN, no keys in env; see infra/kinesis.tf)
    and the developer's AWS_PROFILE locally. Everywhere else the app still receives
    its secrets as plain env vars. notes/KINESIS.md explains why this is worth it.

    JSON, not Avro: the Kafka path keeps its Schema Registry contract; the Kinesis
    path carries `json.loads(record)` for practice. AWS Glue Schema Registry is the
    named productionizing step.
    """
    import boto3  # function-local: the Kafka-only path never needs the SDK

    client = boto3.client("kinesis", region_name=cfg.aws_region)
    stream = cfg.kinesis_stream_name

    def latest_iterators():
        """A LATEST shard iterator per shard. The iterator is the cursor Kafka
        keeps server-side as a committed offset — here it lives in this loop, and
        we re-take it from LATEST on every (re)acquire, so like the Kafka consumer
        (auto.offset.reset=latest) we tail new readings only and never replay."""
        shards = client.list_shards(StreamName=stream)["Shards"]
        return {
            s["ShardId"]: client.get_shard_iterator(
                StreamName=stream, ShardId=s["ShardId"], ShardIteratorType="LATEST"
            )["ShardIterator"]
            for s in shards
        }

    iterators = {}            # shard_id -> shard iterator; empty ⇒ (re)acquire
    records_total = 0
    # Per-shard machinery for the Stream Internals panel — each shard, like a Kafka
    # partition, carries its OWN independent sequence number. Persists across poll
    # cycles (a shard that drew no records this poll keeps its last-known sequence
    # rather than blanking). shard_id -> {sequence_number, millis_behind_latest,
    # records_last_poll, records_total}.
    shard_state = {}
    log.info("live kinesis consumer starting on stream %s", stream)
    try:
        while not stop_event.is_set():
            try:
                if not iterators:
                    iterators = latest_iterators()
                    log.info("kinesis: tailing %d shard(s) from LATEST", len(iterators))
                for shard_id in list(iterators):
                    resp = client.get_records(ShardIterator=iterators[shard_id], Limit=200)
                    iterators[shard_id] = resp["NextShardIterator"]
                    records = resp["Records"]
                    st = shard_state.setdefault(
                        shard_id,
                        {"sequence_number": None, "millis_behind_latest": None,
                         "records_last_poll": 0, "records_total": 0},
                    )
                    for rec in records:
                        try:
                            event = json.loads(rec["Data"])
                        except Exception as exc:  # one poison record mustn't wedge the loop
                            log.warning("failed to decode kinesis record: %r", exc)
                            continue
                        state.update(event)
                        records_total += 1
                        st["records_total"] += 1
                        st["sequence_number"] = rec["SequenceNumber"]
                    # MillisBehindLatest is the iterator age — how far behind the tip
                    # this read sits (Kinesis's analogue of per-partition consumer lag);
                    # it comes back per GetRecords even when the batch is empty.
                    st["millis_behind_latest"] = resp["MillisBehindLatest"]
                    st["records_last_poll"] = len(records)
                # ONE readout per full shard sweep, every shard at once — the twin of
                # the Kafka panel's per-partition offsets (see notes/KINESIS.md).
                state.update_meta(
                    {
                        "backend": "kinesis",
                        "stream_name": stream,
                        "iterator_type": "LATEST",
                        "records_total": records_total,
                        "shards": [
                            {"shard_id": sid, **shard_state[sid]}
                            for sid in sorted(shard_state)
                        ],
                    }
                )
            except client.exceptions.ExpiredIteratorException:
                # The iterator's ~5-min TTL lapsed; drop them so the next cycle
                # re-takes at the live edge. We never replay, so this loses nothing.
                log.info("kinesis shard iterator expired; re-acquiring at LATEST")
                iterators = {}
            except Exception as exc:  # transient AWS/network error mustn't kill the thread
                log.warning("kinesis poll failed, retrying: %r", exc)
                iterators = {}
            # One cycle/sec stays under the 5 GetRecords-TPS-per-shard limit and
            # idles politely; wait() (not sleep) returns at once on shutdown.
            stop_event.wait(1.0)
    finally:
        log.info("live kinesis consumer stopped")
