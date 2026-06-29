# Kinesis

*the same ideas as kafka, different names — a concept map from one to the other*

this project streams the same train telemetry through two backends behind a lever:
confluent kafka and aws kinesis. the event dict is identical either way; only the
transport changes. so the fastest way to understand kinesis is to hold it up against
kafka (see [KAFKA.md](KAFKA.md)) and note what's the same and what's renamed.

## Terms

- stream - named, ordered data stream living in your aws account/region. the **topic** of kinesis. (one stream = one logical feed; you can have many streams in a region, same as many topics on a cluster.)
- shard - a stream is split into shards; each shard is the unit of *throughput* and the unit of *ordering*. the **partition** of kinesis. fixed capacity per shard: 1 MB/s or 1000 records/s in, 2 MB/s out, 5 `GetRecords` calls/s.
- partition key - a string you attach to each record; kinesis MD5-hashes it to pick the shard. same key → same shard → per-key order. the **message key** of kinesis (here it's `engine_id`, e.g. `Thomas`, exactly like the kafka side).
- sequence number - a big, per-record id that increases within a shard, assigned by kinesis at write time. the **offset** of kinesis — but it's an opaque ever-growing number, not a tidy 0,1,2 counter, and it's per-shard.
- shard iterator - a cursor you fetch with `get_shard_iterator`, hand to `get_records`, and advance via the `NextShardIterator` it returns. it's where you are in a shard. expires after **5 minutes** if you stop using it.
- retention - how long records stay readable: **24h** by default, up to 365 days. kafka's topic retention. like kafka (and unlike SQS) a read doesn't destroy the record — many consumers read the same shard independently at their own positions.
- application / consumer - kinesis has no built-in "consumer group" that commits offsets for you. you either hold the iterator yourself (what this app does) or use the **KCL** (kinesis client library), which fakes a consumer group with a DynamoDB table for checkpoints + shard leases.

Quick Hierarchy

- *stream* split into ─►  shards
- *shard* has fixed ─►  throughput (1 MB/s in, 2 MB/s out)
- *partition key* hashes to ─►  one shard
- *record* lands at ─►  a sequence number within its shard

## The mapping (kafka ↔ kinesis)

this table *is* the doc. everything below just expands a row.

| kafka | kinesis | note |
|---|---|---|
| topic | stream | the named feed |
| partition | shard | ordering + parallelism/throughput unit |
| message key | partition key | hashes to one partition/shard; here `engine_id` |
| offset | sequence number | position within a partition/shard |
| committed offset (server-side) | shard iterator (client-held) | "where am i" — kafka stores it for you, kinesis you carry it |
| `auto.offset.reset` (latest/earliest) | iterator type (`LATEST`/`TRIM_HORIZON`/…) | where a fresh reader starts |
| `subscribe()` + `poll()` | `get_shard_iterator` + `get_records` | how you actually read |
| consumer group (independent fan-out) | enhanced fan-out / separate apps | many readers, each with the full feed |
| add partitions (one-way) | resharding: split **or** merge | change the parallelism |
| Avro + Confluent Schema Registry | (here) plain JSON; (prod) Avro + Glue Schema Registry | the wire contract |

## Ordering

same story as kafka, different nouns. order is guaranteed only *within a shard*, not
across the stream. keying by `engine_id` keeps each engine's readings on one shard, in
order, while different engines spread across shards for throughput. the only twist:
kafka picks the partition with `hash(key) % num_partitions`; kinesis hashes the key into
a 128-bit space and each shard owns a *range* of that space. same outcome — same key,
same shard, ordered — just a range lookup instead of a modulo.

## Iterator types (≈ `auto.offset.reset`)

where do you start reading a shard? you say so when you fetch the iterator:

- `LATEST` - only records that arrive from now on. kafka's `auto.offset.reset=latest`. **this is what the app uses** — the live tap tails the edge, never replays, exactly like the kafka consumer (`enable.auto.commit=false` + `latest`).
- `TRIM_HORIZON` - the oldest record still retained. kafka's `earliest`.
- `AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER` - start at/after a specific sequence number — i.e. resume from a checkpoint you saved.
- `AT_TIMESTAMP` - start at a wall-clock time. kafka has the offsets-for-times lookup for this.

kafka only really gives you latest/earliest out of the box; kinesis bakes the
sequence-number and timestamp seeks into the iterator call.

## subscribe/poll ≈ get_shard_iterator / get_records

the kafka consumer is one call — `subscribe([topic])` then `poll()` in a loop, and the
client library handles partitions and offsets under the hood. kinesis (the plain
`GetRecords` path) is more hands-on and *per shard*:

1. `list_shards` to find the shards.
2. `get_shard_iterator(shard, LATEST)` for each — your starting cursor.
3. `get_records(iterator)` → a batch of records **plus** a `NextShardIterator` **plus** `MillisBehindLatest`.
4. feed `NextShardIterator` back into the next `get_records`. repeat. sleep ~1s so you stay under the 5-calls/s/shard ceiling.

this stream runs **3 shards** — deliberately matching the kafka topic's 3 partitions so
the lever is a true side-by-side (3 offsets ↔ 3 sequence numbers in the Stream Internals
panel), not because the volume needs it. so the loop holds 3 iterators and sweeps all of
them each cycle, then publishes one readout. `MillisBehindLatest` comes back per shard: how
far behind the tip that shard's batch is — the **iterator age**, kinesis's per-shard version
of consumer lag. the panel puts each shard's sequence number + age on screen.

## Resharding (the big difference from kafka)

kafka partitions only go *up* — you can add partitions, never remove them. kinesis lets
you **split** a shard (more throughput) or **merge** two adjacent shards (less), so shard
count moves both directions. that's the nicer story.

the catch is the same as kafka's repartitioning, though: resharding redraws the hash-key
ranges, so a key can land on a different shard after the reshard. the old shard doesn't
vanish — it becomes a **closed (parent) shard** that stays readable until retention
expires, with pointers to its **child** shards. a correct consumer drains a parent fully,
then follows to the children, so per-key order survives the boundary. (the KCL does this
for you; our LATEST-only tail sidesteps it by never replaying history.)

## Limits (per shard — memorize these)

- **write:** 1 MB/s **or** 1000 records/s, whichever you hit first.
- **read (shared):** 2 MB/s, and at most **5 `GetRecords` calls/s**. that 5-TPS ceiling is why the consumer loop sleeps ~1s — and why a stream with N shared consumers splits that 2 MB/s between them.
- **enhanced fan-out (EFO):** register a consumer and it gets its *own* dedicated 2 MB/s/shard push pipe via `SubscribeToShard`, instead of sharing the shared 2 MB/s. EFO is the real analogue of kafka consumer **groups** — each group/consumer reads the whole feed independently at full bandwidth. (this app uses the cheaper shared `GetRecords`; one slow demo consumer doesn't need its own pipe.)

`PutRecord` writes one record; `PutRecords` batches up to 500 records / 5 MB in a call —
the throughput knob when you're pushing volume. the app uses `PutRecord` per engine
(5 a tick — nowhere near any limit) so each write's shard id + sequence number come back
individually.

## Questions

Why plain JSON here, when the kafka side has Avro + a schema registry?
- for practice, and because the event dict is already JSON-friendly — `json.dumps(event).encode()` on the way in, `json.loads` on the way out, and the whole downstream (the in-memory state, the SSE layer, the web table) doesn't change one line.
- but you lose what Avro bought on the kafka side: a compact binary payload (JSON repeats every field name on every record) and a *typed contract* enforced at serialize time.
- so what's the real-world fix? **AWS Glue Schema Registry.** it's the kinesis-world counterpart to confluent's schema registry: you register the Avro schema in Glue, the producer serializes against it, consumers (or Firehose) deserialize against it, and you get the same governance the kafka path already has. named here as the productionizing step, not built — that's the honest answer.

Why does the app suddenly make AWS calls at runtime? i thought it didn't.
- right — everywhere else the container makes *no* AWS calls. secrets are handed to it as env vars by the platform (ECS injects them from Secrets Manager), the task role carries no permissions, no boto3. that was a deliberate clean boundary: "the platform" delivers creds, "the app" just reads env.
- kinesis breaks that boundary on purpose. boto3 in the container calls kinesis directly, and it authenticates as the **ECS task role** — temporary credentials boto3 picks up automatically from the container credential endpoint, no keys in env. locally it's whatever `AWS_PROFILE` you've exported.
- the task role is scoped tight: an inline IAM policy grants only the six actions this app uses (`PutRecord`, `PutRecords`, `GetRecords`, `GetShardIterator`, `ListShards`, `DescribeStreamSummary`) on exactly this one stream's ARN. least privilege, keyless, real. it's a departure worth showing, not hiding.

Where does the "consumer group" go? kafka's fan-out was the whole point of step 9.
- kinesis doesn't have the word, but it has the idea twice. cheap version: any number of apps can run their own `GetRecords` loop on the same shard at their own iterators — independent reads, like separate kafka groups, just sharing the shard's 2 MB/s.
- real version: **enhanced fan-out** — register each consumer and each gets a dedicated 2 MB/s/shard pipe pushed to it. that's the true peer of kafka consumer groups when you care about throughput isolation.

Is a sequence number just an offset?
- close, and the right intuition — both answer "where am i in this shard/partition." but a kafka offset is a small per-partition counter (0, 1, 2, …); a kinesis sequence number is a long, opaque, ever-increasing id assigned by the service. you don't compute the next one, you read it off the record. and you don't *commit* it back to a broker — if you want to resume from it, you stash it yourself (DynamoDB, via the KCL) and pass it as an `AT_SEQUENCE_NUMBER` iterator next time.

Why does the consumer sleep a second between polls?
- the 5-`GetRecords`-calls-per-second-per-shard limit. poll a single shard in a tight loop and you'll trip `ProvisionedThroughputExceeded` fast. ~1s/cycle keeps a healthy margin and, at this demo's ~5 records/2s, never falls behind — `MillisBehindLatest` stays near zero.
