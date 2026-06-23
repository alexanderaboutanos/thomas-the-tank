# Snowflake

*how telemetry lands in the warehouse, and the two ways the connector can put it there*

## Terms


- snowflake warehouse - a *compute* cluster that runs queries on stored data, sized XSMALL→6XL, auto-suspends when idle so you stop paying. NOT where data lives — it's the engine that processes it. (`SODOR_WH`).
  - point of confusion... a snowflake warehouse IS NOT a traditional data warehouse. these are not the same thing.
  - a traditional data warehouse is a central repository that consolidates structured, cleaned, query-ready data from many sources into tables, optimized for analytics (OLAP), while a snowflake warehouse is only about COMPUTE, not STORAGE.
  - the unique thing about snowflake is that they separated COMPUTE from STORAGE. In a traditional database, the thing that runs the query (the compute) is directly tied to the thing that holds the data (the STORAGE). Snowflake separated them. So, you store with snowflake, yes, in what they call databases. But when you want to fire up an analytics query, you fire up a compute cluster (which they have decided to call a snowflake warehouse). They essentially narrowed the term warehouse to mean just COMPUTE rather than COMPUTE and STORAGE.
  - one result is that you can have multiple different compute clusters (warehouses) striking a single table. 


- snowflake database - *just a namespace*. the top-level container; a folder that groups *schemas* together, nothing more.
- snowflake schema - *just a namespace*. this is not a database schema (like strict SQL) or a payload schema (like that in Confluent Schema Registry). it lives *inside* a database and is just a folder that groups *tables/views* together.

- snowflake table - *not just a namespace*. this is where the rows of data actually live
- VARIANT - a column type that holds semi-structured data (json-ish).
- view - a saved query that looks like a table. 
- role - a bag of privileges. you grant privileges to a *role*, then grant the *role* to a *user*. the connector gets least privilege, only what it needs to load the table.
- key-pair user - a service user that signs in with an RSA private key instead of a password. snowflake holds the public key; the client holds the private key + signs a JWT. passwords are being retired for programmatic access, so this is the newer way.
- stage / pipe - Snowpipe plumbing. a *stage* is a staging area for files; a *pipe* auto-COPYs staged files into a table. the connector creates these itself.

## Hierarchy

```
Account
├── Warehouse   — compute (SODOR_WH), sits *beside* the data, not above it
└── Database    (SODOR_RAILWAY)
    └── Schema  (TELEMETRY)
        └── Table  (TELEMETRY_RAW)
```

## The sink connector

- "sink" = data flows *into* snowflake (a "source" would feed kafka instead).
- "managed" = confluent runs it; we just hand it config (which topic, which table, the snowflake creds).
- under the hood it's a kafka *consumer* — reads `sodor-telemetry`, writes `TELEMETRY_RAW`. so it needs a consumer group + READ on the topic, same as any consumer.

## The two ingestion methods

*the fork, applied to **our** pipeline — generic tradeoffs are in the pros/cons below; here's how the connector actually lands a message.*

### classic Snowpipe (step 5 — how we started)

- every message lands as **two VARIANT columns**:
  - `RECORD_CONTENT` - the parsed message value (the whole avro record, as a json-ish blob)
  - `RECORD_METADATA` - kafka bookkeeping (topic, partition, offset, timestamp)
- the real fields (engine_id, speed, …) live *inside* `RECORD_CONTENT`. to get tidy columns you write a **view** (`TELEMETRY_LATEST`).
- *mental model: the mail room dumps every letter in one bin; the view is your rule for reading the addresses off them.*
- the catch: file-based, so the warehouse view lagged tens of seconds — too slow for a live on/off switch. that's what step 6 fixes.

### Snowpipe Streaming (step 6 — what we're using now)

- same connector, one config flip: `snowflake.ingestion.method = SNOWPIPE_STREAMING`. rows are written **straight into the table** via the streaming API — no files, no stage, no pipe-driven COPY.
- grants shift with it: streaming uses no stage, so the connector role drops `CREATE STAGE` (least privilege). everything else it needs stays.
- **landing shape is unchanged.** streaming still lands the same two VARIANT columns, so `TELEMETRY_LATEST` and the serving Lambda's query (step 7) are untouched. *the mail room got faster; the letters look the same.*
- **but the freshness goal isn't fully met — step 6 is left open.** measured end-to-end: **~3s right after a flush, ~30s just before the next, avg ~15s.** much better than classic's minute+, but not the single-digit "live" target. the **fully-managed connector pins the streaming client-lag at its 30s default** and silently drops the knobs that would lower it (`snowflake.streaming.max.client.lag`, `enable.single.buffer` — they never land in Terraform state; `buffer.flush.time` is accepted but bypassed by the connector's locked single-buffer mode). true single-digit needs a self-managed connector or the step-9 direct-Kafka tap. *the warehouse path tops out at "near-real-time", not "live" — which is exactly why the faithful live path reads Kafka.*

### schematization — a separate knob we deliberately left OFF

- "schematization" (`snowflake.enable.schematization = true`, default false) is **orthogonal to the ingestion method** — it works under classic Snowpipe too. it makes the connector read the avro schema and **auto-create real typed columns** (`ENGINE_ID STRING`, `SPEED_KMH FLOAT`, …): no VARIANT blob, no view needed.
- cost: it **couples the snowflake table to the avro schema** — evolve the schema and the columns shift under you. more magic, more to reason about, and it would break the `TELEMETRY_LATEST` contract the step-7 serving layer reads.
- so step 6 buys the latency win *without* taking on that coupling. flipping schematization on is a clean future experiment, not part of this step.

### why classic first (the step 5 → 6 arc)

- it's the default + the canonical "learn the connector" path.
- the VARIANT→view pattern is a genuinely useful snowflake skill (querying semi-structured data).
- best learning arc: got it flowing the simple way in step 5, then switched to streaming in step 6 and *felt* the latency drop. that contrast is the lesson.

## Classic vs Streaming — pros & cons

Worth being precise about terminology, because "Classic vs Streaming" in Snowflake maps to two genuinely different architectures, not just two speeds of the same thing.

- **Classic ingestion** = *file-based*. Either bulk `COPY INTO` (you drive a warehouse) or **Snowpipe** (serverless, auto-triggered by cloud-storage notifications). Either way, data must land as **files in a stage** first, then get loaded.
- **Streaming ingestion** = **Snowpipe Streaming API**. Rows are written **directly into the table** via a client SDK (or the Kafka connector in Snowpipe Streaming mode) — **no intermediate files**.

That "files vs rows" distinction is the root cause of every pro and con below.

### Classic / File-based (COPY + Snowpipe)

**Pros**

- **Mature and simple mental model.** Files → stage → load. Easy to reason about, debug, and audit; the staged files are a natural replay/recovery buffer.
- **Cost-efficient at high volume per batch.** Loading large files amortizes overhead well — ideal for backfills, historical loads, and scheduled batch ETL.
- **Plays nicely with existing pipelines** that already emit files (S3/GCS/Azure exports, dumps, partitioned Parquet).
- **Serverless option (Snowpipe)** removes warehouse management; `COPY` gives you explicit control when you want it.

**Cons**

- **Latency floor of ~1 minute+.** Snowpipe reacts to file-arrival events; you'll never get sub-minute freshness. Not real-time.
- **The small-file problem.** Per-file overhead means many tiny files are slow *and* expensive. You're forced to micro-batch upstream to hit a healthy file size (~100–250 MB), which is operational toil.
- **Staging infrastructure required** — cloud bucket plus event notifications (SQS / Event Grid / Pub/Sub) to wire up and maintain.
- **Latency/cost are coupled to file cadence**, so tuning freshness means re-tuning batching.

### Streaming / Snowpipe Streaming API

**Pros**

- **Seconds-level latency.** Rows become queryable in roughly single-digit seconds — suitable for event-driven, IoT, CDC, and dashboards that need freshness.
- **No files, no small-file problem.** You append rows (or row-sets) directly, which is exactly why it's cheaper for **high-frequency, low-volume-per-message** workloads — there's no per-file tax.
- **First-class Kafka integration.** The Snowflake Kafka connector in Snowpipe Streaming mode is the natural sink for a Confluent/Kafka pipeline — directly relevant to where this repo is heading.
- **Exactly-once via offset tokens / channels**, so you get clean resumability and dedup on retries.

**Cons**

- **More integration effort.** You need the client SDK (or Kafka connector) and must manage **channels**, offsets, and per-channel ordering — heavier than dropping files in a bucket.
- **More involved error/schema handling.** Schema evolution, malformed-row routing, and channel lifecycle are application concerns you own.
- **Not the cheapest for pure bulk.** For massive one-shot loads, `COPY` on a big warehouse still wins — streaming shines on continuous trickle, not giant batches.
- **Newer surface area.** Fewer years of operational lore than classic Snowpipe (though the newer high-performance streaming architecture has closed much of the gap).

### Rule of thumb


| Your situation                                                 | Use                    |
| -------------------------------------------------------------- | ---------------------- |
| Backfills, historical loads, large scheduled batches           | **`COPY` (Classic)**   |
| Periodic file drops, minutes of latency acceptable, serverless | **Snowpipe (Classic)** |
| Continuous events, Kafka source, seconds of latency needed     | **Snowpipe Streaming** |


Given this project is a Kafka/Confluent pipeline, **Snowpipe Streaming via the Kafka connector** is the architecturally consistent choice — it preserves the event-by-event nature of the stream end-to-end instead of forcing it back into files at the Snowflake boundary. The one caveat: if you ever need to bulk-load history into the same tables, reach for `COPY` for that one-off rather than trying to push it all through the streaming API.

## Questions

why was the warehouse view near-real-time under classic, not sub-second?

- classic Snowpipe (step 5) buffered messages into files and the pipe ingested on its own cadence. that buffering *was* the latency — a cost/throughput trade, fewer/bigger loads being cheaper than a constant trickle.
- step 6 swapped that for Snowpipe Streaming (above), which appends rows directly and improves the view's freshness to ~3–30s (near-real-time). it stops short of single-digit because the managed connector caps the flush at ~30s. the remaining lower-latency option — genuinely sub-second — is tapping kafka directly with a websocket/SSE bridge (the step-9 reach goal), which skips the warehouse on the live path entirely.

why key-pair auth instead of a password?

- snowflake is retiring single-factor passwords for programmatic logins (MFA now required for humans). a service user authenticates with an RSA key pair: snowflake stores the public key, the client signs a JWT with the private key.
- bonus: no password sitting in connector config to leak — though now you guard a private key instead.

three shapes of the same key — don't mix them up:

- terraform provider (`private_key`) wants the **full PEM** (`-----BEGIN…` and all), read via `file()`.
- the connector (`snowflake.private.key`) and the snowflake user (`rsa_public_key`) both want the **body only** — header/footer stripped, one line.

