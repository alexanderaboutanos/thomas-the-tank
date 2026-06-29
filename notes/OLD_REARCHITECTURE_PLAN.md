# Old Re-Architecture Plan (superseded)

*Living plan: move off Confluent → Snowflake onto an all-AWS, single-account shop, fed by
**real** Finnish train data. Nothing built yet. Steps 1–9 stay live until cutover (step 16).*

## Why

1. **One account, one bill.** Confluent + Snowflake are credit trials, not free tiers; my
   always-on Snowflake Sink bills per task-hour once credits lapse. All-AWS I can `destroy` to ~$0.
2. **Real data > synthetic loop.** Pulling a real firehose makes "open the valve" literal.
3. **Breadth Confluent hides.** Self-hosted Redpanda + proprietary Kinesis show range a SaaS doesn't.

Retiring the live integrations, not the knowledge — Kafka/Snowflake stay résumé talking points;
`notes/KAFKA.md` and `notes/SNOWFLAKE.md` stay.

## What changes

| Layer | Today | Proposed |
|---|---|---|
| Source | Synthetic `producer.py` | **Finland Digitraffic** MQTT firehose (synthetic kept as test source) |
| Registry | Confluent SR | **AWS Glue Schema Registry** (Avro, same `.avsc`) |
| Broker | Confluent Kafka | **Kinesis (always-on) + Redpanda (on-demand)** — lever-selected, *shared downstream* |
| Delivery | Managed Snowflake Sink | **Firehose + Lambda** (Flink parked as showpiece) |
| Lake | — | **S3** (Parquet) — source of truth |
| Warehouse | Snowflake | **Redshift Serverless (always-on)** — loads from S3 |
| Live tap / hosting / UI | Fargate → SSE, ALB, CloudFront, S3 | **No ALB** — CloudFront → small always-on box (EC2+EIP); consumer reads *active* broker |
| Brand | Sodor | **Thomas in Finland** — personas on a subset of real trains |

SSE live-tap logic stays; **drop the ALB** (~$16/mo) — CloudFront fronts the box directly. Surgery is upstream: source, registry, broker, sinks.

## Target architecture

```
Digitraffic MQTT ──valve──► Ingest adapter ──Avro(Glue SR)──► [Kinesis always-on]
(rate-capped, Fargate)         ▲                              [Redpanda on-demand]
                          broker lever                              │
                                                   shared downstream ▼
                                      Fargate consumer (reads ACTIVE broker)
                                       ├─► live-tap state ─► SSE /stream ─► CloudFront ─► UI
                                       └─► Firehose ─► S3 lake ─COPY─► Redshift Serverless
```

## Decisions locked

| Decision | Choice |
|---|---|
| Cost posture | Hybrid — Kinesis always-on, expensive bits on-demand |
| Brokers | Kinesis (always-on) + Redpanda (on-demand, self-hosted EC2). MSK/Confluent dropped. |
| Sink | Both — S3 lake + Redshift warehouse, lake-first |
| Redshift | Always-on Serverless (idles ~$0; don't keep-alive-poll it) |
| Schema | Avro + AWS Glue Schema Registry |
| Topology | Shared downstream — broker is a swappable transport |
| Source | Finland Digitraffic (free, no key, MQTT firehose) |
| Cast | Hybrid — real trains, Thomas personas on a recognized subset |
| Processing | Lambda + Firehose ($0 idle); Flink showpiece-only |

**Honest framing:** "I run a proprietary AWS stream (Kinesis) and self-host a Kafka-compatible
broker (Redpanda) end-to-end — so I know what MSK/Confluent abstract away." No overclaiming.

## Key design notes

- **Valve** = adapter's MQTT subscription; UI toggle hits server-side `POST /valve/open|close`
  (replaces browser-timer `/produce`). Rate cap + hard ceiling (~20k/open) so it can't run up cost.
- **Schema reworked to real fields** — `trainNumber`, `operator`, `trainType`, `lat/lon`, `speed`,
  `timestamp`. No `door_status`/`car_temperature_c`; reworking it *is* a schema-evolution exercise.
- **Shared downstream** = one S3 lake + one Redshift + one live-tap; only the read-from-broker
  adapter swaps (KCL vs Kafka consumer). Redpanda greyed out when not provisioned.
- **Analytics views** rework to real fields — "doors-open-while-moving" → a real ops metric
  (over-speed / late-running). Speed leaderboard + throughput/min survive.
- **No ALB.** CloudFront → one always-on box (EC2 t4g.micro/small + Elastic IP for a stable
  origin), SG locked to CloudFront's prefix list. Redpanda can co-locate. Fargate alt needs an
  ECS-event→Route53 updater for a stable hostname — the *only* thing the ALB was buying.

## Migration (continues step numbering; current = step 9)

> Build AWS in parallel, verify live, **then** tear down old (step 16).

- **10** — Glue Schema Registry + rework `telemetry.avsc` to real fields.
- **11** — Kinesis + Digitraffic ingest adapter (the valve); retire `producer.py` to test-only.
- **12** — Fargate consumer on Kinesis (keep SSE) + Firehose → S3 lake. *Live view shows real trains.*
- **13** — Redshift Serverless; load from S3; rebuild views; wire `/query`.
- **14** — Rebrand "Thomas in Finland" + persona overlay; new README + diagram.
- **15** — Redpanda on-demand (EC2, apply/destroy) + broker-selector lever.
- **16** — `destroy` Confluent + Snowflake, drop their secrets, prune modules, update README.

## Open questions

- Digitraffic specifics: `Digitraffic-User` header, MQTT host/topics, message shape, CC BY 4.0. *(verify before step 11)*
- Dual registry (Glue SR for Kinesis + Redpanda's built-in) — clean, or drift risk?
- Redpanda path → shared Firehose (Direct PUT): one unified consumer for both brokers, or one per broker?
- Keying: `trainNumber` (+ `departureDate`?) — confirm cardinality for shards/personas.
- Serving compute: small EC2+EIP (cheapest, co-locates Redpanda) vs keep Fargate (~$9 + Route53-updater glue)?
- How close are Confluent/Snowflake credits to lapsing? Sets cutover speed.

## Rough always-on cost (us-east-1)

| Component | ~Monthly |
|---|---|
| Kinesis (1 shard) | ~$11 |
| Serving box (EC2 t4g.micro/small + EIP, or Fargate 256/512 ~$9) | ~$6–12 |
| S3 + Glue + Firehose + CloudFront | ~cents |
| Redshift Serverless | ~$0 idle |
| **Always-on total** | **~$18–25** (no ALB) |
| Redpanda (EC2) | +~$32 *while up* |
| Managed Flink | +~$160 *showpiece only* |

## Future enhancements

*Breadth across brokers is the headline; depth here is what actually impresses. Pick off one at a time.*

**Streaming depth (senior-signal):**
- Schema-evolution demo — add optional field, show registry reject an incompatible change.
- Hot-partition — skew a key, watch one shard saturate, fix with salting (note: breaks per-key order).
- Lag in *time* not message count — alarm on iterator age's rate of change.
- DLQ + poison pill — route malformed records aside, commit past them, redrive path.
- Replay — `AT_TIMESTAMP` / offset reset through fixed consumer code.
- Idempotency — at-least-once + dedup on `event_id` (deliberately, not EOS-everywhere).
- Observability — CloudWatch dashboard (iterator age, Firehose, end-to-end latency); OTel headers.
- CDC from a real DB (Debezium/DMS) as an alt source — highest ceiling.
- Flink showpiece — event-time windowing/watermarks, spin up → screenshot → destroy.

**Product:**
- Finland map (Leaflet + OpenStreetMap) with train-face markers interpolated between updates.
- Schematized typed columns in Redshift (vs VARIANT blob).
- Athena ad-hoc layer over the S3 lake (query without waking Redshift).
