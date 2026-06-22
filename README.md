# Thomas The Tank

![Thomas the Tank Engine emerging from a tunnel](web/thomas.png)

A small train-IoT data pipeline — a hands-on exercise built for fun to get to grips with the
Kafka, Confluent, and Snowflake stack, themed as the **North Western Railway** of the Island of Sodor.

The simulated engines (Thomas, Edward, Henry, Gordon, James…) emit telemetry to
**Confluent Cloud (Kafka)**, which the managed **Snowflake Sink Connector** lands in
**Snowflake** via **Snowpipe Streaming** (near-real-time latency — measured ~3–30s; see step 6). The web UI reads
everything from Snowflake through a Lambda — both the **live view** (latest row per engine,
refreshed every couple of seconds) and **analytics** (aggregate query buttons).

A train-themed on/off switch in the UI drives a `produce` Lambda, so the whole demo runs in
the cloud — no laptop needed. (Reading the live view from Snowflake is a deliberate
simplification; the production-faithful version taps Kafka directly — see the reach goal.)

## Live demo

The step-7 serving layer is deployed in `us-east-2`:

- **Web UI** — https://dhfibaytx3cxo.cloudfront.net
- **API** — https://3xbw6f984c.execute-api.us-east-2.amazonaws.com — `POST /produce`, `GET /latest`, `GET /query?name=…`

(URLs are environment-specific; re-deploying `/infra` prints fresh ones via `terraform output` `web_url` / `api_endpoint`.)

## Architecture

```mermaid
flowchart LR
    W["S3 + CloudFront web UI<br/>on/off switch + query buttons"]
    W -->|"switch on → POST /produce (per tick)"| PR["produce Lambda"]
    PR -->|"Avro, keyed by engine_id"| K["Confluent Cloud<br/>Kafka topic + Schema Registry"]
    K -->|"managed Sink Connector<br/>(Snowpipe Streaming)"| S["Snowflake"]
    W -->|"GET /latest (live) &<br/>GET /query?name=… (analytics)"| L["serving Lambda"]
    L -->|SQL| S
    L -->|JSON| W
```

## Stack

| Layer            | Tool                          | Managed in   |
|------------------|-------------------------------|--------------|
| Producer         | Python + `confluent-kafka` (local script + `produce` Lambda) | `/producer` (+ `/infra`) |
| Stream           | Confluent Cloud (Kafka)       | `/confluent` (Terraform) |
| Sink             | Confluent Snowflake Sink Connector (Snowpipe Streaming) | `/confluent` (Terraform) |
| Warehouse        | Snowflake (live view + analytics) | `/snowflake` (Terraform) |
| API + hosting    | AWS Lambda, API Gateway, S3, CloudFront | `/infra` (Terraform) |
| UI               | Static HTML/JS                | `/web`       |

## Tools needed

- A **Confluent Cloud** account and a **Snowflake** trial 
- An **AWS** account.
- CLIs/tools: `python3`, `terraform`, `aws` CLI, `confluent` CLI (optional but handy), `git`.
- Credentials live in your shell / a secrets store — **never in this repo** (see `.gitignore`).

## Build order

Build the unfamiliar foundation first; the AWS UI last. Each step is its own directory; the checkbox is its status, and ← marks where we are.

- [x] **1. Accounts** — create the Confluent + Snowflake accounts.
- [x] **2. One message flowing** (`/producer`) — spin up a Basic cluster + a `sodor-telemetry` topic, then run `producer.py` with plain JSON.
- [x] **3. Codify the Confluent Terraform** (`/confluent`) — by-hand setup translated to IaC (provider pin, producer + admin service accounts, API keys, ACL, outputs); imported the surviving environment and recreated the cluster/topic/identities under Terraform. Topic partitioned at 3.
- [x] **4. Avro + Schema Registry** (`/producer`, `/confluent`) — upgraded the producer off plain JSON to a registered Avro schema; this sets up the schema-evolution experiments.
- [x] **5. The Snowflake hop** (`/snowflake`, then the connector in `/confluent`) — set up the Snowflake DB/table + a key-pair user, then configure the managed Snowflake Sink Connector. Confirm rows land in the table. *(Verified: 150/150 produced messages landed in `TELEMETRY_RAW`, all 5 engines surfaced in `TELEMETRY_LATEST`.)*
- [ ] **6. Snowpipe Streaming** (`/confluent`, `/snowflake`) ← *we are here* — upgrade the managed sink connector from classic file-based **Snowpipe** to **Snowpipe Streaming** so the Snowflake-backed live view is fresh within single-digit seconds (classic batch lags tens of seconds — too slow for a live on/off switch). Adjust the Snowflake role/grants the streaming ingest needs, re-verify rows land, and note the new end-to-end latency. Verify the exact connector config against current Confluent + Snowflake docs before applying.
    - **Status — deployed, but the single-digit goal is *not yet met*; left open.** The switch is live and verified: `snowflake.ingestion.method = SNOWPIPE_STREAMING`, rows land continuously and exactly-once (same two-VARIANT shape — schematization left off, view untouched), and the Snowflake-side grants were trimmed (`CREATE STAGE` dropped). Measured end-to-end freshness: **~3s right after a flush, ~30s just before the next, avg ~15s** — a clear win over classic's minute+, but *not* single-digit. Root cause: the **fully-managed connector pins the streaming client-lag at its 30s default** and won't let us lower it (`snowflake.streaming.max.client.lag` and `enable.single.buffer` are silently dropped — never land in state; `buffer.flush.time` is accepted but bypassed by the connector's locked single-buffer mode). True single-digit/live needs a self-managed connector or the **step-9 direct-Kafka tap** — so this freshness gap is parked and revisited there.
- [x] **7. Serving layer + UI** (`/infra` + `/web`) — stand up the cloud serving layer so the whole demo runs without a laptop. One **container-image Lambda** (carries `confluent-kafka` for `/produce` **and** `snowflake-connector-python` for the Snowflake reads — this unblocks the `handler.py` stub), behind API Gateway as three routes, plus the static site:
    - `POST /produce` — produce one tick of Avro telemetry for all engines to Kafka (reuses `producer.py`'s logic; holds the Confluent + Schema Registry creds from Secrets Manager). Replaces running `producer.py` on your laptop.
    - `GET /latest` — **the live view: `SELECT` latest-per-engine from Snowflake** (`ROW_NUMBER() … PARTITION BY engine_id ORDER BY timestamp DESC`, `rn = 1`). Same query shape as the analytics routes; un-stubs `handler.py`.
    - `GET /query?name=…` — run one of three **canned, server-side** analytics queries against Snowflake (never raw SQL from the browser): **(1) speed leaderboard**, **(2) fleet throughput per minute**, **(3) doors-open-while-moving**.
    - **Web UI** — a train-themed **on/off master switch**: while on, the browser calls `/produce` on a ~2s interval and polls `/latest`; three **query buttons** call `/query`. Per-viewer — closing the tab stops production, so it scales to zero with no runaway cost on a public URL. (Theme/polish — map, lever switch, engine faces — is a later pass; wire the plumbing first.)
    - **Status — live in `us-east-2` and verified end-to-end.** All three routes return 200 (`POST /produce`, `GET /latest`, `GET /query?name=…`); the UI switch + analytics buttons work in-browser. CORS is locked to the CloudFront origin (tracked from the distribution in `infra/api.tf`, so it never goes stale).
- [ ] **8. Play** — evolve the schema (watch the Schema Registry compatibility check), watch consumer lag, reason about partitioning by `engine_id`. This is the fun part — tinker and see what breaks.
- [ ] **9. Reach goal** (after everything else) — the "true push" live feed: read the live view **straight from Kafka** instead of Snowflake — a **persistent Kafka consumer** pushing to the browser over **WebSocket/SSE** (e.g. API Gateway WebSocket API + a small Fargate consumer). This is how the real system is wired (Kafka → UI for live; Snowflake for analytics) — lower-latency and more faithful, but it reintroduces an always-on process, hence a reach goal rather than the baseline. Optional extra: a Snowflake **Stream + TASK** maintaining a `TELEMETRY_LATEST` table to showcase CDC.
- [ ] **10. Schematization — typed columns** (`/confluent`, `/snowflake`) — an enhancement to the step-6 connector; do it any time after step 6 (it pairs naturally with step 8's schema-evolution play). Step 6 deliberately left schematization **off**, so every message still lands in the `RECORD_CONTENT` VARIANT. Flip the managed connector's schematization **on** (verify the exact key against current Confluent docs — e.g. `snowflake.enable.schematization`, which needs a schema-based value converter) so the Sink reads the Avro schema and **auto-creates real typed columns** (`ENGINE_ID STRING`, `SPEED_KMH FLOAT`, …) — no VARIANT blob. The flatten-from-VARIANT views collapse to bare column refs: that seam is centralized in `TELEMETRY_FLAT` (`/snowflake/analytics.tf`), so it's a one-line change there and `TELEMETRY_LATEST` + the analytics views + the Lambda follow unchanged. Trade-off to own out loud: it **couples the Snowflake table to the Avro schema**, so a schema-evolution change (step 8) now shifts columns under you — which is exactly what makes it a good experiment to run alongside that step.

> Tip: stand the Confluent + Snowflake resources up **by hand once** (console/CLI) to
> learn what each piece is, *then* codify them into the Terraform here. Writing IaC for
> resources you don't yet understand is painful; codifying ones you just configured is fast.

## Secrets

- `.gitignore` excludes `.env`, `*.tfvars`, `*.tfstate`, key files, and Terraform/build artifacts.
- Put Snowflake creds the Lambda needs in **AWS Secrets Manager / SSM**, not in code.
- Each subdir has a `.env.example` / `terraform.tfvars.example` — copy, fill in, and keep the real one out of git.

## Theoretical Considerations

- Why partition by `engine_id` (per-engine ordering) and how partition count affects parallelism.
- Schema Registry + a schema-evolution change you made and what broke / didn't.
- Snowflake Sink Connector config: batching, and why the warehouse view is near-real-time, not sub-second (and that Snowpipe Streaming / a direct Kafka tap is the lower-latency option).
- Consumer lag: what you saw and how you monitor it in production.
- **Live view reads Snowflake (via Snowpipe Streaming); the production-correct version taps Kafka.** For the demo, both live and analytics read Snowflake — one backend, one query shape, far less to break in front of an interviewer. The trade-off, owned out loud: Snowflake is a columnar analytical warehouse, so using it as a low-latency operational store for "latest row per engine" works against its grain; the real system reads the live path straight from Kafka (the step-9 reach goal). Snowpipe Streaming pushes the Snowflake-backed view to near-real-time (~3–30s), but the fully-managed connector's ~30s flush cap keeps it short of truly "live" — another reason the faithful live path is the Kafka tap.
- **Snowflake *Stream* vs Snowpipe *Streaming*.** A *Stream* is a CDC change-feed object (drained by a TASK); *Snowpipe Streaming* is a low-latency row-based ingestion method. Easy to conflate — only the latter reduces ingest lag (it's what step 6 turns on).
- **Consuming live from Kafka in a stateless Lambda (the reach goal).** Partition `assign()` + `seek()` to the high watermark and dedupe by key, vs. a persistent consumer + WebSocket/SSE push. Trade-off: per-invoke connection overhead and "scales to zero" vs. lower latency and an always-on process.
