# Plan — Swappable stream backend: Confluent Kafka ↔ AWS Kinesis lever

*Status: nothing built yet. This is a handoff plan for an implementing agent. Steps 1–9 stay
live throughout; this is purely additive — the working Avro + Schema Registry + Snowflake path
is never disturbed.*

## Goal

Add AWS Kinesis as a **second, swappable stream backend** beside the existing Confluent Kafka
stream, and let a viewer flip a front-end lever to route the same train telemetry through either
service — *everything downstream flows the same regardless of transport*. The deliverable is a
live CloudFront URL where a prospective employer can toggle Confluent ↔ Kinesis and watch both
work, side by side.

Two things make the **Kinesis skill legible** (not hidden behind the abstraction):
1. **Stream Internals panel** — a live readout of the active backend's machinery (Kinesis:
   shard, sequence number, iterator age; Kafka: partition, offset, group), so the toggle proves
   real, backend-specific knowledge instead of "they look identical."
2. **`notes/KINESIS.md`** — a Kafka↔Kinesis concept map in the exact voice of `notes/KAFKA.md`.

## Why this is a clean fit

The in-memory **event dict** is already the de-facto contract. `LiveState`
(`consumer/live.py:27`), the SSE layer (`consumer/app.py:100`), and the web table all operate on
a transport-agnostic dict. We keep that dict identical and only swap the wire format per backend.
The swappable part is the in-app abstraction, not the event shape.

## Scope

**In:** the real-time Live View (`/stream`) and the Produce ticks (`/produce`), plus the two
visibility additions above.

**Out (optional follow-on):** the Analytics tab stays Kafka/Snowflake-backed. Extending the
lever there means a Kinesis Firehose → Snowflake/S3 sink, parallel landing table + views, and
`?source=` routing on `/query` — a much larger, costlier build. Ship the Live-View lever first.

---

## Verified facts about the codebase (don't re-derive these)

| Fact | Where |
|---|---|
| Region is **us-east-2** | `infra/terraform.tfvars:3` |
| **Deployed** page (the demo) is `web/index.html` → S3 via `infra/site.tf:18` | `infra/site.tf` |
| About tab renders the **root `README.md`** (uploaded to S3 key `README.md`) | `infra/site.tf:53-57` |
| Separate **localhost** test page at `GET /` is `consumer/static/index.html` | `consumer/app.py:134-136` |
| Event dict is all JSON-friendly primitives (event_id, engine_id, timestamp, latitude, longitude, speed_kmh, heading, door_status, car_temperature_c) | `consumer/serving.py:238-249` |
| `aws_iam_role.task` is the task role and currently carries **no permissions** by design | `infra/ecs.tf:67-73` |
| Task env (non-secret) block to extend | `infra/ecs.tf:103-109` |
| `local.name_prefix` is the resource-name prefix | used throughout `infra/ecs.tf` |
| The producer's engine sim (`ENGINES`, `_step`, `_build_event`) + the `_produce_lock` guarding the shared random walk | `consumer/serving.py:169-249`, `:183` |

**Key design caveat — runtime AWS access is a deliberate departure.** Today the app makes *no*
AWS calls at runtime: empty task role, no boto3, secrets arrive as env vars
(`consumer/app.py:21-22`, `consumer/serving.py:14-18`). Kinesis requires boto3 + the task role
granted Kinesis actions scoped to the stream ARN. On AWS, boto3 picks up credentials from the
task role automatically (no keys in env); locally it uses the dev's `AWS_PROFILE`. This is a
*good* thing to show an employer (real IAM-scoped access) but is a conscious change — call it out
in code comments, `KINESIS.md`, and the README.

---

## Key design decisions

1. **Serialization split.** Kafka path stays **Avro** (don't touch the working Schema Registry +
   Snowflake sink). Kinesis path uses **plain JSON** (`json.dumps(event).encode()` /
   `json.loads`). The event dict is already JSON-friendly, so `LiveState`/SSE/UI need zero change.
   *Be ready to answer "why no schema registry on Kinesis?"* — JSON-for-practice now; AWS Glue
   Schema Registry is the productionizing step (document this in `KINESIS.md`).

2. **Both backends run at once.** One `LiveState` per source; two daemon consumer threads from
   boot. The flip is instant and bidirectional.

3. **Produce targets the selected backend only** (`/produce?source=`). Because the master
   regulator already calls `produce()` on a 2s timer, the live cutover choreography is: keep the
   regulator thrown → flip the source → the selected backend's table fills within ~one beat.
   That ~1-beat cutover is a *feature* to narrate, not a bug. (Alternative if you want zero-gap
   toggling: produce to both every tick and let the lever only switch which consumer you watch —
   not chosen here, but noted.)

4. **Shared cost gate + shared sim lock.** `ENGINES` is shared global state, so
   `produce_tick_kinesis()` MUST acquire the existing `_produce_lock` (`serving.py:183`) and
   honor the existing process-wide rate gate (`MIN_PRODUCE_INTERVAL_S`). One cadence across both
   backends is fine — the regulator only targets one source at a time.

5. **Stream Internals via a tiny per-source meta channel.** Each consumer loop writes a small
   `meta` dict onto its `LiveState`; a new `GET /internals?source=` returns it; the web panel
   polls it for the active source. This is what puts the Kinesis machinery on screen.

---

## Build chunks

> Ordered for a clean build + handoff. Each chunk is independently verifiable. Dependencies
> noted. A chunk is "done" when its acceptance check passes.

### Chunk 1 — Infra: Kinesis stream + scoped IAM + task env
**Files:** `infra/kinesis.tf` (new), `infra/ecs.tf` (edit), optionally `infra/outputs.tf`.

- New `aws_kinesis_stream` named `${local.name_prefix}-telemetry`, `shard_count = 1`,
  `retention_period = 24`.
- New `aws_iam_role_policy` on `aws_iam_role.task` (`infra/ecs.tf:70`) granting **only**
  `kinesis:PutRecord`, `PutRecords`, `GetRecords`, `GetShardIterator`, `ListShards`,
  `DescribeStreamSummary` on the stream ARN — least privilege, mirroring the doc pattern at
  `infra/ecs.tf:48-65`.
- In `infra/ecs.tf` add two non-secret env vars to the `environment` block
  (`infra/ecs.tf:103-109`): `AWS_REGION = var.region` and
  `KINESIS_STREAM_NAME = aws_kinesis_stream.<name>.name`. No new secrets — Kinesis auth is the
  task role.
- (Optional) output the stream name/ARN in `infra/outputs.tf`.

**Done when:** `terraform plan` shows the stream + inline policy + two new env vars and nothing
else unexpected. (Real apply happens in Chunk 7.)

### Chunk 2 — Consumer: Kinesis read path + per-source state
**Files:** `consumer/requirements.txt`, `consumer/live.py`, `consumer/app.py`,
`consumer/.env.example`. **Depends on:** nothing (can create the stream by hand to test).

- `consumer/requirements.txt`: add `boto3>=1.34`. Keep `confluent-kafka` — both stacks coexist.
- `consumer/live.py`:
  - Extend `LiveState` with a tiny meta channel: `self._meta = {}`, a `update_meta(self, d)`
    that replaces it under `self._lock`, and a `meta(self)` snapshot. (Feeds Chunk 4.)
  - Add `run_kinesis_consumer(state, cfg, stop_event)` next to `run_consumer`
    (`consumer/live.py:101`). Build `boto3.client("kinesis", region_name=cfg.aws_region)`;
    `list_shards`; hold a `LATEST` `get_shard_iterator` per shard; loop `get_records` per shard
    (sleep ~1s — mirrors `consumer.poll(1.0)` and stays under the 5 GetRecords-TPS/shard limit);
    `json.loads` each record's `Data`; `state.update(event)`. Advance via `NextShardIterator`.
    Keep the same poison-record guard as `run_consumer:127-129` (one bad record must not wedge
    the loop) and the daemon-thread contract. Re-acquire a fresh iterator on
    `ExpiredIteratorException`. After each poll, `state.update_meta({...})` with: `shard_id`,
    last `sequence_number`, `millis_behind_latest` (from the GetRecords response — this is
    iterator age), `records_last_poll`, `records_total`, `iterator_type: "LATEST"`,
    `stream_name`. (1-shard stream ⇒ exactly one shard ⇒ simplest loop.)
  - In `run_consumer` (Kafka), also call `state.update_meta({...})` after `state.update(event)`
    with `topic`, `group_id`, `partition` (`msg.partition()`), last `offset` (`msg.offset()`),
    `messages_total`. (So the panel works for both backends.)
- `consumer/app.py`:
  - Extend `Config` (`consumer/app.py:47`) with `self.aws_region = os.environ.get("AWS_REGION")`
    and `self.kinesis_stream_name = os.environ.get("KINESIS_STREAM_NAME")` — both **optional** so
    the app still boots without Kinesis configured.
  - Replace the module-level `state = LiveState()` (`:62`) with
    `STATES = {"kafka": LiveState(), "kinesis": LiveState()}`.
  - In `lifespan` (`:65`): `set_loop` on both; always start the Kafka consumer thread; start a
    second daemon thread `run_kinesis_consumer` for `STATES["kinesis"]` **only if**
    `cfg.kinesis_stream_name` is set. On shutdown, stop + join both.
  - `/stream` (`:100`): accept `source: str = Query("kafka")`, subscribe to
    `STATES.get(source, STATES["kafka"])`.
  - `/healthz` (`:95`): keep simple — report `STATES["kafka"]` count (back-compat) or both;
    per-source counts are non-essential.
- `consumer/.env.example`: add `AWS_REGION`, `KINESIS_STREAM_NAME`, and a note that local runs
  use the dev's `AWS_PROFILE`.

**Done when:** locally, with a hand-made 1-shard stream + env set, `GET /stream?source=kinesis`
yields a snapshot then `update` events once records land; `source=kafka` still works unchanged.

### Chunk 3 — Produce: Kinesis write path
**Files:** `consumer/serving.py`. **Depends on:** Chunk 2 (env/config) for local testing.

- Add `produce_tick_kinesis()` next to `_produce_tick` (`consumer/serving.py:252`), reusing the
  module-level `ENGINES`/`_step`/`_build_event` directly (same module, no refactor). It must:
  acquire the existing `_produce_lock`, honor the existing rate gate, then per engine
  `client.put_record(StreamName=…, Data=json.dumps(event).encode(), PartitionKey=engine_id)`
  (partition key = `engine_id`, the direct analogue of the Kafka message key). Cache the boto3
  client like `_kafka` is cached. Return `(produced, stream_name, errors)` to match the Kafka
  tick's shape.
- Change `@router.post("/produce")` (`:316`) to accept `source: str = Query("kafka")` and
  dispatch to `_produce_tick` or `produce_tick_kinesis` (keep the `_Throttled` → 429 handling).

**Done when:** `POST /produce?source=kinesis` returns `{"produced": N, ...}` and
`aws kinesis get-shard-iterator` + `get-records` shows JSON telemetry landing; `source=kafka`
unchanged.

### Chunk 4 — Stream Internals endpoint
**Files:** `consumer/app.py`. **Depends on:** Chunk 2 (the `meta` channel).

- Add `GET /internals` accepting `source: str = Query("kafka")`, returning
  `STATES.get(source, STATES["kafka"]).meta()` as JSON. Cheap, read-only, no auth surface.

**Done when:** `GET /internals?source=kinesis` returns the shard/sequence/iterator-age fields and
`?source=kafka` returns partition/offset fields.

### Chunk 5 — Web UI: source lever + Stream Internals panel
**Files:** `web/index.html` (deployed page). Optionally mirror into
`consumer/static/index.html` (localhost test page) — not required for the demo.

- **Source toggle.** Add a two-option control (Kafka ↔ Kinesis). Baseline = a compact segmented
  control (least code); optional polish = clone the `.lever` CSS (`web/index.html:164-206`) to
  match the master regulator. Default `kafka`. Place it in/near the live-view panel header
  (beside the `streamStatus` badge).
- **Wire the toggle.** Introduce `let currentSource = "kafka"`. Refactor the single
  `const es = new EventSource("/stream")` (`:471`) into a mutable `let es` + `connectStream(src)`
  that closes the old `es` and opens `new EventSource("/stream?source=" + src)`. On toggle: set
  `currentSource`, clear the `engines` map + `renderLatest()` (so the other backend's rows don't
  linger), then `connectStream(currentSource)`. Snapshot-on-connect repaints, so the flip is
  seamless.
- **Produce source.** Point `produce()` (`:498-501`) at `"/produce?source=" + currentSource`.
- **Stream Internals panel.** New `.panel` inside the Live-view tab, under the table. A
  `pollInternals()` fetches `/internals?source=${currentSource}` every ~2s and renders the
  active backend's live machinery using *that backend's native vocabulary*, with a one-line "≈
  the Kafka/Kinesis equivalent" subtitle so the toggle teaches the mapping:
  - Kinesis: stream · shard id · sequence number · **iterator age (ms behind latest)** · records/poll.
  - Kafka: topic · group · partition · **offset** · messages seen.

**Done when:** on the deployed URL, flipping the lever reconnects the stream, repaints from the
new backend within a beat, and the internals panel shows live, backend-specific values.

### Chunk 6 — Docs
**Files:** `notes/KINESIS.md` (new), root `README.md` (edit).

- **`notes/KINESIS.md`** — a Kafka↔Kinesis concept map in the **voice of `notes/KAFKA.md`** (see
  the voice spec below). An agent can draft this; it must copy the voice and get the concept map
  right. Content checklist:
  - `## Terms` (stream, shard, partition key, sequence number, shard iterator, retention) + a
    "Quick Hierarchy" arrow block like KAFKA.md's.
  - The mapping: topic↔stream, partition↔shard, message key↔partition key, consumer group +
    offset ↔ application + shard iterator + sequence number, `auto.offset.reset` ↔ iterator types
    (`LATEST` / `TRIM_HORIZON` / `AT_SEQUENCE_NUMBER` / `AT_TIMESTAMP`), subscribe/poll ↔
    `get_shard_iterator`/`get_records`, **resharding (split/merge)** vs Kafka's one-way partition
    increase, the **5 GetRecords-TPS + 2 MB/s read + 1 MB/s/1000-rec/s write per shard** limits,
    enhanced fan-out as the consumer-group analogue.
  - A `## Questions` Q&A section in the same casual voice.
  - Why this build uses **plain JSON for practice** vs **Avro + AWS Glue Schema Registry** (the
    productionizing step).
  - Why the app now uses an **IAM task role at runtime** (scoped to the stream ARN, no keys) —
    the deliberate departure from "no AWS at runtime."
- **Root `README.md`** — the About tab renders it verbatim (`web/index.html:593`,
  `infra/site.tf:53-57`), so this is what the employer reads. Document the lever + dual-backend
  architecture: a "Swappable stream backend" note and the Stream row in the Stack table.

**Done when:** `KINESIS.md` reads like KAFKA.md wrote it and the concept map is correct; the
README's About tab explains the lever.

### Chunk 7 — Verify + deploy + cost note
**Local:**
1. `pip install -r consumer/requirements.txt` (now includes boto3).
2. `aws kinesis create-stream --stream-name <name> --shard-count 1`; set `AWS_REGION`,
   `KINESIS_STREAM_NAME`, `AWS_PROFILE` in `consumer/.env`.
3. `uvicorn app:app --reload --port 8000` from `consumer/` (check the port is free first).
4. `GET /healthz` ok; `curl -N ".../stream?source=kinesis"` → snapshot then updates once producing.
5. `POST /produce?source=kinesis` → `{"produced": N}`; confirm with
   `get-shard-iterator` + `get-records`. Confirm `/internals?source=kinesis` shows live fields.
6. Repeat 4–5 with `source=kafka` — both backends side by side.

**Deployed:** `cd infra && terraform apply` (creates stream + IAM, redeploys the task with the
new env vars + image). On the live CloudFront URL: throw the master regulator, flip
Confluent↔Kinesis, watch each backend populate and the internals panel update. Check CloudWatch
for the `kinesis-consumer` thread.

**Cost / cleanup:** ~1 shard ≈ $11/mo + PUT payload units (standing, always-on — name this given
the project's destroy-to-~$0 posture). `terraform destroy` (or remove `infra/kinesis.tf`) tears
down the stream cleanly; nothing else depends on it.

---

## Voice spec for `notes/KINESIS.md` (match `notes/KAFKA.md`)

- First person, lowercase-leaning, conversational. Em-dashes for asides. Short declaratives
  ("done."). Occasional **bold lead-in** on a term, then a plain-English gloss.
- Structure mirrors KAFKA.md: a one-line italic summary under the H1, then `## Terms` (bullets +
  an arrow "Quick Hierarchy"), then topic sections, then a `## Questions` Q&A where the question
  is posed plainly and answered as if thinking aloud ("well, …", "but what does this do?").
- Technically precise, zero marketing fluff. Define a term by contrast with its Kafka twin
  wherever possible — the doc IS the concept map.
- Reuse the running example (`engine_id` as the key/partition key, `Thomas`, the Sodor engines).

---

## Open questions / risks

- **Does the abstraction hide the skill?** The Stream Internals panel + `KINESIS.md` are the
  mitigation. Confirm the panel actually surfaces shard/sequence/iterator-age live before
  calling the demo done — that readout is the proof.
- **Serialization asymmetry** (Avro vs JSON) means the two paths aren't apples-to-apples on
  schema governance. Intended for v1; Glue SR is the named next step. Have the answer ready.
- **Cutover gap.** Producing to the selected backend only means a ~1-beat empty moment on flip
  (mitigated by snapshot-on-connect of accumulated state). Decide whether to narrate it or switch
  to produce-to-both. (Decision above: narrate it.)
- **Localhost test page** (`consumer/static/index.html`) won't have the lever unless mirrored —
  fine for the demo (CloudFront serves `web/index.html`), but note it.
