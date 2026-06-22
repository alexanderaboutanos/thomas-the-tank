# infra (Terraform — AWS)

The cloud runtime. Step 7 stood this up as a container **Lambda** behind API
Gateway; **step 9 folded that into one always-on Fargate container** behind an ALB,
behind the same CloudFront distribution that serves the static site — and retired
the Lambda + API Gateway + CORS. One service now serves everything: the live SSE
view straight from Kafka **and** the three former Lambda routes.

```
                          ┌──────────────── default (/*) ──────────▶ S3 (static site)
  browser ──HTTPS──▶ CloudFront ┤
                          └─ /stream /produce /latest /query ─HTTP─▶ ALB ─▶ Fargate task
                                  (caching disabled)                         │  confluent-kafka  ─▶ Kafka
                                                                             └  snowflake-connector ─▶ Snowflake
```

The image (built from `../consumer`) carries both native libraries —
`confluent-kafka` (librdkafka: the live consumer + `/produce`) and
`snowflake-connector-python` (`/latest` + `/query`). One always-on task is the
first piece here that **doesn't scale to zero** (~a few $/mo); set
`desired_count = 0` between demos and the live view goes dark (analytics still work
— they read Snowflake, which the sink keeps filling).

> ⚠️ **Account TODO.** Every `apply` so far landed on the **corpa** account
> (`346299178925`, the `default` profile); this belongs on **personal**
> (`319120789796`, the `personal` profile). Not yet moved — see the callout in the
> root `README.md` for the (deliberate, not-a-bare-profile-flip) migration. Until
> then, `plan`/`apply` here will hit corpa.

## Routes (all behind CloudFront, same origin as the site)

| Route               | Does                                                              |
|---------------------|------------------------------------------------------------------|
| `GET /stream`       | SSE live view from Kafka (snapshot + per-engine updates + heartbeat). |
| `POST /produce`     | Emit one tick of Avro telemetry (all engines) to Kafka. Producer SA (WRITE). |
| `GET /latest`       | `SELECT * FROM TELEMETRY_LATEST` (latest row per engine).        |
| `GET /query?name=…` | One of three canned analytics **views**: `speed_leaderboard`, `throughput_per_minute`, `doors_open_moving`. |
| `GET /healthz`      | ALB target-group health check.                                   |

App code lives in `../consumer` (`app.py` = live SSE; `serving.py` = the folded
routes). The serving routes hold **no SQL** — all query logic is Snowflake views
(`../snowflake/analytics.tf`); the app only `SELECT * FROM <whitelisted view>`.

## Files

| File                        | What it does                                                    |
|-----------------------------|----------------------------------------------------------------|
| `main.tf`                   | Providers + the **three** Secrets Manager lookups.             |
| `network.tf`                | Default VPC + subnets + CloudFront managed prefix list.        |
| `alb.tf`                    | ALB, target group (`/healthz`), HTTP listener, the two SGs.    |
| `ecs.tf`                    | Cluster, task def (secret→env injection), service, IAM, logs.  |
| `consumer.tf`               | ECR repo + build/push of the live-tap image (source-hash tag). |
| `site.tf`                   | S3 (private) + CloudFront: S3 default origin, ALB app origin.  |
| `variables.tf`/`outputs.tf` | Inputs and the CloudFront/ALB/ECR URLs.                        |
| `build_and_push.sh`         | Docker build + ECR push (generic; the consumer image uses it). |

## Prerequisites

- **A running Docker daemon** and AWS creds in your shell — `terraform apply`
  shells out to `build_and_push.sh` to build + push the image to ECR.
- `region` **must match** the Confluent + Snowflake region (see `../confluent`),
  or the task can't reach the cluster cleanly. (Currently `us-east-2`.)
- The **three** secrets below must exist first, on the account you're deploying to.

## Secrets

The task reads creds from **Secrets Manager** — never from code. Create all three
out-of-band; Terraform grants the execution role `GetSecretValue` on exactly these
and the task definition injects specific JSON keys as env vars. Fill them from
`terraform output` in `../confluent` and `../snowflake`.

`sodor-railway/snowflake` — `/latest` + `/query` reads (the connector key-pair user):

```json
{ "account": "<org>-<account>", "user": "SODOR_CONNECTOR",
  "private_key": "-----BEGIN PRIVATE KEY-----\n…full PEM…\n-----END PRIVATE KEY-----\n",
  "warehouse": "SODOR_WH", "database": "SODOR_RAILWAY",
  "schema": "TELEMETRY", "role": "SODOR_CONNECTOR_ROLE" }
```

`sodor-railway/confluent` — `/produce` (the **producer** service-account keys, WRITE):

```json
{ "bootstrap_servers": "pkc-xxxxx.<region>.aws.confluent.cloud:9092",
  "kafka_api_key": "…", "kafka_api_secret": "…",
  "schema_registry_url": "https://psrc-xxxxx.<region>.aws.confluent.cloud",
  "schema_registry_api_key": "…", "schema_registry_api_secret": "…",
  "topic": "sodor-telemetry" }
```

`sodor-railway/confluent-live` — the `/stream` live consumer (the **read-only
app-live** keys; **same JSON shape** as `confluent`, different identity):

```json
{ "bootstrap_servers": "pkc-xxxxx.<region>.aws.confluent.cloud:9092",
  "kafka_api_key": "<live_kafka_api_key>", "kafka_api_secret": "<live_kafka_api_secret>",
  "schema_registry_url": "https://psrc-xxxxx.<region>.aws.confluent.cloud",
  "schema_registry_api_key": "<live_schema_registry_api_key>",
  "schema_registry_api_secret": "<live_schema_registry_api_secret>",
  "topic": "sodor-telemetry" }
```

```bash
aws secretsmanager create-secret --name sodor-railway/snowflake      --secret-string file://snowflake-secret.json
aws secretsmanager create-secret --name sodor-railway/confluent      --secret-string file://confluent-secret.json
aws secretsmanager create-secret --name sodor-railway/confluent-live --secret-string file://confluent-live-secret.json
# keep those *.json files out of git (the .gitignore already covers *.env/keys)
```

> The serving routes reuse the **connector** identity for the Snowflake reads — a
> deliberate demo simplification (`../snowflake/analytics.tf` grants that role
> `SELECT` on the four serving views). Production would use a separate read-only role.

## Deploy

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # then edit (region!)
terraform init
terraform plan
terraform apply      # builds + pushes the image, then creates everything
```

`apply` builds the image (so Docker must be up), pushes it to ECR, then creates the
cluster, task, ALB, SGs, and the CloudFront app behaviors. Outputs: `web_url`
(CloudFront — the whole app), `alb_dns_name`, `consumer_ecr_repository_url`,
`consumer_image_uri`, `site_bucket`. The web UI is same-origin now (`/stream`,
`/produce`, …) — no `API_BASE` to wire.

## Notes

- The task runs on **public subnets with a public IP** (no NAT gateway), so it has
  egress to Confluent Cloud (9092) and Snowflake (443). It's fenced by SGs: the
  task SG accepts only the ALB SG, and the ALB SG accepts only CloudFront's managed
  prefix list — the ALB is public-facing in name only.
- **SSE through CloudFront:** the app routes use the managed `CachingDisabled` +
  `AllViewerExceptHostHeader` policies with `compress = false`, and the ALB origin's
  read timeout is 60s — comfortably above the app's 20s heartbeat, so streams stay
  open.
- The image is built `linux/amd64` for Fargate's `x86_64`; on Apple silicon the
  build goes through emulation (`build_and_push.sh` pins `--platform`).
- Generated artifacts (`.terraform/`, `*.tfstate`, `build/`) stay out of git.
