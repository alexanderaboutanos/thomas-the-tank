# infra (Terraform — AWS)

The cloud runtime: one always-on **Fargate** container behind an **ALB**, behind the
**CloudFront** distribution that also serves the static site. One service serves
everything — the live SSE view straight from Kafka and the three Snowflake/Kafka
routes — with no Lambda, API Gateway, or CORS.

```
                    ┌─ default (/*) ──────────────────────▶ S3 (static site)
browser ─HTTPS─▶ CloudFront ┤
                    └─ /stream /produce /latest /query ─HTTP─▶ ALB ─▶ Fargate ─┬─▶ Kafka
                       (caching disabled)                                       └─▶ Snowflake
```

The image (built from `../consumer`) carries both native libs — `confluent-kafka`
(live consumer + `/produce`) and `snowflake-connector-python` (`/latest` + `/query`).
This always-on task is the only piece that **doesn't scale to zero** (~a few $/mo);
set `desired_count = 0` between demos and the live view goes dark (analytics still
read Snowflake, which the sink keeps filling).

Deployed on the **personal** account (`319120789796`) in **`us-east-2`** — the region
**must match** Confluent + Snowflake or the task can't reach the cluster. The provider
is pinned to `profile = "personal"`; apply with `AWS_PROFILE=personal` too, since
`build_and_push.sh`'s `aws ecr` call uses ambient creds.

## Routes (all behind CloudFront, same origin as the site)

| Route               | Does                                                              |
|---------------------|------------------------------------------------------------------|
| `GET /stream`       | SSE live view from Kafka (snapshot + per-engine updates + heartbeat). |
| `POST /produce`     | Emit one tick of Avro telemetry (all engines) to Kafka. Producer SA (WRITE). |
| `GET /latest`       | `SELECT * FROM TELEMETRY_LATEST` (latest row per engine).        |
| `GET /query?name=…` | One of three canned analytics **views**: `speed_leaderboard`, `throughput_per_minute`, `doors_open_moving`. |
| `GET /healthz`      | ALB target-group health check.                                   |

App code lives in `../consumer` (`app.py` = live SSE; `serving.py` = folded routes).
The serving routes hold **no SQL** — all query logic is Snowflake views
(`../snowflake/analytics.tf`); the app only `SELECT * FROM <whitelisted view>`.

## Files

| File                        | What it does                                                    |
|-----------------------------|----------------------------------------------------------------|
| `main.tf`                   | Providers + the three Secrets Manager lookups.                 |
| `network.tf`                | Default VPC + subnets + CloudFront managed prefix list.        |
| `alb.tf`                    | ALB, target group (`/healthz`), HTTP listener, the two SGs.    |
| `ecs.tf`                    | Cluster, task def (secret→env injection), service, IAM, logs.  |
| `consumer.tf`               | ECR repo + build/push of the live-tap image (source-hash tag). |
| `site.tf`                   | S3 (private) + CloudFront: S3 default origin, ALB app origin.  |
| `variables.tf`/`outputs.tf` | Inputs and the CloudFront/ALB/ECR URLs.                        |
| `build_and_push.sh`         | Docker build + ECR push (the consumer image uses it).          |

## Secrets

The task reads creds from **Secrets Manager** (never code). Create all three
out-of-band on the target account; Terraform grants the execution role
`GetSecretValue` on exactly these and injects specific JSON keys as env vars. Fill
from `terraform output` in `../confluent` + `../snowflake`.

- **`sodor-railway/snowflake`** — `/latest` + `/query` reads (connector key-pair
  user): `account`, `user`, `private_key` (full PEM, `\n`-escaped), `warehouse`,
  `database`, `schema`, `role`.
- **`sodor-railway/confluent`** — `/produce` (producer SA keys, WRITE):
  `bootstrap_servers`, `kafka_api_key`/`_secret`, `schema_registry_url`,
  `schema_registry_api_key`/`_secret`, `topic`.
- **`sodor-railway/confluent-live`** — `/stream` consumer (read-only **app-live**
  keys; **same JSON shape** as `confluent`, different identity).

```bash
aws secretsmanager create-secret --name sodor-railway/snowflake      --secret-string file://snowflake-secret.json
aws secretsmanager create-secret --name sodor-railway/confluent      --secret-string file://confluent-secret.json
aws secretsmanager create-secret --name sodor-railway/confluent-live --secret-string file://confluent-live-secret.json
# keep those *.json files out of git
```

> The Snowflake reads reuse the **connector** identity — a demo simplification
> (`../snowflake/analytics.tf` grants it `SELECT` on the serving views); production
> would use a separate read-only role.

## Deploy

Needs a **running Docker daemon** + AWS creds — `apply` shells out to
`build_and_push.sh` to build the image (`linux/amd64`; emulated on Apple silicon) and
push it to ECR, then creates the cluster, task, ALB, SGs, and CloudFront behaviors.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # then edit (region!)
AWS_PROFILE=personal terraform init && AWS_PROFILE=personal terraform apply
```

Outputs: `web_url` (CloudFront — the whole app), `alb_dns_name`,
`consumer_ecr_repository_url`, `consumer_image_uri`, `site_bucket`. The UI is
same-origin — no `API_BASE` to wire.

## Notes

- The task runs on **public subnets with a public IP** (no NAT gateway), so it has
  egress to Confluent (9092) + Snowflake (443), fenced by SGs: the task SG accepts
  only the ALB SG; the ALB SG accepts only CloudFront's managed prefix list — the ALB
  is public-facing in name only.
- **SSE through CloudFront:** the app routes use the managed `CachingDisabled` +
  `AllViewerExceptHostHeader` policies with `compress = false`, and the ALB origin's
  60s read timeout sits above the app's 20s heartbeat, so streams stay open.
