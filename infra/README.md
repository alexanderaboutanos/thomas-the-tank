# infra (Terraform — AWS)

The serving layer (build-order step 7): one **container-image Lambda** behind an
API Gateway HTTP API with three routes, plus a private S3 bucket + CloudFront for
the `/web` static site. This is what lets the whole demo run in the cloud with no
laptop.

```
              POST /produce ─┐                 ┌─ Kafka  (confluent-kafka)
  CloudFront ──> API GW ─────┼─> serving Lambda ┤
   (web UI)    GET  /latest ─┤   (one image)    └─ Snowflake (snowflake-connector-python)
               GET  /query ──┘
```

One image carries **both** native libraries — `confluent-kafka` (librdkafka, for
producing) and `snowflake-connector-python` (for the reads). That's the reason
this is a container image rather than a zip: vendoring those manylinux wheels
into a zip is brittle; an image carries them cleanly.

## Routes

| Route               | Does                                                              |
|---------------------|------------------------------------------------------------------|
| `POST /produce`     | Emit one tick of Avro telemetry (all engines) to Kafka. Replaces running `producer.py` on your laptop. |
| `GET /latest`       | Live view: `SELECT * FROM TELEMETRY_LATEST` (latest row per engine). |
| `GET /query?name=…` | One of three canned analytics **views**: `speed_leaderboard`, `throughput_per_minute`, `doors_open_moving`. |

The Lambda holds **no SQL** — all query logic lives in Snowflake views
(`../snowflake/analytics.tf`); the function only `SELECT * FROM <whitelisted view>`.

## Files

| File                        | What it does                                                    |
|-----------------------------|----------------------------------------------------------------|
| `main.tf`                   | Providers + the two Secrets Manager lookups.                   |
| `ecr.tf`                    | ECR repo + the build/push step (image tagged by source hash).  |
| `lambda.tf`                 | IAM role, `GetSecretValue` on both secrets, the function.      |
| `api.tf`                    | HTTP API, the three routes, CORS, invoke permission.           |
| `site.tf`                   | S3 (private) + CloudFront via Origin Access Control.           |
| `variables.tf`/`outputs.tf` | Inputs and the API/CloudFront/ECR URLs.                        |
| `build_and_push.sh`         | Docker build + ECR push (called by `ecr.tf`, or by hand).      |
| `lambda/Dockerfile`         | `public.ecr.aws/lambda/python:3.12` + the two deps + handler.  |
| `lambda/handler.py`         | Router: produce to Kafka / read the Snowflake views → JSON.    |
| `lambda/telemetry.avsc`     | Avro schema for `/produce` (mirrors `../producer/telemetry.avsc`). |

## Prerequisites

- **A running Docker daemon** and AWS creds in your shell — `terraform apply`
  shells out to `build_and_push.sh` to build and push the image to ECR.
- `region` **must match** the Confluent + Snowflake region (see `../confluent`),
  or the produce path can't reach the cluster cleanly.
- The two secrets below must exist first.

## Secrets

The Lambda reads creds from **Secrets Manager** — never from env vars or code.
Create both out-of-band; Terraform only grants the role `GetSecretValue` on these
two. Fill them from `terraform output` in `../confluent` and `../snowflake`.

`sodor-railway/snowflake` — for the reads (reuses the connector key-pair user):

```json
{
  "account": "<org>-<account>",
  "user": "SODOR_CONNECTOR",
  "private_key": "-----BEGIN PRIVATE KEY-----\n…full PEM…\n-----END PRIVATE KEY-----\n",
  "warehouse": "SODOR_WH",
  "database": "SODOR_RAILWAY",
  "schema": "TELEMETRY",
  "role": "SODOR_CONNECTOR_ROLE"
}
```

`sodor-railway/confluent` — for `/produce` (the producer service-account keys):

```json
{
  "bootstrap_servers": "pkc-xxxxx.<region>.aws.confluent.cloud:9092",
  "kafka_api_key": "…",
  "kafka_api_secret": "…",
  "schema_registry_url": "https://psrc-xxxxx.<region>.aws.confluent.cloud",
  "schema_registry_api_key": "…",
  "schema_registry_api_secret": "…",
  "topic": "sodor-telemetry"
}
```

```bash
aws secretsmanager create-secret --name sodor-railway/snowflake \
  --secret-string file://snowflake-secret.json
aws secretsmanager create-secret --name sodor-railway/confluent \
  --secret-string file://confluent-secret.json
# keep those *.json files out of git (the repo .gitignore already covers *.env/keys)
```

> The serving Lambda reuses the **connector** identity for reads — a deliberate
> demo simplification. `../snowflake/analytics.tf` grants that role `SELECT` on
> the four serving views. Production would use a separate read-only role.

## Deploy

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # then edit (region!)
terraform init
terraform plan
terraform apply      # builds + pushes the image, then creates everything
```

Then set `API_BASE` in `../web/index.html` to the `api_endpoint` output and
re-`apply` to re-upload the page. Outputs: `api_endpoint`, `web_url` (CloudFront),
`ecr_repository_url`.

## Notes

- The function runs **outside any VPC**, so it keeps default internet egress to
  Confluent Cloud and Snowflake — no NAT gateway needed.
- The image is built `linux/amd64` for the function's `x86_64` architecture; on an
  Apple-silicon host the build goes through emulation.
- Tighten `cors_allow_origins` to the CloudFront URL once the distribution is up.
- Generated artifacts (`.terraform/`, `*.tfstate`, `build/`) stay out of git.
