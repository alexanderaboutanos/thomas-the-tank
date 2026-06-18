# infra (Terraform — AWS)

The serving layer: a Lambda that queries Snowflake, fronted by API Gateway
(HTTP API), plus a private S3 bucket + CloudFront for the `/web` static site.

This mirrors the `confluent/` and `snowflake/` modules — same skeleton style,
pin the provider versions and verify attributes against the AWS provider docs.

## Files

| File                       | What it does                                          |
|----------------------------|-------------------------------------------------------|
| `main.tf`                  | Providers + the Secrets Manager lookup for SF creds.  |
| `lambda.tf`                | IAM role, `GetSecretValue` policy, the function.      |
| `api.tf`                   | HTTP API, `GET /latest` route, invoke permission.     |
| `site.tf`                  | S3 (private) + CloudFront via Origin Access Control.  |
| `variables.tf` / `outputs.tf` | Inputs and the API/CloudFront URLs.                |
| `lambda/handler.py`        | Lambda source: read secret, query Snowflake, return JSON. |

## Deploy

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # then edit
terraform init
terraform plan
terraform apply
```

Outputs include `api_endpoint` (the web UI calls `<api_endpoint>/latest`) and
`web_url` (the CloudFront site). Set `API_BASE` in `../web/index.html` to the
API endpoint, then re-`apply` to re-upload the page.

## Secrets

The Lambda reads Snowflake creds from **Secrets Manager** — never from env vars
or code. Create the secret out-of-band (the key-pair user from `../snowflake`)
and pass its name as `snowflake_secret_name`; Terraform only grants the role
`GetSecretValue` on that one secret.

## Notes / TODO

- `snowflake-connector-python` is **not** in the base Lambda runtime. Attach a
  Lambda layer or use a container image, then implement the query in
  `lambda/handler.py` (it returns `[]` until then).
- Tighten `cors_allow_origins` to the CloudFront URL once the distribution is up.
- Generated artifacts (`.terraform/`, `*.tfstate`, `build/`) stay out of git.
