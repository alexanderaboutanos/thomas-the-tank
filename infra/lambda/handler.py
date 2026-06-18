"""
Serving-layer Lambda for the Sodor engine telemetry exercise.

API Gateway (GET /latest) -> this function -> Snowflake -> JSON for /web.

Snowflake credentials come from AWS Secrets Manager (secret name in the
SNOWFLAKE_SECRET_NAME env var), NEVER from committed env vars or code.

Packaging note: boto3 ships in the Lambda runtime, but
`snowflake-connector-python` does NOT. Ship it via a Lambda layer or a
container image, or vendor it into this directory before `terraform apply`.
Until then this returns an empty list so the API and web UI work end-to-end.
"""
import json
import os

import boto3

_secrets = boto3.client("secretsmanager")


def _snowflake_creds():
    name = os.environ["SNOWFLAKE_SECRET_NAME"]
    resp = _secrets.get_secret_value(SecretId=name)
    return json.loads(resp["SecretString"])


def _query_latest(creds):
    """Return the latest row per engine as a list of dicts.

    TODO: implement with snowflake-connector-python once it's packaged:

        import snowflake.connector
        conn = snowflake.connector.connect(
            account=creds["account"],
            user=creds["user"],
            private_key=...,            # key-pair auth; see ../snowflake
            warehouse=creds["warehouse"],
            database="SODOR_RAILWAY",
            schema="TELEMETRY",
        )
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(
            '''
            SELECT engine_id, speed_kmh, latitude, longitude,
                   door_status, timestamp
            FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY engine_id ORDER BY timestamp DESC) AS rn
              FROM telemetry_raw
            ) WHERE rn = 1
            '''
        )
        return cur.fetchall()
    """
    return []


def handler(event, context):
    creds = _snowflake_creds()
    engines = _query_latest(creds)
    # API Gateway adds the CORS headers; don't set them here (avoids duplicates).
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(engines, default=str),
    }
