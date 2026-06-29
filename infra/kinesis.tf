# AWS Kinesis Data Stream — the SECOND, swappable stream backend beside Confluent
# Kafka (see notes/PLAN_KINESIS_LEVER.md, notes/KINESIS.md). A viewer flips a
# front-end lever to route the same train telemetry through either service;
# everything downstream (the in-memory event dict, the SSE layer, the web table)
# is transport-agnostic, so only the wire format changes per backend.
#
# This is purely additive — the working Avro + Schema Registry + Snowflake path is
# never disturbed. The Kinesis path carries plain JSON (JSON-for-practice now; AWS
# Glue Schema Registry is the productionizing step — documented in notes/KINESIS.md).
#
# COST: each shard is standing, always-on capacity (~$11/mo/shard + PUT payload
# units), unlike the rest of this project's destroy-to-~$0 posture. At 3 shards
# that's ~$33/mo. `terraform destroy` (or removing this file) tears the stream
# down cleanly — nothing else depends on it.

resource "aws_kinesis_stream" "telemetry" {
  name = "${local.name_prefix}-telemetry"

  # Three shards MIRROR the Kafka topic's three partitions, so the lever is a true
  # side-by-side: each backend fans the same engines across the same number of
  # ordering/throughput units, and the Stream Internals panel reads out 3-vs-3
  # (a partition's offset beside a shard's sequence number). Both counts are wildly
  # over-provisioned for ~5 records/2s — that's deliberate symmetry, not capacity.
  #
  # NOTE: Kinesis won't scale shard_count by more than 2x in one UpdateShardCount
  # call, so a 1->3 change can't apply in place — recreate with
  # `terraform apply -replace=aws_kinesis_stream.telemetry` (the stream holds only a
  # disposable LATEST tail; the name, hence the ARN and its scoped IAM policy, is
  # unchanged). Still trivial vs the per-shard limits (5 GetRecords TPS, 2 MB/s read,
  # 1 MB/s + 1000 rec/s write).
  shard_count = 3

  # Hours of retention (Kinesis's analogue of Kafka's topic retention). 24 is the
  # floor and all we need — the live tap only ever reads from the live edge (LATEST).
  retention_period = 24
}

# ─── Scoped IAM — the deliberate "AWS at runtime" departure ───────────────────
#
# Until now the app made NO AWS calls at runtime: the task role carried no
# permissions and creds arrived as env vars (see ecs.tf:67-73). Kinesis changes
# that — boto3 in the container picks up the TASK ROLE's credentials automatically
# (no keys in env) and calls Kinesis directly. We grant that role ONLY the actions
# this app needs, scoped to exactly this stream's ARN — least privilege, mirroring
# the execution role's secrets policy (ecs.tf:48-65). Showing real IAM-scoped
# access is a feature for an employer to see, not an accident.

data "aws_iam_policy_document" "task_kinesis" {
  statement {
    actions = [
      # Produce (/produce?source=kinesis writes telemetry).
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      # Consume (the live tap reads the stream).
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:DescribeStreamSummary",
    ]
    resources = [aws_kinesis_stream.telemetry.arn]
  }
}

resource "aws_iam_role_policy" "task_kinesis" {
  name   = "kinesis-telemetry-access"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_kinesis.json
}
