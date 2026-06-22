# ECS/Fargate runtime for the step-9 live-tap service — the first piece that does
# NOT scale to zero (one always-on task ~a few $/mo; scale to 0 between demos and
# the live view goes dark). It runs the /consumer image (built+pushed by
# consumer.tf) which serves /stream (SSE from Kafka) AND the folded step-7 routes
# /produce, /latest, /query — one container, behind the ALB (alb.tf), behind
# CloudFront (site.tf).
#
# Secret delivery is the platform's job, not the app's: the task definition injects
# the three Secrets Manager secrets as env vars via `secrets[].valueFrom` (one
# JSON key per env var). The app reads only the environment — no boto3, no IAM at
# runtime (see consumer/serving.py + consumer/app.py).

# ─── Cluster + logs ──────────────────────────────────────────────────────────

resource "aws_ecs_cluster" "live" {
  name = "${local.name_prefix}-live"
}

resource "aws_cloudwatch_log_group" "live" {
  name              = "/ecs/${local.name_prefix}-live"
  retention_in_days = 14
}

# ─── IAM ─────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: what the ECS agent uses to pull the image and — because we use
# `secrets[].valueFrom` — to read the secret values at task launch and inject them.
resource "aws_iam_role" "task_execution" {
  name               = "${local.name_prefix}-task-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Grant GetSecretValue on exactly the three secrets the task injects — least
# privilege, same spirit as the retired Lambda's role.
data "aws_iam_policy_document" "task_secrets" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      data.aws_secretsmanager_secret.snowflake.arn,
      data.aws_secretsmanager_secret.confluent.arn,
      data.aws_secretsmanager_secret.confluent_live.arn,
    ]
  }
}

resource "aws_iam_role_policy" "task_secrets" {
  name   = "read-task-secrets"
  role   = aws_iam_role.task_execution.id
  policy = data.aws_iam_policy_document.task_secrets.json
}

# Task role: the application itself makes no AWS API calls at runtime (its creds
# arrive as env vars), so this identity carries no permissions. Defining it
# explicitly keeps a clean boundary between "the platform" and "the app".
resource "aws_iam_role" "task" {
  name               = "${local.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# ─── Task definition ─────────────────────────────────────────────────────────

locals {
  sf_arn   = data.aws_secretsmanager_secret.snowflake.arn
  conf_arn = data.aws_secretsmanager_secret.confluent.arn
  live_arn = data.aws_secretsmanager_secret.confluent_live.arn
}

resource "aws_ecs_task_definition" "live" {
  family                   = "${local.name_prefix}-live"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "live"
    image     = local.consumer_image_uri
    essential = true

    portMappings = [{
      containerPort = var.container_port
      protocol      = "tcp"
    }]

    # Non-secret config.
    environment = [
      { name = "TOPIC", value = var.topic },
      { name = "GROUP_ID", value = var.live_group_id },
      { name = "HEARTBEAT_SECONDS", value = "20" },
      { name = "NUM_ENGINES", value = tostring(var.num_engines) },
      { name = "PORT", value = tostring(var.container_port) },
    ]

    # Secret config, one JSON key per env var (valueFrom "<arn>:<json-key>::").
    secrets = [
      # Live consumer — app-live (read-only) identity + the shared cluster
      # endpoints (bootstrap + SR URL are the same cluster the producer uses).
      { name = "BOOTSTRAP_SERVERS", valueFrom = "${local.live_arn}:bootstrap_servers::" },
      { name = "SCHEMA_REGISTRY_URL", valueFrom = "${local.live_arn}:schema_registry_url::" },
      { name = "LIVE_KAFKA_API_KEY", valueFrom = "${local.live_arn}:kafka_api_key::" },
      { name = "LIVE_KAFKA_API_SECRET", valueFrom = "${local.live_arn}:kafka_api_secret::" },
      { name = "LIVE_SCHEMA_REGISTRY_API_KEY", valueFrom = "${local.live_arn}:schema_registry_api_key::" },
      { name = "LIVE_SCHEMA_REGISTRY_API_SECRET", valueFrom = "${local.live_arn}:schema_registry_api_secret::" },
      # /produce — producer (WRITE) identity; app-live has no topic WRITE ACL.
      { name = "PRODUCE_KAFKA_API_KEY", valueFrom = "${local.conf_arn}:kafka_api_key::" },
      { name = "PRODUCE_KAFKA_API_SECRET", valueFrom = "${local.conf_arn}:kafka_api_secret::" },
      { name = "PRODUCE_SCHEMA_REGISTRY_API_KEY", valueFrom = "${local.conf_arn}:schema_registry_api_key::" },
      { name = "PRODUCE_SCHEMA_REGISTRY_API_SECRET", valueFrom = "${local.conf_arn}:schema_registry_api_secret::" },
      # /latest + /query — Snowflake reads (connector key-pair user).
      { name = "SNOWFLAKE_ACCOUNT", valueFrom = "${local.sf_arn}:account::" },
      { name = "SNOWFLAKE_USER", valueFrom = "${local.sf_arn}:user::" },
      { name = "SNOWFLAKE_PRIVATE_KEY", valueFrom = "${local.sf_arn}:private_key::" },
      { name = "SNOWFLAKE_WAREHOUSE", valueFrom = "${local.sf_arn}:warehouse::" },
      { name = "SNOWFLAKE_DATABASE", valueFrom = "${local.sf_arn}:database::" },
      { name = "SNOWFLAKE_SCHEMA", valueFrom = "${local.sf_arn}:schema::" },
      { name = "SNOWFLAKE_ROLE", valueFrom = "${local.sf_arn}:role::" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.live.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "live"
      }
    }
  }])

  # The image must exist in ECR before the task def references it.
  depends_on = [terraform_data.consumer_build_push]
}

# ─── Service ─────────────────────────────────────────────────────────────────

resource "aws_ecs_service" "live" {
  name            = "${local.name_prefix}-live"
  cluster         = aws_ecs_cluster.live.id
  task_definition = aws_ecs_task_definition.live.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  # Public-subnet task with a public IP: egress to Kafka/Snowflake without a NAT.
  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.task.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.live.arn
    container_name   = "live"
    container_port   = var.container_port
  }

  # Give the app time to boot (uvicorn + consumer thread) before health checks
  # can kill it.
  health_check_grace_period_seconds = 60

  depends_on = [aws_lb_listener.http]

  lifecycle {
    # Allow manual scale-to-zero between demos (the documented cost lever) without
    # Terraform reverting it on the next apply.
    ignore_changes = [desired_count]
  }
}
