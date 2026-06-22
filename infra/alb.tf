# Application Load Balancer for the step-9 live-tap service, plus the two security
# groups that fence it in.
#
# CloudFront (the same distribution that fronts the S3 site, see site.tf) is the
# ONLY thing allowed to reach this ALB — its SG ingress is locked to CloudFront's
# managed prefix list. The viewer→CloudFront hop is HTTPS on CloudFront's cert;
# the CloudFront→ALB hop is plain HTTP (the ALB has no domain/cert), which is why
# the listener below is :80 and the CloudFront origin is http-only.

# ─── Security groups ─────────────────────────────────────────────────────────

resource "aws_security_group" "alb" {
  name_prefix = "${local.name_prefix}-alb-"
  description = "Live-tap ALB: HTTP ingress from CloudFront only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "HTTP from CloudFront origin-facing ranges"
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.cloudfront.id]
  }

  egress {
    description = "To the Fargate tasks"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-alb" }

  lifecycle {
    create_before_destroy = true # name_prefix + in-use by the ALB
  }
}

resource "aws_security_group" "task" {
  name_prefix = "${local.name_prefix}-task-"
  description = "Live-tap Fargate task: app-port ingress from the ALB only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "App port from the ALB"
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  # Egress anywhere: the task needs to reach Confluent Cloud (9092) and Snowflake
  # (443) over the public internet (public-subnet task, no NAT).
  egress {
    description = "Egress to Kafka + Snowflake"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-task" }

  lifecycle {
    create_before_destroy = true
  }
}

# ─── Load balancer ───────────────────────────────────────────────────────────

resource "aws_lb" "live" {
  name               = "${local.name_prefix}-live"
  load_balancer_type = "application"
  internal           = false
  security_groups    = [aws_security_group.alb.id]
  subnets            = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "live" {
  name        = "${local.name_prefix}-live"
  port        = var.container_port
  protocol    = "HTTP"
  target_type = "ip" # Fargate awsvpc networking registers task ENIs as IP targets
  vpc_id      = data.aws_vpc.default.id

  health_check {
    path                = "/healthz"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  # SSE clients hold long-lived connections; keep drain short so deploys roll
  # quickly (EventSource auto-reconnects through CloudFront on the cut).
  deregistration_delay = 30
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.live.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.live.arn
  }
}
