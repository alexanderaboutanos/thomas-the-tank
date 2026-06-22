# Shared network data sources for the step-9 live-tap runtime (ALB + Fargate).
#
# We deliberately ride the account's DEFAULT VPC and its public subnets: the task
# gets a public IP for egress to Confluent Cloud (9092) + Snowflake (443) with no
# NAT gateway (NAT would be the single biggest line item here). The trade-off —
# tasks on public subnets — is contained by security groups: the task SG only
# accepts traffic from the ALB SG, and the ALB SG only from CloudFront (see alb.tf).

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# CloudFront's origin-facing IP ranges, as an AWS-managed prefix list. The ALB SG
# locks ingress to this so only CloudFront — not the open internet — can reach the
# ALB, even though it sits on public subnets with a public DNS name.
data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}
