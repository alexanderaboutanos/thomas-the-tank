#!/usr/bin/env bash
# Build a container image and push it to ECR. Generic — driven entirely by the
# env vars below, so the same script serves both images in this project: the
# serving-Lambda image (terraform_data.build_push in ecr.tf, context lambda/) and
# the step-9 live-tap consumer image (terraform_data.consumer_build_push in
# consumer.tf, context ../consumer).
#
# Invoked by Terraform on every apply where the relevant source hash changes;
# runnable by hand too. Requires a running Docker daemon and AWS creds in the
# environment. Env vars are passed by the Terraform provisioner:
#   REPO_URL  ECR repository URL (…dkr.ecr.<region>.amazonaws.com/<name>)
#   TAG       image tag (content hash of the context, so the URI changes on edits)
#   REGION    AWS region
#   CONTEXT   Docker build context dir
set -euo pipefail

: "${REPO_URL:?}" "${TAG:?}" "${REGION:?}" "${CONTEXT:?}"
REGISTRY="${REPO_URL%%/*}"  # account.dkr.ecr.<region>.amazonaws.com

echo "==> ECR login ($REGISTRY)"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

echo "==> Build $REPO_URL:$TAG (linux/amd64)"
docker build --platform linux/amd64 -t "$REPO_URL:$TAG" "$CONTEXT"

echo "==> Push $REPO_URL:$TAG"
docker push "$REPO_URL:$TAG"
