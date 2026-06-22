#!/usr/bin/env bash
# Build the serving-Lambda container image and push it to ECR.
#
# Invoked by Terraform (terraform_data.build_push in ecr.tf) on every apply where
# the lambda/ source hash changes; runnable by hand too. Requires a running
# Docker daemon and AWS creds in the environment. Env vars are passed by the
# Terraform provisioner:
#   REPO_URL  ECR repository URL (…dkr.ecr.<region>.amazonaws.com/<name>)
#   TAG       image tag (content hash of lambda/, so the URI changes on edits)
#   REGION    AWS region
#   CONTEXT   Docker build context (the lambda/ dir)
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
