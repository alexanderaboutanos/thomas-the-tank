# Container image for the always-on live-tap consumer (build-order step 9).
#
# Unlike the serving Lambda (../infra/lambda, AWS base image + Runtime Interface),
# this is a long-running web service: a plain python:3.12-slim base running
# uvicorn behind the ALB. Built for linux/amd64 to match the Fargate task's
# x86_64 platform — build_and_push.sh pins --platform, so an Apple-silicon host
# builds through emulation and the wheels still match.
FROM python:3.12-slim

# librdkafka (bundled inside the confluent-kafka wheel) verifies Confluent Cloud's
# TLS cert against the system CA store, which the slim base doesn't ship. Install
# it explicitly or SASL_SSL to the broker + Schema Registry fails at connect.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Deps first, for layer caching. confluent-kafka ships a manylinux wheel that
# bundles librdkafka, so no compiler or system headers are needed on slim.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App source: the FastAPI server, the consumer/state module, the folded serving
# routes (/produce, /latest, /query), the Avro schema both the consumer decodes
# and the producer serializes against, and the same-origin test page served at "/".
COPY app.py live.py serving.py telemetry.avsc ./
COPY static ./static

# Run unprivileged — nothing here needs root, and PORT stays >1024 so the
# non-root user can bind it.
RUN useradd --uid 1001 --no-create-home --shell /usr/sbin/nologin sodor
USER sodor

# Long-running service: bind all interfaces so the ALB target group can reach it.
# PORT is overridable (ECS may inject it); `exec` replaces the shell so uvicorn
# receives SIGTERM directly → graceful lifespan shutdown stops the consumer thread.
ENV PORT=8000
EXPOSE 8000
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
