# Container image for the serving Lambda. The AWS base image bundles the Lambda
# Runtime Interface so the function just needs deps + the handler.
#
# Built for linux/amd64 to match the function's x86_64 architecture (see
# ../lambda.tf). On an Apple-silicon host, `terraform apply` / build_and_push.sh
# build through emulation — keep the platform pinned so the wheels match.
FROM public.ecr.aws/lambda/python:3.12

# Native wheels (confluent-kafka, snowflake-connector-python) install cleanly on
# the Amazon Linux 2023 base — the whole reason this is an image, not a zip.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Handler module + the Avro schema it serializes against (mirrors
# ../../producer/telemetry.avsc; the registered SR schema is the runtime source
# of truth via use.latest.version).
COPY handler.py telemetry.avsc ./

# <module>.<function> — Lambda invokes handler.handler.
CMD ["handler.handler"]
