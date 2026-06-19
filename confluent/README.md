# confluent

Terraform for the Confluent Cloud side of the pipeline: environment, Basic Kafka
cluster, the `sodor-telemetry` topic (3 partitions), and the producer/admin
service accounts + API keys.

Everything here is **created** by Terraform except the **environment, which is
imported** — Confluent won't let you delete your last environment, so we adopted
the existing one instead of recreating it.

## Stand it up

```bash
cp terraform.tfvars.example terraform.tfvars   # cloud API key/secret + region
terraform init
terraform import confluent_environment.sodor env-XXXXX   # env id from the console
terraform apply
```

Then point the producer at the outputs:

```bash
terraform output kafka_bootstrap_servers         # -> producer/.env BOOTSTRAP_SERVERS
terraform output producer_kafka_api_key          # -> producer/.env KAFKA_API_KEY
terraform output -raw producer_kafka_api_secret  # -> producer/.env KAFKA_API_SECRET
```

## Gotchas

- **`terraform destroy` fails** on the environment (org's last one). Target the
  cluster instead — `terraform destroy -target=confluent_kafka_cluster.basic`
  cascades the topic/ACL — or `terraform state rm confluent_environment.sodor` first.
- **First apply 403s on the topic/ACL?** The `app-manager` role binding hasn't
  propagated yet — just re-run `apply`.
