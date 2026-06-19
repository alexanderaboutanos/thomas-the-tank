variable "confluent_cloud_api_key" {
  description = "Cloud (account-level) API key for Terraform — not the producer's cluster key."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Cloud API secret paired with confluent_cloud_api_key."
  type        = string
  sensitive   = true
}

variable "region" {
  description = "AWS region for the Basic cluster (e.g. us-east-1)."
  type        = string
  default     = "us-east-1"
}
