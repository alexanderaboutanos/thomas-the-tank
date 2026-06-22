variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "sodor-railway"
}

variable "snowflake_secret_name" {
  description = "Secrets Manager secret holding Snowflake read creds (for /latest, /query)."
  type        = string
  default     = "sodor-railway/snowflake"
}

variable "confluent_secret_name" {
  description = "Secrets Manager secret holding Confluent Kafka + Schema Registry creds (for /produce)."
  type        = string
  default     = "sodor-railway/confluent"
}

variable "num_engines" {
  description = "How many engines /produce emits a reading for per tick (matches the producer)."
  type        = number
  default     = 5
}

variable "cors_allow_origins" {
  description = "Extra origins allowed to call the API beyond the CloudFront URL (always allowed). E.g. [\"http://localhost:8000\"] for local UI dev."
  type        = list(string)
  default     = []
}
