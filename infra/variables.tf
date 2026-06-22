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
  description = "Secrets Manager secret holding the PRODUCER's Confluent Kafka + Schema Registry creds (for /produce, WRITE)."
  type        = string
  default     = "sodor-railway/confluent"
}

variable "live_confluent_secret_name" {
  description = "Secrets Manager secret holding the read-only app-live Confluent creds (for the /stream live consumer). Same JSON shape as confluent."
  type        = string
  default     = "sodor-railway/confluent-live"
}

variable "num_engines" {
  description = "How many engines /produce emits a reading for per tick (matches the producer)."
  type        = number
  default     = 5
}

# ─── Live-tap runtime (ECS/Fargate + ALB) ────────────────────────────────────

variable "container_port" {
  description = "Port the live-tap container (uvicorn) listens on; the ALB target + task SG use it too."
  type        = number
  default     = 8000
}

variable "task_cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)."
  type        = number
  default     = 256
}

variable "task_memory" {
  description = "Fargate task memory (MiB). Must be a valid pairing with task_cpu."
  type        = number
  default     = 512
}

variable "desired_count" {
  description = "Live-tap tasks to run. 1 for the demo; set 0 to go dark between demos (the cost lever)."
  type        = number
  default     = 1
}

variable "topic" {
  description = "Kafka topic the live consumer reads and /produce writes."
  type        = string
  default     = "sodor-telemetry"
}

variable "live_group_id" {
  description = "Consumer group.id for the live tap. MUST start with 'sodor-live-' (the app-live ACL prefix)."
  type        = string
  default     = "sodor-live-consumer"
}
