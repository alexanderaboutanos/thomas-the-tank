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
  description = "Name of the Secrets Manager secret holding Snowflake read creds."
  type        = string
  default     = "sodor-railway/snowflake"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12"
}

variable "cors_allow_origins" {
  description = "Origins allowed to call the API. Tighten to the CloudFront URL."
  type        = list(string)
  default     = ["*"]
}
