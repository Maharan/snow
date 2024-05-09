variable "snowflake_account" {
  description = "Account id for snowflake"
  type        = string
  sensitive   = true
}

variable "snowflake_region" {
  description = "region and cloud for Snowflake account"
  type        = string
  sensitive   = true
}

variable "snowflake_username_inside" {
  description = "username for useraccount within snowflake account"
  type        = string
  sensitive   = true
}