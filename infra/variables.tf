variable "aws_region" {
  description = "AWS region to deploy resources into"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Project name used as a prefix for all resource names"
  type        = string
  default     = "olist"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "Name of the S3 bucket for raw data"
  type        = string
  default     = "olist-raw-data"
}

variable "processed_bucket_name" {
  description = "Name of the S3 bucket for processed/curated data"
  type        = string
  default     = "olist-processed-data"
}

variable "kinesis_shard_count" {
  description = "Number of shards for the Kinesis data stream"
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  description = "Data retention period for the Kinesis stream (hours)"
  type        = number
  default     = 24
}
