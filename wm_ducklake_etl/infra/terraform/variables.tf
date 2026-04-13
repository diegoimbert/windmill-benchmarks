variable "region" {
  description = "AWS region for the EKS cluster"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "ducklake-bench"
}

variable "node_instance_type" {
  description = "EC2 instance type for worker nodes"
  type        = string
  default     = "m6a.8xlarge"
}

variable "s3_bucket_name" {
  description = "S3 bucket for benchmark data (TPC-DS parquet files)"
  type        = string
  default     = "ducklake-bench-data"
}
