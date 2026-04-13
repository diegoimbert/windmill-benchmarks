output "cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.bench.name
}

output "cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = aws_eks_cluster.bench.endpoint
}

output "configure_kubectl" {
  description = "Command to configure kubectl"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${aws_eks_cluster.bench.name}"
}

output "s3_bucket" {
  description = "S3 bucket for benchmark data"
  value       = aws_s3_bucket.bench_data.bucket
}

output "ecr_airflow_pandas" {
  description = "ECR repository URL for Airflow Pandas image"
  value       = aws_ecr_repository.airflow_pandas.repository_url
}

output "ecr_airflow_snowflake" {
  description = "ECR repository URL for Airflow Snowflake image"
  value       = aws_ecr_repository.airflow_snowflake.repository_url
}
