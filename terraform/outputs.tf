# Outputs
output "data_lake_bucket_name" {
  description = "Name of the S3 bucket for the data lake."
  value       = aws_s3_bucket.data_lake_bucket.id
}