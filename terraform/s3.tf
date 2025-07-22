# S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.data_lake_bucket_name
  tags = {
    Name = "DataLakeBucket"
  }
}

# S3 Event Notification to trigger Lambda
resource "aws_s3_bucket_notification" "s3_raw_upload_notification" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_glue_job_function.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/orders/"
  }
  depends_on = [
    aws_lambda_function.trigger_glue_job_function,
    aws_lambda_permission.allow_s3_invocation
  ]
}