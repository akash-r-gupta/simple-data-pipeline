# The Lambda Function that Triggers the Glue Job
resource "aws_lambda_function" "trigger_glue_job_function" {
  function_name    = "s3-to-glue-trigger"
  handler          = "trigger_glue_job.handler"
  runtime          = "python3.9"
  timeout          = 60
  filename         = "lambda_code/src.zip"
  source_code_hash = filebase64sha256("lambda_code/src.zip")
  role             = aws_iam_role.lambda_trigger_role.arn

  environment {
    variables = {
      GLUE_JOB_NAME            = aws_glue_job.orders_glue_job.name
      DESTINATION_PARQUET_PATH = "s3://${aws_s3_bucket.data_lake_bucket.id}/curated/orders/parquet/"
      DESTINATION_CSV_PATH     = "s3://${aws_s3_bucket.data_lake_bucket.id}/curated/orders/csv/"
    }
  }
}

# Grant S3 permission for the Lambda function to be invoked
resource "aws_lambda_permission" "allow_s3_invocation" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_glue_job_function.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake_bucket.arn
}