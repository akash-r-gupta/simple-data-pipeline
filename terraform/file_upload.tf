# --- NEW FILE UPLOAD RESOURCES ---
resource "aws_s3_bucket_object" "glue_script_file" {
  bucket       = aws_s3_bucket.data_lake_bucket.id
  key          = "scripts/${var.glue_job_script_name}"
  source       = "inputFiles/scripts/${var.glue_job_script_name}"
  etag         = filemd5("inputFiles/scripts/${var.glue_job_script_name}")
  content_type = "application/x-python"
  depends_on   = [aws_s3_bucket_object.s3_scripts_folder]
}

resource "aws_s3_bucket_object" "ingestion_glue_script_file" {
  bucket       = aws_s3_bucket.data_lake_bucket.id
  key          = "scripts/${var.ingestion_glue_job_script_name}"
  source       = "inputFiles/scripts/${var.ingestion_glue_job_script_name}"
  etag         = filemd5("inputFiles/scripts/${var.ingestion_glue_job_script_name}")
  content_type = "application/x-python"
  depends_on   = [aws_s3_bucket_object.s3_scripts_folder]
}
# --- END NEW FILE UPLOAD RESOURCES ---