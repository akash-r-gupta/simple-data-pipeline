# --- NEW FOLDER RESOURCES ---
resource "aws_s3_bucket_object" "s3_scripts_folder" {
  bucket       = aws_s3_bucket.data_lake_bucket.id
  key          = "scripts/"
  content_type = "binary/octet-stream" # Standard content type for empty objects
  depends_on   = [aws_s3_bucket.data_lake_bucket]
}

resource "aws_s3_bucket_object" "s3_raw_orders_folder" {
  bucket       = aws_s3_bucket.data_lake_bucket.id
  key          = "raw/orders/"
  content_type = "binary/octet-stream"
  depends_on   = [aws_s3_bucket.data_lake_bucket]
}

resource "aws_s3_bucket_object" "s3_curated_folder" {
  bucket       = aws_s3_bucket.data_lake_bucket.id
  key          = "curated/"
  content_type = "binary/octet-stream"
  depends_on   = [aws_s3_bucket.data_lake_bucket]
}
# --- END NEW FOLDER RESOURCES ---