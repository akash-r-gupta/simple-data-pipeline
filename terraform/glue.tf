# The ETL Glue Job
resource "aws_glue_job" "orders_glue_job" {
  name              = "orders-transformation-job"
  role_arn          = aws_iam_role.etl_glue_job_role.arn
  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake_bucket.id}/scripts/${var.glue_job_script_name}"
  }

  default_arguments = {
    "--job-bookmark-option"      = "job-bookmark-enable"
    "--source_path"              = "" #"s3://${aws_s3_bucket.data_lake_bucket.id}/raw/orders/"
    "--destination_parquet_path" = "s3://${aws_s3_bucket.data_lake_bucket.id}/curated/orders/parquet/"
    "--destination_csv_path"     = "s3://${aws_s3_bucket.data_lake_bucket.id}/curated/orders/csv/"
  }
}

# Glue Database for the Data Catalog
resource "aws_glue_catalog_database" "data_lake_glue_database" {
  name        = "datalake_db"
  description = "Database for the curated data lake tables."
}

# Glue Table for the Data Catalog Database
resource "aws_glue_catalog_table" "curated_orders_table" {
  name          = "curated_orders"
  database_name = aws_glue_catalog_database.data_lake_glue_database.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake_bucket.id}/curated/orders/parquet/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "order_id"
      type = "string"
    }
    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "order_date"
      type = "date"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "int"
    }
    columns {
      name = "order_total"
      type = "double"
    }
    columns {
      name = "category"
      type = "string"
    }
  }
  partition_keys {
    name = "processing_date"
    type = "date"
  }
}

# The Ingestion Glue Job
resource "aws_glue_job" "ingestion_job" {
  name     = "ingestion-job-daily-extract"
  role_arn = aws_iam_role.ingestion_glue_job_role.arn

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake_bucket.id}/scripts/${var.ingestion_glue_job_script_name}"
  }

  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake_bucket.id}/temp/"
    "--S3_BUCKET"                        = "${aws_s3_bucket.data_lake_bucket.id}"
    "--RAW_PREFIX"                       = "raw/orders/"
    "--NUM_RECORDS"                      = "10"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--JOB_NAME"                         = "ingestion-job-daily-extract"
  }

  tags = {
    Environment = "Demo"
    Project     = "GlueIngestion"
  }
}