# IAM Role for the ETL Glue Job
resource "aws_iam_role" "etl_glue_job_role" {
  name = "glue-job-role-for-orders-transformation" # A more descriptive name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role_policy_attachment" {
  role       = aws_iam_role.etl_glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_catalog_write_access" {
  name = "GlueCatalogWriteAccess"
  role = aws_iam_role.etl_glue_job_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetCatalogs",
          "glue:SearchTables",
          "glue:GetTables",
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetTable",
          "glue:GetDatabases",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:GetPartition",
          "glue:GetPartitions"
        ],
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/datalake_db",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/datalake_db/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/datalake_db/curated_orders"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_access_policy" {
  name = "S3AccessPolicyForGlueJob"
  role = aws_iam_role.etl_glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetBucketLocation",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          "${aws_s3_bucket.data_lake_bucket.arn}/*",
          aws_s3_bucket.data_lake_bucket.arn,
        ]
      }
    ]
  })
}

# IAM Role for Glue Job - Ingesting Structured Data
resource "aws_iam_role" "ingestion_glue_job_role" {
  name = "ingestion_glue_job_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = "Demo"
    Project     = "GlueIngestion"
  }
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess" # For simplicity, full S3 access. In production, narrow this down.
  role       = aws_iam_role.ingestion_glue_job_role.name
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess" # Allows Glue to write logs
  role       = aws_iam_role.ingestion_glue_job_role.name
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.ingestion_glue_job_role.name
}

# IAM Role for Glue Crawler
resource "aws_iam_role" "glue_crawler_role" {
  name = "glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach the AWS Glue Service Role policy to the crawler role
resource "aws_iam_role_policy_attachment" "glue_service_role_policy_attachment" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Policy for Glue Crawler Access
resource "aws_iam_role_policy" "glue_crawler_access_policy" {
  name = "GlueCrawlerAccessPolicy"
  role = aws_iam_role.glue_crawler_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:GetCatalogs",
          "glue:GetPartitions",
          "glue:*"
        ],
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.data_lake_glue_database.name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.data_lake_glue_database.name}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetObject"
        ],
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.data_lake_bucket.id}",
          "arn:aws:s3:::${aws_s3_bucket.data_lake_bucket.id}/*"
        ]
      }
    ]
  })
}

# IAM Role for the Lambda Function
resource "aws_iam_role" "lambda_trigger_role" {
  name = "lambda-s3-to-glue-trigger-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "lambda_glue_trigger_policy" {
  name = "LambdaGlueTriggerPolicy"
  role = aws_iam_role.lambda_trigger_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:StartJobRun"
        Resource = aws_glue_job.orders_glue_job.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}
