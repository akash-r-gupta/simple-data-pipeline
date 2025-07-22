variable "region" {
  type    = string
  default = "us-east-1"
}

variable "data_lake_bucket_name" {
  description = "A unique S3 bucket name for the data lake."
  type        = string
  default     = "akash-datalake-3-1"
}

variable "glue_job_script_name" {
  description = "The name of the Glue script file."
  type        = string
  default     = "transform_orders.py"
}

variable "ingestion_glue_job_script_name" {
  description = "The name of the Glue script file."
  type        = string
  default     = "ingestion_script.py"
}
