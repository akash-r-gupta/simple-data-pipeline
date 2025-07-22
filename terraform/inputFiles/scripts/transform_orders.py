import boto3
import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
import logging

# --- Initialize Logging ---
logging.basicConfig(level=logging.INFO)

# --- Get job arguments ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_path",
    "destination_parquet_path",
    "destination_csv_path",
    "database_name",
    "table_name",
    "crawler_name"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Read raw CSV input ---
try:
    input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [args["source_path"]]},
        format="csv",
        format_options={"withHeader": True, "inferSchema": True},
    )
except Exception as e:
    logging.error(f"Failed to read input data: {str(e)}")
    raise
# --- Transform ---
df = input_dynamic_frame.toDF()

if df.rdd.isEmpty():
    raise Exception("Input data is empty. Aborting ETL job.")

# Data validation: Check for negative order_totals
if df.filter(col("order_total") < 0).count() > 0:
    raise Exception("Invalid order_total found: Cannot have negative values.")
    
# Column transformations
df = df.select(
    col("order_id"),
    col("customer_id"),
    col("order_total").cast(DoubleType()),
    col("category"),
    col("product_id"),
    col("quantity"),
    to_date(col("order_date"), "yyyy-MM-dd").alias("order_date")
)

# Fill missing values
df = df.withColumn("processing_date", col("order_date")) \
       .na.fill({"order_total": 0.0, "category": "unknown"})

output_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output_df")

# --- Write to Parquet (Partitioned) ---
try:
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": args["destination_parquet_path"],
            "partitionKeys": ["processing_date"]
        },
        format="parquet",
        transformation_ctx="parquet_sink"
    )
    logging.info(f"Data written to Parquet at: {args['destination_parquet_path']}")
except Exception as e:
    logging.error(f"Failed to write to Parquet: {str(e)}")
    raise

# --- Register with Glue Data Catalog ---
try:
    glueContext.write_dynamic_frame.from_catalog(
        frame=output_dynamic_frame,
        database=args["database_name"],
        table_name=args["table_name"],
        additional_options={"partitionKeys": ["processing_date"]},
        transformation_ctx="catalog_sink"
    )
    logging.info(f"Table {args['table_name']} successfully registered in Glue Catalog.")
except Exception as e:
    logging.error(f"Failed to register table in Glue Catalog: {str(e)}")
    raise

# --- Write to CSV (Non-Partitioned for Simplicity) ---
try:
    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": args["destination_csv_path"]
        },
        format="csv",
        format_options={"separator": ",", "quoteChar": '"', "writeHeader": True},
        transformation_ctx="csv_sink"
    )
    logging.info(f"Data written to CSV at: {args['destination_csv_path']}")
except Exception as e:
    logging.error(f"Failed to write to CSV: {str(e)}")
    raise

# --- Trigger a Glue Crawler (optional) ---
try:
    glue_client = boto3.client('glue')
    glue_client.start_crawler(Name=args["crawler_name"])
    logging.info("Glue Crawler triggered successfully.")
except Exception as e:
    logging.error(f"Failed to start Glue Crawler: {str(e)}")

job.commit()
