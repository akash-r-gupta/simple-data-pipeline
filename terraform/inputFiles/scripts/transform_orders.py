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

# --- Get job arguments ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_path",
    "destination_parquet_path",
    "destination_csv_path"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Read raw CSV input ---
input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["source_path"]]},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

# --- Transform ---
df = input_dynamic_frame.toDF()

if df.rdd.isEmpty():
    raise Exception("Input data is empty. Aborting ETL job.")

df = df.select(
    col("order_id"),
    col("customer_id"),
    col("order_total").cast(DoubleType()),
    col("category"),
    to_date(col("order_date"), "yyyy-MM-dd").alias("order_date")
)

df = df.withColumn("processing_date", col("order_date")) \
       .na.fill({"order_total": 0.0, "category": "unknown"})

output_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "output_df")

# --- Write to Parquet (Partitioned) ---
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

# --- Register with Glue Data Catalog ---
glueContext.write_dynamic_frame.from_catalog(
    frame=output_dynamic_frame,
    database="datalake_db",
    table_name="curated_orders",
    additional_options={"partitionKeys": ["processing_date"]},
    transformation_ctx="catalog_sink"
)

# --- Write to CSV (Non-Partitioned for Simplicity) ---
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

job.commit()
