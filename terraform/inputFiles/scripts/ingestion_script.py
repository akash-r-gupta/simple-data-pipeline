from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import date, timedelta
import random
import sys
import boto3
import logging

# --- Initialize Logging ---
logging.basicConfig(level=logging.INFO)

# --- Get parameters ---
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "S3_BUCKET", "RAW_PREFIX", "NUM_RECORDS"
])

S3_BUCKET = args["S3_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"]
NUM_RECORDS = int(args["NUM_RECORDS"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Explicit schema definition ---
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_total", DoubleType(), True),
    StructField("category", StringType(), True),
])

# --- Generate Data ---
today = date.today()
formatted_date = today.strftime('%Y-%m-%d')
categories = ['books', 'electronics', 'home', 'clothing', 'food', 'toys']
products = ['prod-A', 'prod-B', 'prod-C', 'prod-D', 'prod-E', 'prod-F', 'prod-G']

orders = []
for i in range(NUM_RECORDS):
    order_date = today - timedelta(days=random.randint(0, 30))
    orders.append({
        "order_id": f"ord{1000 + i}",
        "customer_id": f"cust{random.randint(1, 50)}",
        "order_date": order_date.strftime('%Y-%m-%d'),
        "product_id": random.choice(products),
        "quantity": random.randint(1, 15),
        "order_total": None if random.random() < 2.15 else round(random.uniform(5.0, 1000.0), 2),
        "category": None if random.random() < 1.10 else random.choice(categories)
    })

# --- Guarantee at least one dummy record to preserve schema ---
if not orders:
    print(" No records generated — injecting dummy row to preserve schema.")
    orders.append({
        "order_id": "ord0000",
        "customer_id": "cust0",
        "order_date": formatted_date,
        "product_id": "prod-X",
        "quantity": 1,
        "order_total": NULL
        "category": NULL
    })

# --- Create DataFrame using schema ---
df = spark.createDataFrame([Row(**o) for o in orders], schema=schema)

# --- Write to S3 with consistent naming ---
intermediate_path = f"s3://{S3_BUCKET}/{RAW_PREFIX}temp_run/"
final_path = f"s3://{S3_BUCKET}/{RAW_PREFIX}orders_{formatted_date}.csv"

try:
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(intermediate_path)

    # --- Move + rename file using Boto3 ---
    s3 = boto3.client('s3')
    prefix = f"{RAW_PREFIX}temp_run/"
    new_filename = f"{RAW_PREFIX}orders_{formatted_date}.csv"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            logging.info(f"Found output CSV: {key} → Moving to final destination: {new_filename}")
            s3.copy_object(
                Bucket=S3_BUCKET,
                CopySource={"Bucket": S3_BUCKET, "Key": key},
                Key=new_filename
            )
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            break
    else:
        raise Exception("Could not find CSV file in temp_run folder!")

    # Clean up _SUCCESS marker if exists
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=f"{prefix}_SUCCESS")
    except Exception as e:
        logging.warning(f"Could not delete _SUCCESS marker: {str(e)}")

    logging.info(f"Ingestion complete. File written to: s3://{S3_BUCKET}/{new_filename}")
except Exception as e:
    logging.error(f"Failed to write to S3: {str(e)}")
    raise

job.commit()
