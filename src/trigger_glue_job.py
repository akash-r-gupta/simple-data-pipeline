import json
import boto3
import os
import urllib.parse

print("Loading function")
glue = boto3.client("glue")
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

def handler(event, context):
    """
    Triggers a Glue job when a file is uploaded to the 'raw/orders/' S3 prefix.
    """
    try:
        # Get the bucket and key from the S3 event
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding='utf-8')
        
        print(f"File uploaded: s3://{bucket}/{key}")

        # Define source and destination paths for the Glue job
        source_path = f"s3://{bucket}/{key}"
        destination_parquet_path = os.environ["DESTINATION_PARQUET_PATH"]
        destination_csv_path = os.environ["DESTINATION_CSV_PATH"]

        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--source_path": source_path,
                "--destination_parquet_path": destination_parquet_path,
                "--destination_csv_path": destination_csv_path,
            },
        )
        
        print(f"Started Glue job: {GLUE_JOB_NAME}")
        print(f"Job Run ID: {response['JobRunId']}")
        
        return {"status": "success", "job_run_id": response["JobRunId"]}

    except Exception as e:
        print(f"Error processing event: {event}")
        print(f"Error: {e}")
        raise e



