import json
import boto3

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

ORDER_BUCKET = 'flipdummyreturned'
RETURNS_BUCKET = 'flipedummyorders'

def lambda_handler(event, context):
    # Check if new file is uploaded in one of the buckets
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    # Check if files exist in both Order and Returns bucket
    order_files = check_s3_bucket_for_files(ORDER_BUCKET)
    returns_files = check_s3_bucket_for_files(RETURNS_BUCKET)

    if order_files and returns_files:
        # If files exist in both, trigger Glue ETL job
        try:
            response = glue_client.start_job_run(JobName='glue etl')
            print(f"Glue job started: {response['JobRunId']}")
        except Exception as e:
            print(f"Error starting Glue job: {str(e)}")
    else:
        print("Both buckets don't have files. Glue job not triggered.")

def check_s3_bucket_for_files(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return 'Contents' in response  # Returns True if there are files
