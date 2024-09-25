import json
import boto3

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
step_client = boto3.client('stepfunctions')

ORDER_BUCKET = 'Enter your order budget name'
RETURNS_BUCKET = 'Enter your return budget name'

def lambda_handler(event, context):
    
    # Check if new file is uploaded in one of the buckets
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    # Check if files exist in both Order and Returns bucket
    order_files = check_s3_bucket_for_files(ORDER_BUCKET)
    returns_files = check_s3_bucket_for_files(RETURNS_BUCKET)

    input_data = {
        'file1': f"s3://{ORDER_BUCKET}/ your csv file name",
        'file2': f"s3://{RETURNS_BUCKET}/your csv file name"  # Replace with the second file's key
    }

    if order_files and returns_files:
        # If files exist in both, trigger Glue ETL job
        try:
            #response = glue_client.start_job_run(JobName='your glue job name')
            step_response = step_client.start_execution(
        stateMachineArn="Enter your arn step function",
    )
            print(f"Glue job started: {response['JobRunId']}")
            print(f"Glue job started: {step_response}")
        except Exception as e:
            print(f"Error starting Glue job: {str(e)}")
    else:
        print("Both buckets don't have files. Glue job not triggered.")

def check_s3_bucket_for_files(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    return 'Contents' in response  # Returns True if there are files
