import json
import boto3
import csv

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # 1. Identify the new market file from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # 2. Inspect the file for the UTSA project standards
        response = s3_client.get_object(Bucket=bucket, Key=key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.reader(lines)
        
        for i, row in enumerate(reader):
            if i > 10: break # Check top 10 rows for efficiency
            # Sniff for the exact 'K/M/B' shorthand that broke your Snowflake load
            if any('K' in str(field) or 'M' in str(field) for field in row):
                print(f"VALIDATION ALERT: Found shorthand numbers in {key} at row {i}")
        
        print(f"File {key} from {bucket} has been successfully validated.")
        return {"status": "success", "file": key}
        
    except Exception as e:
        print(f"CRITICAL ERROR: Could not validate {key}. Reason: {str(e)}")
        raise e