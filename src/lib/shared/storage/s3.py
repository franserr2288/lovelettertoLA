import json
import boto3


def read_json_from_s3(bucket, key, s3_client=None):
    if not s3_client:
        s3_client = boto3.client('s3')
    
    try:
        s3_response = s3_client.get_object(Bucket=bucket, Key=key)

        s3_object_body = s3_response['Body']
        content_bytes = s3_object_body.read()
        
        content_str = content_bytes.decode('utf-8')

        json_data = json.loads(content_str)
        
        return json_data
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def find_and_read_snapshots(bucket_name, prefix):
    return find_and_read_all_jsons(bucket_name, prefix, "snapshot.json")

def find_and_read_all_jsons(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    snapshot_keys = []
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('snapshot.json'):
                snapshot_keys.append(obj['Key'])
    
    
    results = [
        read_json_from_s3(bucket_name, key, s3) 
        for key in snapshot_keys
    ]
    return results