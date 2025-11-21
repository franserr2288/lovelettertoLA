import requests, os, json
import awswrangler as wr 
import pandas as pd


def handler(event, context):
    body = json.loads(event["Records"][0]["body"])
    dataset_name = body["DATASET_NAME"]
    dataset_resource_id = body["DATASET_RESOURCE_ID"]
    format = body["FORMAT"]

    assert format
    assert dataset_name 
    assert dataset_resource_id


    url = construct_url_for_full_dataset_json()
    headers = {
        'Content-Type': 'application/json',
        'X-App-Token': os.environ["SOCRATA_APP_TOKEN"],
    }

    response = requests.get(url=url, headers=headers)
    data = response.json()

    data_frame = pd.DataFrame(data)
    bucket_name = os.environ["BUCKET_NAME"]

    raw_data_path = f"s3://{bucket_name}/{dataset_name}/raw/data.csv"
    parquet_path = f"s3://{bucket_name}/{dataset_resource_id}/parquet/"
    
    if format == "CSV":
        wr.s3.to_csv(df=data_frame, path=raw_data_path, index=False)
    else:
        wr.s3.to_parquet(df=data_frame, dataset=True, path=parquet_path, max_rows_by_file=100000)
    

def construct_url_for_full_dataset_json():
    base_url = os.environ["BASE_URL"]
    resource_id = os.environ["DATASET_RESOURCE_ID"]
    return base_url+"/api/v3/views/"+resource_id+"/query.json"