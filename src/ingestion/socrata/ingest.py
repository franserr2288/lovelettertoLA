import boto3
import requests, os, json
import awswrangler as wr 
import pandas as pd
import traceback
from helper import setup_logger
import datetime as dt

logger = setup_logger(__name__)

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    try:
        # batch size 1
        body = json.loads(event["Records"][0]["body"])
        dataset_name = body["DATASET_NAME"]
        dataset_resource_id = body["DATASET_RESOURCE_ID"]
        format = body["FORMAT"]

        today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


        url = construct_url_for_full_dataset_json(dataset_resource_id)
        headers = {
            'Content-Type': 'application/json',
            'X-App-Token': get_app_token(),
        }

        response = requests.get(url=url, headers=headers)
        data = response.json()

        data_frame = pd.DataFrame(data)
        bucket_name = os.environ["BUCKET_NAME"]

        
        # if format == "CSV":
        #     path = f"s3://{bucket_name}/{dataset_name}/raw/ingestion_date={today_str}/data.csv"
        #     wr.s3.to_csv(df=data_frame, path=path, index=False)
        if format == "PARQUET":
            parquet_path = f"s3://{bucket_name}/{dataset_name}/raw/ingestion_date={today_str}"
            wr.s3.to_parquet(
                df=data_frame, 
                dataset=True, 
                path=parquet_path, 
                mode="overwrite",
                compression="snappy"
            )
        else:
            logger.error("Invalid input, only supporting CSV or PARQUET")
            raise ValueError
    
    except Exception as e:
        logger.exception(f"Exception: {json.dumps(e)}")
        raise


def construct_url_for_full_dataset_json(dataset_resource_id):
    base_url = os.environ["BASE_URL"]
    return base_url + "/api/v3/views/" + dataset_resource_id + "/query.json"

def get_app_token():
    socrata_app_token_param_name = os.environ["SOCRATA_TOKEN_PARAM_NAME"]
    return get_secret_from_ssm(socrata_app_token_param_name, True)

def get_secret_from_ssm(param_name, is_secure_string=True):
    ssm = boto3.client('ssm')
    response = ssm.get_parameter(
        Name=param_name,
        WithDecryption=is_secure_string
    )
    return response['Parameter']['Value']
    