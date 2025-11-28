import boto3
import requests, os, json
import awswrangler as wr 
import pandas as pd
from helper import setup_logger
import datetime as dt

logger = setup_logger(__name__)



def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    try:
        # batch size 1
        body = json.loads(event["Records"][0]["body"])

        dataset_name: str = body["DATASET_NAME"]
        format = body["FORMAT"]
        partition_col = body["PARTITION_COL"] # council_district_number for 311
        dataset_resource_id = body["DATASET_RESOURCE_ID"]


        today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        bucket_name = os.environ["BUCKET_NAME"]
        data_frame = get_data(dataset_resource_id)

        if format not in ["CSV", "PARQUET"]:
            raise ValueError

        
        if format == "CSV":
            path = f"s3://{bucket_name}/{dataset_name}/raw/ingestion_date={today_str}/data.csv"
            wr.s3.to_csv(df=data_frame, path=path, index=False)
        else:
            path = f"s3://{bucket_name}/{dataset_name}/raw/ingestion_date={today_str}"
            wr.s3.to_parquet(
                df=data_frame, 
                dataset=True, 
                path=path, 
                mode="overwrite",
                compression="snappy",
                partition_cols=[partition_col] # break dataset up now to make it easy for downstream consumers
            )
            kick_off_processing_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id) 
    
    except Exception as e:
        logger.exception(f"Exception: {json.dumps(e)}")
        raise

def kick_off_processing_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id):
    partition_values = data_frame[partition_col].unique().tolist()
    sqs_client, queue_url = get_client_and_url_for_processing_queue(dataset_name)
    for val in partition_values:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "PATH": f"{path}/{partition_col}={val}",
                "DATASET_NAME": dataset_name,
                "DATASET_RESOURCE_ID": dataset_resource_id,
                "PARTITION_COL": partition_col,
                "PARTITION_VALUE": val
            }),
        )  
    # kick off processor/orchestrator
    orchestrator_queue_url = get_client_and_url_for_orchestrator_queue(sqs_client)
    sqs_client.send_message(
        QueueUrl=orchestrator_queue_url,
        MessageBody=json.dumps({
            "TASK_TYPE": "POLLER",
            "DATASET_NAME": dataset_name,
            "PARTITION_COL": partition_col,
            "OUTPUT_PATH": path,
            "EXPECTED_COUNT": len(partition_values),
            "START_TIME": dt.now(dt.timezone.utc).timestamp()
        }),
    )  
     


def get_data(dataset_resource_id):
    url = construct_url_for_full_dataset_json(dataset_resource_id)
    headers = {
        'Content-Type': 'application/json',
        'X-App-Token': get_app_token(),
    }

    response = requests.get(url=url, headers=headers)
    data = response.json()

    data_frame = pd.DataFrame(data)
    return data_frame
     
def get_client_and_url_for_processing_queue(dataset_name):
    sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_url(
        QueueName=f"Socrata{dataset_name}ProcessingQueue"
    )
    queue_url = response["QueueUrl"]
    return sqs_client, queue_url

def get_client_and_url_for_orchestrator_queue(sqs_client):
    response = sqs_client.get_queue_url(
        QueueName=f"SocrataOrchestratorQueue"
    )
    queue_url = response["QueueUrl"]
    return queue_url

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
    