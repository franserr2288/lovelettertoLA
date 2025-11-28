import requests, os, json
import awswrangler as wr 
import pandas as pd
from lib.shared.messages.sqs import get_sqs_client_and_url
from lib.shared.secrets.ssm import get_secret_from_ssm
from lib.shared.utils.logging.logger import setup_logger
from lib.shared.utils.paths.data_paths import get_ingestion_path
from lib.shared.utils.time.time_utils import get_today_str, get_time_stamp

logger = setup_logger(__name__)
BUCKET_NAME = os.environ["BUCKET_NAME"]

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    try:
        # batch size 1
        body = json.loads(event["Records"][0]["body"])
        format = body["FORMAT"]
        if format not in ["CSV", "PARQUET"]:
            raise ValueError

        dataset_name: str = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
        dataset_resource_id = body["DATASET_RESOURCE_ID"]
        data_frame = get_data(dataset_resource_id)

        if format == "CSV":
            wr.s3.to_csv(
                df=data_frame, 
                path=get_ingestion_path(format,BUCKET_NAME, dataset_name, get_today_str()), 
                index=False
            )
        else:
            path = get_ingestion_path(format, BUCKET_NAME, dataset_name, get_today_str())
            wr.s3.to_parquet(
                df=data_frame, 
                dataset=True, 
                path=path,
                mode="overwrite",
                compression="snappy",
                partition_cols=[partition_col]
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
    orchestrator_queue_url = get_client_and_url_for_orchestrator_queue(sqs_client)
    sqs_client.send_message(
        QueueUrl=orchestrator_queue_url,
        MessageBody=json.dumps({
            "TASK_TYPE": "POLLER",
            "DATASET_NAME": dataset_name,
            "PARTITION_COL": partition_col,
            "OUTPUT_PATH": path,
            "EXPECTED_COUNT": len(partition_values),
            "START_TIME": get_time_stamp()
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

def construct_url_for_full_dataset_json(dataset_resource_id):
    base_url = os.environ["BASE_URL"]
    return base_url + "/api/v3/views/" + dataset_resource_id + "/query.json"
     
def get_client_and_url_for_processing_queue(dataset_name):
    return get_sqs_client_and_url(f"Socrata{dataset_name}ProcessingQueue")

def get_client_and_url_for_orchestrator_queue(sqs_client):
    _, queue_url = get_sqs_client_and_url("SocrataOrchestratorQueue")
    return queue_url

def get_app_token():
    socrata_app_token_param_name = os.environ["SOCRATA_TOKEN_PARAM_NAME"]
    return get_secret_from_ssm(socrata_app_token_param_name, True)


    