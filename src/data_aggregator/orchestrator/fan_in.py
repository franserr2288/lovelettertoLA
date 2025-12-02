import datetime
import json
import os
import awswrangler as wr
import boto3
from shared.utils.logging.logger import setup_logger

logger = setup_logger(__name__)



POLL_DELAY_SECONDS = 60  # check S3 every minute
MAX_POLL_DURATION_MINS = 60  # kill switch

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    body = json.loads(event["Records"][0]["body"])
    analysis_path = body["ANALYSIS_PATH"]
    expected_count = body["EXPECTED_COUNT"]
    start_time = body["START_TIME"]
    
    elapsed_mins = (datetime.datetime.now(datetime.timezone.utc).timestamp() - start_time) / 60
    if elapsed_mins > MAX_POLL_DURATION_MINS:
        print(f"TIMEOUT: Batch took longer than {MAX_POLL_DURATION_MINS} mins. Stopping poller.")
        return
    
    try:
        # remove wr here, heavy dependency for not much benefit
        files = wr.s3.list_objects(analysis_path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        current_count = len(parquet_files)
    except Exception:
        current_count = 0

    if current_count >= expected_count:
        kick_off_next_processing_layer(body["DATASET_NAME"], body["PARTITION_COL"], analysis_path)
    else:
        requeue_poller(body)
        
def requeue_poller(body):
    sqs = boto3.client("sqs")
    queue_url = get_queue_url(sqs, os.environ["SQS_QUEUE_NAME"])
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body), DelaySeconds=POLL_DELAY_SECONDS)

def kick_off_next_processing_layer(dataset_name, partition_col, analysis_path):
    sqs = boto3.client("sqs")
    # TODO alter queue names later
    queue_url = get_queue_url(sqs, "SocrataSnapshotRollUpQueue")
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({
            "PROCESSED_DATA_PATH": analysis_path, 
            "DATASET_NAME": dataset_name,
            "PARTITION_COL": partition_col,
        }),
    )

def get_queue_url(client, name):
    return client.get_queue_url(QueueName=name)["QueueUrl"]