import json
import boto3
from shared.messages.sqs import get_queue_url
from shared.models.tables import JobBatch
from shared.utils.logging.logger import setup_logger

logger = setup_logger(__name__)

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    body = json.loads(event["Records"][0]["body"])

    message_id = event["Records"][0]["messageId"] 
    job_type = body["JOB_TYPE"]
    dataset_name = body["DATASET_NAME"]
    partition_col = body["PARTITION_COL"]

    JobBatch.increment_completed(job_type, message_id)
    batch = JobBatch.get_batch(job_type)

    if batch.completed == batch.expected:
        kick_off_next_processing_layer(dataset_name, partition_col)


def kick_off_next_processing_layer(dataset_name, partition_col):
    sqs = boto3.client("sqs")
    # TODO alter queue names later
    # TODO: create mapping based on inputs that maps to a specific queue name, so that i can reuse this as an orchestration 
    #       piece between components 

    queue_url = get_queue_url(sqs, "SocrataSnapshotRollUpQueue")
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({
            "DATASET_NAME": dataset_name,
            "PARTITION_COL": partition_col,
        }),
    )

