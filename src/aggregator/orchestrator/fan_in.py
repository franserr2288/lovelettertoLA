import json
import boto3
from shared.messages.sqs import get_queue_url
from shared.models.tables import JobBatch, JOB_BATCH_SK
from shared.utils.logging.logger import setup_logger


from pynamodb.expressions.update import Action
from pynamodb.expressions.operand import Value

logger = setup_logger(__name__)

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    body = json.loads(event["Records"][0]["body"])
    analysis_path = body["ANALYSIS_PATH"]

    JobBatch(hash_key="snapshot_generation", range_key=JOB_BATCH_SK).update(
        actions=[
            Action(JobBatch.completed, Value(1), action='ADD')
        ]
    )

    batch = JobBatch.get(hash_key="snapshot_generation", range_key=JOB_BATCH_SK)

    if batch.completed == batch.expected:
        kick_off_next_processing_layer(body["DATASET_NAME"], body["PARTITION_COL"], analysis_path)


def kick_off_next_processing_layer(dataset_name, partition_col, analysis_path):
    sqs = boto3.client("sqs")
    # TODO alter queue names later
    # TODO: create mapping based on inputs that maps to a specific queue name, so that i can reuse this as an orchestration 
    #       piece between components 

    queue_url = get_queue_url(sqs, "SocrataSnapshotRollUpQueue")
    
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({
            "PROCESSED_DATA_PATH": analysis_path, 
            "DATASET_NAME": dataset_name,
            "PARTITION_COL": partition_col,
        }),
    )

