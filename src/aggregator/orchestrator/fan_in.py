import json
import boto3
from shared.messages.sqs import get_queue_url
from shared.models.tables import JobBatch, JOB_BATCH_SK
from shared.utils.logging.logger import setup_logger


from pynamodb.expressions.update import AddAction
from pynamodb.expressions.operand import Value
from pynamodb.exceptions import UpdateError

logger = setup_logger(__name__)

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    body = json.loads(event["Records"][0]["body"])

    message_id = event["Records"][0]["messageId"] 
    job_type = body["JOB_TYPE"]
    dataset_name = body["DATASET_NAME"]
    partition_col = body["PARTITION_COL"]
    # JobBatch(hash_key="snapshot_generation", range_key=JOB_BATCH_SK).update(
    #     actions=[
    #         Action(JobBatch.completed, Value(1), action='ADD')
    #     ]
    # )
    try:
        JobBatch(job_type).update(
            actions=[
                AddAction(JobBatch.completed, Value(1), action='ADD'),
                AddAction(JobBatch.processed_message_ids, Value({message_id}), action='ADD')
            ],
            condition=(JobBatch.processed_message_ids.does_not_contain(message_id))
        )
    except UpdateError as e:
        if e.cause_response_code == "ConditionalCheckFailedException":
            logger.info(f"Message {message_id} already processed (idempotent retry)")
        else:
            raise

    batch = JobBatch.get(hash_key="snapshot_generation", range_key=JOB_BATCH_SK)

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

