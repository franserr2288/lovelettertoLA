import json
import os
from shared.messages.sqs import get_sqs_client_and_url
from shared.models.tables import JobBatch


def kick_off_analytics_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id, job_type):
    partition_values = data_frame[partition_col].unique().tolist()
    if job_type == "snapshot_generation":
        sqs_client, queue_url = get_client_and_url_for_processing_queue()
        batch = JobBatch.create(job_type=f"snapshot_generation_{dataset_name}", expected=len(partition_values))
        batch.save()
    elif job_type == "reference_normalization":
        sqs_client, queue_url = get_client_and_url_for_reference_normalization_queue()
    else: 
        raise ValueError("bad input")

    for val in partition_values:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "INGESTION_PATH": f"{path}/{partition_col}={val}",
                "DATASET_NAME": dataset_name,
                "DATASET_RESOURCE_ID": dataset_resource_id,
                "PARTITION_COL": partition_col,
                "PARTITION_VALUE": val,
            }),
        )  

#TODO: change queue names in template and here to better reflect their purpose
def get_client_and_url_for_processing_queue():
    region = os.environ["REGION"]
    return get_sqs_client_and_url(queue_name="SnapshotGeneratorQueue", region=region)

# for when you have lighterweight reference datasets that change less frequently
def get_client_and_url_for_reference_normalization_queue():
    region = os.environ["REGION"]
    return get_sqs_client_and_url(queue_name="ReferenceNormalizationQueue", region=region)