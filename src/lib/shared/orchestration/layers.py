import os
from shared.messages.sqs import get_sqs_client_and_url
from shared.models.tables import JobBatch


def kick_off_processing_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id, format):
    partition_values = data_frame[partition_col].unique().tolist()
    sqs_client, queue_url = get_client_and_url_for_processing_queue(dataset_name)
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
    batch = JobBatch.create(job_type=f"snapshot_generation_{dataset_name}", expected=len(partition_values))
    batch.save()

#TODO: change queue names in template and here to better reflect their purpose
def get_client_and_url_for_processing_queue(dataset_name):
    region = os.environ["REGION"]
    return get_sqs_client_and_url(queue_name="SocrataSnapshotGeneratorQueue", region=region)