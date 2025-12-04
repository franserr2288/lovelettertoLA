import boto3, json

def handler(event, context):
    queue_name = event["QUEUE_NAME"]
    dataset_name = event["DATASET_NAME"]
    dataset_resource_id = event["DATASET_RESOURCE_ID"]
    format = event["FORMAT"]
    partition_col = event["PARTITION_COL"]


    sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response["QueueUrl"]

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"DATASET_NAME":dataset_name, "DATASET_RESOURCE_ID":dataset_resource_id, "FORMAT":format, "PARTITION_COL":partition_col}),
    )
