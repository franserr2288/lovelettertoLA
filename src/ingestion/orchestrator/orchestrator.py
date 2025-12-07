import boto3, json

def handler(event, context):

    api = event.get("SOURCE")
    queue_name = event["QUEUE_NAME"]

    payload = {
        "DATASET_NAME": event["DATASET_NAME"],
        "DATASET_RESOURCE_ID": event["DATASET_RESOURCE_ID"], 
        "PARTITION_COL": event["PARTITION_COL"]
    }

    if api == "ESRI":
        payload["NEEDS_PROCESSING"] = event.get("NEEDS_PROCESSING", False)
        payload["LAYERS"] = [] 

    sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )

    queue_url = response["QueueUrl"]

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
    )
