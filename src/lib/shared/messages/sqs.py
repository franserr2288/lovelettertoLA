import boto3


def get_sqs_client_and_url(queue_name, sqs_client=None):
    if not sqs_client:
        sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response["QueueUrl"]
    return sqs_client, queue_url