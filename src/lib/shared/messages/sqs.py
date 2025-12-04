import boto3


def get_sqs_client_and_url(queue_name, sqs_client=None, region="us-east-1"):
    if not sqs_client:
        sqs_client = boto3.client("sqs", region_name=region)
    queue_url = get_queue_url(sqs_client, queue_name)
    return sqs_client, queue_url

def get_queue_url(client, name):
    return client.get_queue_url(QueueName=name)["QueueUrl"]