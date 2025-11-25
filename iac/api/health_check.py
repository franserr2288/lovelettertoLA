import json

def handler(event, context):
    response_body = {
        "status": "OK",
        "message": "Service is reachable."
    }

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
        },
        'body': json.dumps(response_body)
    }