import boto3


def get_secret_from_ssm(param_name, is_secure_string=True, region="us-east-1"):
    ssm = boto3.client('ssm', region_name=region)
    response = ssm.get_parameter(
        Name=param_name,
        WithDecryption=is_secure_string
    )
    return response['Parameter']['Value']