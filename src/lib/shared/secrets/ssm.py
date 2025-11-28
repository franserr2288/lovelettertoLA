import boto3


def get_secret_from_ssm(param_name, is_secure_string=True):
    ssm = boto3.client('ssm')
    response = ssm.get_parameter(
        Name=param_name,
        WithDecryption=is_secure_string
    )
    return response['Parameter']['Value']