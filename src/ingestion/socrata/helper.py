import requests, os, json
import awswrangler as wr 
import pandas as pd
from shared.messages.sqs import get_sqs_client_and_url
from shared.models.tables import JobBatch
from shared.secrets.ssm import get_secret_from_ssm



def get_data(source, dataset_resource_id, partition_col, layer_ids):
    if source not in ["SOCRATA"]:
        raise ValueError("bad input")
    
    data_frame = get_socrata_data(dataset_resource_id)
    data_frame_filtered = data_frame[data_frame[partition_col].notna()]
    
    return data_frame_filtered


def get_socrata_data(dataset_resource_id):
    url = construct_url_for_full_socrata_dataset_json(dataset_resource_id)
    headers = {
        'Content-Type': 'application/json',
        'X-App-Token': get_app_token(),
    }

    response = requests.get(url=url, headers=headers)
    data = response.json()

    data_frame = pd.DataFrame(data)
    return data_frame

def construct_url_for_full_socrata_dataset_json(dataset_resource_id):
    base_url = os.environ["BASE_URL"]
    return base_url + "/api/v3/views/" + dataset_resource_id + "/query.json"

def get_app_token():
    socrata_app_token_param_name = os.environ["SOCRATA_TOKEN_PARAM_NAME"]
    region = os.environ["REGION"]
    return get_secret_from_ssm(socrata_app_token_param_name, True, region)
