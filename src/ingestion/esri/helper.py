import requests, os
import pandas as pd

def get_data(source, dataset_resource_id, partition_col, layer_ids, service_url):
    if source not in ["ESRI"]:
        raise ValueError("bad input")

    data_frame = get_esri_data(dataset_resource_id, layer_ids, service_url)
    data_frame_filtered = data_frame[data_frame[partition_col].notna()]
    return data_frame_filtered

def get_esri_data(dataset_resource_id, layer_ids, service_url):
    #TODO: make the orchestrator do this so that this can just assume the layer ids are there
    if not layer_ids:
        layer_ids = get_all_layer_ids(service_url)
    
    all_data = []
    for layer_id in layer_ids:
        layer_url = construct_url_for_arcgis_featureserver(dataset_resource_id, layer_id)
        
        response = requests.get(url=layer_url)
        data = response.json()
        
        if 'features' in data:
            records = [feature['attributes'] for feature in data['features']]
            df = pd.DataFrame(records)
            df['layer_id'] = layer_id
            all_data.append(df)
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def get_all_layer_ids(service_url):
    response = requests.get(f"{service_url}?f=json")
    metadata = response.json()
    return [layer['id'] for layer in metadata.get('layers', [])]

def construct_url_for_arcgis_featureserver(
    dataset_resource_id,
    layer_id,
    where_clause="1=1",
    out_fields="*",
    out_sr="4326",
    response_format="json"
):
    feature_server_url = f"{construct_service_url(dataset_resource_id)}/{layer_id}/query"
    
    params = {
        'where': where_clause,
        'outFields': out_fields,
        'outSR': out_sr,
        'f': response_format
    }
    
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
    full_url = f"{feature_server_url}?{query_string}"
    
    return full_url

def construct_service_url(dataset_resource_id):
    base_url = os.environ.get("ARCGIS_BASE_URL", "https://services.arcgis.com")
    feature_server_url = f"{base_url}/{dataset_resource_id}/FeatureServer/"
    return feature_server_url
