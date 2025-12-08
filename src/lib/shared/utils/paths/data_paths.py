#TODO: need better/clearer naming for future me
def get_dated_snapshot_root_path(bucket_name, dataset_name, date):
    return f"{get_base_dataset_processing_path(bucket_name, dataset_name)}/snapshot_analysis/analysis_date={date}/"  

def get_partition_snapshot_path(bucket_name, dataset_name, date, partition_col, partition_val):
    return get_dated_snapshot_root_path(bucket_name, dataset_name, date) + f"{partition_col}={partition_val}"

def get_partition_snapshot_json_file_path(bucket_name, dataset_name, date, partition_col, partition_val):
    return get_dated_snapshot_root_path(bucket_name, dataset_name, date) + f"{partition_col}={partition_val}" + "/snapshot.json"

def get_ingestion_path(bucket_name, dataset_name, date):
    return f"{get_base_dataset_processing_path(bucket_name, dataset_name)}/raw/ingestion_date={date}"

def get_dated_aggregate_snapshot_path(bucket_name, dataset_name, date):
    return f"{get_base_dataset_processing_path(bucket_name, dataset_name)}/final_rollups/daily/analysis_date={date}/"

def get_dated_aggregate_snapshot_json_file_path(bucket_name, dataset_name, date):
    return get_dated_aggregate_snapshot_path(bucket_name, dataset_name, date) + "/snapshot_rollup.json"

def get_base_dataset_processing_path(bucket_name, dataset_name):
    return f"{get_bath_path(bucket_name)}/{dataset_name}"

def get_bath_path(bucket_name):
    return f"s3://{bucket_name}"

# events 
def get_event_id_path(date, event_type, event_id):
    return f"events/{date}/{event_type}/{event_id}"

def get_all_events_path(date, event_type=None):
    prefix = f"events/{date}/"
    if event_type:
        prefix += f"{event_type}/"
    return prefix
                      
# reference data
def get_reference_data_path(bucket_name):
    return f"{get_bath_path(bucket_name)}/reference_data/"

# storing small files as jsons, don't need them in parquet format
def get_reference_dataset_path(bucket_name, dataset_name):
    return f"{get_bath_path(bucket_name)}/reference_data/{dataset_name}.json"