#TODO: need better/clearer naming for future me
def get_dated_snapshot_root_path(bucket_name, dataset_name, today_str):
    return f"{get_base_path(bucket_name, dataset_name)}/snapshot_analysis/analysis_date={today_str}/"  

def get_partition_snapshot_path(bucket_name, dataset_name, today_str, partition_col, partition_val):
    return get_dated_snapshot_root_path((bucket_name, dataset_name, today_str)) + f"{partition_col}={partition_val}"

def get_partition_snapshot_json_file_path(bucket_name, dataset_name, today_str, partition_col, partition_val):
    return get_dated_snapshot_root_path((bucket_name, dataset_name, today_str)) + f"{partition_col}={partition_val}" + "/snapshot.json"

def get_ingestion_path(format, bucket_name, dataset_name, today_str):
    if format == "CSV":
        return f"{get_base_path(bucket_name, dataset_name)}/raw/ingestion_date={today_str}/data.csv"
    else:
        return f"{get_base_path(bucket_name, dataset_name)}/raw/ingestion_date={today_str}"

def get_dated_aggregate_snapshot_path(bucket_name, dataset_name, today_str):
    return f"{get_base_path(bucket_name, dataset_name)}/final_rollups/daily/analysis_date={today_str}/"

def get_dated_aggregate_snapshot_path(bucket_name, dataset_name, today_str):
    return get_dated_aggregate_snapshot_path(bucket_name, dataset_name, today_str) + "/snapshot_rollup.json"

def get_base_path(bucket_name, dataset_name):
    return f"s3://{bucket_name}/{dataset_name}"