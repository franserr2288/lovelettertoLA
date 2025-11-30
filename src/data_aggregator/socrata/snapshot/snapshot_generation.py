import os, json
import awswrangler as wr 
import pandas as pd
import traceback
import datetime as dt
from datetime import timezone
from shared.utils.paths.data_paths import get_dated_snapshot_root_path, get_partition_snapshot_json_file_path
from shared.utils.time.time_utils import get_today_str
# TODO: geospatial analysis with the location data they give

def handler(event, context):
    try:
        bucket_name = os.environ["BUCKET_NAME"]
        
        body = json.loads(event["Records"][0]["body"])
        format = body["FORMAT"]
        dataset_name: str = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
                
        if format != "PARQUET" or dataset_name != "City311":
            raise ValueError("Unsupported format or dataset.")
        
        path = body["PATH"]
        partition_val = body["PARTITION_VALUE"]

        df = wr.s3.read_parquet(path=path)
        today_str = get_today_str()

        snapshot_metrics = run_analysis(
            dataset_name, 
            df, 
            partition_val, 
            today_str 
        )
        snapshot_metrics["analysis_date"] = today_str

        metrics_df = pd.DataFrame([snapshot_metrics])
        wr.s3.to_parquet(
            df=metrics_df,
            path=get_dated_snapshot_root_path(bucket_name, dataset_name, today_str),
            dataset=True,
            mode="overwrite",
            partition_cols=[partition_col],
        )
        wr.s3.to_json(
            df=metrics_df,
            path=get_partition_snapshot_json_file_path(bucket_name, dataset_name, today_str, partition_col, partition_val),
            partition_cols=[partition_col],
        )
    except Exception as e:
        print(f"An error occurred in handler: {e}")
        print(traceback.format_exc())


def run_analysis(dataset_name, df, partition_val, today_str):
    if dataset_name == "City311":    
        return run_city_311_analysis(df, partition_val, today_str)
    else:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

def run_city_311_analysis(df, partition_val, today_str):
    
    date_cols = ['CreatedDate', 'UpdatedDate', 'ClosedDate', 'ServiceDate']
    for col in date_cols:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
    
    today_utc_date_obj = dt.datetime.strptime(today_str, "%Y-%m-%d").date()

    active = ['Open', 'In-Progress', 'Pending']
    active_df = df.loc[df['Status'].isin(active)]
    total_active_request_count = active_df.shape[0]

    active_request_count_by_request_type = active_df.groupby('RequestType').size()
    active_request_count_by_owner = active_df.groupby('Owner').size()

    new_request_count = df.loc[df['CreatedDate'].dt.date == today_utc_date_obj].shape[0]

    closed_df = df.loc[df['ClosedDate'].dt.date == today_utc_date_obj].copy()
    total_records_closed_today = closed_df.shape[0]

    closed_df["DaysToClose"] = (closed_df["ClosedDate"] - closed_df["CreatedDate"]).dt.days
    median_days_to_close_today = closed_df['DaysToClose'].median() if total_records_closed_today > 0 else 0


    seven_days_ago_utc = dt.datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) - dt.timedelta(days=7)
    recent_requests_df = df.loc[df['CreatedDate'] >= seven_days_ago_utc]
    source_channel_distribution = recent_requests_df['RequestSource'].value_counts(normalize=True).head(5)

    action_taken_distribution_today = closed_df['ActionTaken'].value_counts()
    
    snapshot_metrics = {
        "analysis_date": today_str,
        "council_district": partition_val,
        "total_active_request_count": total_active_request_count,
        "new_request_count_today": new_request_count,
        "total_records_closed_today": total_records_closed_today,
        "median_days_to_close_today": median_days_to_close_today,
        
        "active_count_by_type": active_request_count_by_request_type.to_dict(),
        "active_count_by_owner": active_request_count_by_owner.to_dict(),
        "action_taken_distribution_today": action_taken_distribution_today.to_dict(),
        "source_channel_distribution_7d": source_channel_distribution.to_dict(),
    }
    return snapshot_metrics
