import os, json
import awswrangler as wr 
import pandas as pd
import traceback
import datetime as dt
from datetime import timezone
from shared.messages.sqs import get_sqs_client_and_url
from shared.utils.logging.logger import setup_logger
from shared.utils.paths.data_paths import get_dated_snapshot_root_path, get_partition_snapshot_json_file_path, get_partition_snapshot_path
from shared.utils.time.time_utils import get_today_str
# TODO: geospatial analysis with the location data they give
logger = setup_logger(__name__)
BUCKET_NAME = os.environ["BUCKET_NAME"]


def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    try:
        bucket_name = os.environ["BUCKET_NAME"]
        
        body = json.loads(event["Records"][0]["body"])
        format = body["FORMAT"]
        dataset_name: str = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
                
        if format != "PARQUET" or dataset_name != "City311":
            raise ValueError("Unsupported format or dataset.")
        
        ingestion_path = body["INGESTION_PATH"]
        partition_val = body["PARTITION_VALUE"]

        df = wr.s3.read_parquet(path=ingestion_path)
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
            path=get_partition_snapshot_path(bucket_name, dataset_name, today_str, partition_col, partition_val),
            dataset=True,
            mode="overwrite",
        )
        wr.s3.to_json(
            df=metrics_df,
            path=get_partition_snapshot_json_file_path(bucket_name, dataset_name, today_str, partition_col, partition_val),
        )

        job_type = body["JOB_TYPE"]
        sqs_client, orchestrator_queue_url = get_client_and_url_for_orchestrator_queue()
        sqs_client.send_message(
            QueueUrl=orchestrator_queue_url,
            MessageBody=json.dumps({
                "JOB_TYPE": job_type
            }),
        )  


    except Exception as e:
        print(f"An error occurred in handler: {e}")
        print(traceback.format_exc())
        
def get_client_and_url_for_orchestrator_queue():
    sqs_client, queue_url = get_sqs_client_and_url(queue_name="SocrataProcessingOrchestrator")
    return sqs_client, queue_url

def run_analysis(dataset_name, df, partition_val, today_str):
    if dataset_name == "City311":    
        return run_city_311_analysis(df, partition_val, today_str)
    else:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

def run_city_311_analysis(df, partition_val, today_str):
    
    date_cols = ['createddate', 'updateddate', 'closeddate', 'servicedate']
    for col in date_cols:
        if col in df.columns and (df[col].dtype == 'object' or df[col].dtype=="string"):
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
    
    today_utc_date_obj = dt.datetime.strptime(today_str, "%Y-%m-%d").date()

    active = ['open', 'In-Progress', 'pending']
    active_df = df.loc[df['status'].isin(active)]
    total_active_request_count = active_df.shape[0]

    active_request_count_by_request_type = active_df.groupby('requesttype').size()
    active_request_count_by_owner = active_df.groupby('owner').size()

    new_request_count = df.loc[df['createddate'].dt.date == today_utc_date_obj].shape[0]

    closed_df = df.loc[df['closeddate'].dt.date == today_utc_date_obj].copy()
    total_records_closed_today = closed_df.shape[0]

    closed_df["daystoclose"] = (closed_df["closeddate"] - closed_df["createddate"]).dt.days
    median_days_to_close_today = closed_df['daystoclose'].median() if total_records_closed_today > 0 else 0


    seven_days_ago_utc = dt.datetime.strptime(today_str, "%Y-%m-%d").replace(tzinfo=timezone.utc) - dt.timedelta(days=7)
    recent_requests_df = df.loc[df['createddate'] >= seven_days_ago_utc]
    source_channel_distribution = recent_requests_df['requestsource'].value_counts(normalize=True).head(5)

    action_taken_distribution_today = closed_df['actiontaken'].value_counts()
    
    snapshot_metrics = {
        "analysis_date": today_str,
        "council_district": partition_val,
        "total_active_request_count": total_active_request_count,
        "new_request_count_today": new_request_count,
        "total_records_closed_today": total_records_closed_today,
        "median_days_to_close_today": median_days_to_close_today,
        
        "active_count_by_type": active_request_count_by_request_type.to_dict() if active_request_count_by_request_type.size > 0 else {"is_empty":True},
        "active_count_by_owner": active_request_count_by_owner.to_dict() if active_request_count_by_owner.size > 0 else {"is_empty":True},
        "action_taken_distribution_today": action_taken_distribution_today.to_dict() if action_taken_distribution_today.size > 0 else {"is_empty":True},
        "source_channel_distribution_7d": source_channel_distribution.to_dict() if source_channel_distribution.size > 0 else {"is_empty":True},
    }
    return snapshot_metrics
