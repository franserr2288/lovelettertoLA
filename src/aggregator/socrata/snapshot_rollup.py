import awswrangler as wr 
import pandas as pd
import json
import os

from shared.utils.paths.data_paths import get_dated_aggregate_snapshot_json_file_path, get_dated_aggregate_snapshot_path, get_partition_snapshot_json_file_path, get_dated_snapshot_root_path
from shared.utils.time.time_utils import get_today_str

BUCKET_NAME = os.environ["BUCKET_NAME"]

def handler(event, context):    
    try:
        body = json.loads(event["Records"][0]["body"])
        dataset_name = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
        
        input_path = get_dated_snapshot_root_path(BUCKET_NAME, dataset_name, get_today_str())
        
        metrics_df = read_daily_snapshot_metrics(input_path)
        final_rollups = perform_cross_district_rollup(metrics_df, partition_col)
        
        write_final_rollups(final_rollups, dataset_name)
    
    except Exception as e:
        raise


def read_daily_snapshot_metrics(input_path):    
    df = wr.s3.read_parquet(
        path=input_path, 
        dataset=True
    )
    return df


def perform_cross_district_rollup(df: pd.DataFrame, partition_col: str) -> dict:
    city_avg_days_to_close = df['median_days_to_close_today'].mean()
    total_city_new_requests = df['new_request_count_today'].sum()
    total_city_active = df['total_active_request_count'].sum()
    
    total_city_new_requests = 1 if total_city_new_requests == 0 else total_city_new_requests

    report_df = df.copy()

    #  positive % == slower than avg, neg % means faster than average
    report_df['speed_delta_vs_avg_pct'] = (
        (report_df['median_days_to_close_today'] - city_avg_days_to_close) / city_avg_days_to_close
    ) * 100

    # % of city's total new work
    report_df['city_burden_share_pct'] = (
        (report_df['new_request_count_today'] / total_city_new_requests) * 100
    )

    # higher is better
    report_df['churn_efficiency_rate'] = report_df.apply(
        lambda x: x['total_records_closed_today'] / x['total_active_request_count'] 
        if x['total_active_request_count'] > 0 else 0, axis=1
    )
    
    # normalize speed, inverted because lower speed == better
    max_days = report_df['median_days_to_close_today'].max()
    report_df['norm_speed'] = 1 - (report_df['median_days_to_close_today'] / max_days)

    # normalize throughput
    max_closed = report_df['total_records_closed_today'].max()
    report_df['norm_throughput'] = report_df['total_records_closed_today'] / max_closed

    report_df['composite_score'] = (
        (report_df['norm_speed'] * 0.5) + 
        (report_df['norm_throughput'] * 0.5)
    ) * 100

    top_districts = report_df.sort_values(by='composite_score', ascending=False).head(5)
    bottom_districts = report_df.sort_values(by='composite_score', ascending=True).head(5)
    card_columns = [
        partition_col, 
        'composite_score', 
        'median_days_to_close_today', 
        'total_records_closed_today',
        'speed_delta_vs_avg_pct',
        'city_burden_share_pct' 
    ]

    top_5_report_card = top_districts[card_columns].to_dict(orient='records')
    bottom_5_report_card = bottom_districts[card_columns].to_dict(orient='records')
    
    return {
        "city_wide_stats": {
            "avg_days_to_close": city_avg_days_to_close,
            "total_new_requests": total_city_new_requests,
            "total_active_backlog": total_city_active
        },
        "district_comparisons": report_df[[
            partition_col, 
            'speed_delta_vs_avg_pct', 
            'city_burden_share_pct', 
            'churn_efficiency_rate',
            'composite_score'
        ]].to_dict(orient='records'),
        "top_5_report_card": top_5_report_card,
        "bottom_5_report_card": bottom_5_report_card
    }


def write_final_rollups(df: pd.DataFrame, dataset_name: str):
    bucket_name = os.environ["BUCKET_NAME"]
    today_str = get_today_str()
    
    wr.s3.to_json(
        df=df,
        path=get_dated_aggregate_snapshot_json_file_path(bucket_name, dataset_name, today_str),
    )
    wr.s3.to_parquet(
        df=df,
        path=get_dated_aggregate_snapshot_path(bucket_name, dataset_name, today_str),
        dataset=True,
        mode="append",
    )

