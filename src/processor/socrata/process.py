import boto3
import requests, os, json
import awswrangler as wr 
import pandas as pd
import traceback
import datetime as dt


def handler(event, context):
    try:
        bucket_name = os.environ["BUCKET_NAME"]
        # batch size 1
        body = json.loads(event["Records"][0]["body"])
        format = body["FORMAT"]
        dataset_name: str = body["DATASET_NAME"]
        partition_val = body["PARTITION_VALUE"]
        partition_col = body["PARTITION_COL"]

        if format != "PARQUET" or dataset_name != "City311":
            raise ValueError

        path = body["PATH"]
        df = wr.s3.read_parquet(path=path)
        
        today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        output_path = f"{bucket_name}/{dataset_name}/analysis_date={today_str}/{partition_col}={partition_val}/"
    
    except Exception as e:
        print(e)        
        raise

def run_analysis(dataset_name):
    if dataset_name == "City311":    
        run_city_311_analysis()
    else:
        raise ValueError(f"Unsupported dataset: {dataset_name}")

def run_city_311_analysis():
    pass

    