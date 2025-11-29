from datetime import datetime
import os
from typing import  Optional
import boto3
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum

from lib.shared.storage.s3 import read_json_from_s3
from lib.shared.utils.paths.data_paths import get_partition_snapshot_path, get_dated_aggregate_snapshot_path
from lib.shared.utils.time.time_utils import get_today_str
from metrics_api.config.config import DATASETS

app = FastAPI()
s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET_NAME"]

@app.get("/datasets")
def list_datasets():
    return {
        "datasets": [
            {
                "name": k,
                "partitioned_by": v["partition_by"],
                "description": v["description"]
            }
            for k, v in DATASETS.items()
        ]
    }

@app.get("/datasets/{dataset_name}/snapshots/{partition_value}")
def get_partition_snapshot(
    dataset_name: str,
    partition_value: str,
    date: Optional[str] = Query(None, description="Analysis date (YYYY-MM-DD), defaults to today")
):
    if dataset_name not in DATASETS:
        raise HTTPException(404, f"Dataset '{dataset_name}' not found")
    
    today_str = get_today_str()
    partition_col = DATASETS[dataset_name]["partition_by"]

    path = get_partition_snapshot_path()
    data = read_json_from_s3(BUCKET_NAME, path)
    
    return {
        "dataset": dataset_name,
        "date": today_str,
        "data": data,
        "partition": {
            "type": partition_col,
            "value": partition_value
        },
    }

@app.get("/datasets/{dataset_name}/summary")
def get_aggregate_snapshot(
    dataset_name: str,
    date: Optional[str] = Query(None, description="Analysis date (YYYY-MM-DD), defaults to today")
):
    if dataset_name not in DATASETS:
        raise HTTPException(404, f"Dataset '{dataset_name}' not found")
    if not date:
        date = get_today_str()
    path = get_dated_aggregate_snapshot_path()
    data = read_json_from_s3(BUCKET_NAME, path)

    
    return {
        "dataset": dataset_name,
        "date": date,
        "data": data,
        "description": "Aggregated summary across all partitions"
    }


handler = Mangum(app, lifespan="off")






# 