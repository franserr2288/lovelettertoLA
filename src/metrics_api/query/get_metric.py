import os
from typing import  Optional
import boto3
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum

from lib.shared.storage.s3 import find_and_read_snapshots, read_json_from_s3
from lib.shared.utils.paths.data_paths import (
    get_dated_aggregate_snapshot_json_file_path, 
    get_dated_snapshot_root_path, 
    get_partition_snapshot_json_file_path
)
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
    
    if not date:
        date = get_today_str()

    partition_col = DATASETS[dataset_name]["partition_by"]

    if not partition_value:
        path  = get_dated_snapshot_root_path(BUCKET_NAME, dataset_name, date)
        data = find_and_read_snapshots(BUCKET_NAME, path)

    else:
        path = get_partition_snapshot_json_file_path(BUCKET_NAME, dataset_name, date, partition_col, partition_value)
        data = read_json_from_s3(BUCKET_NAME, path)

    return {
        "dataset": dataset_name,
        "date": date,
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

    path = get_dated_aggregate_snapshot_json_file_path(BUCKET_NAME, dataset_name, date)
    data = read_json_from_s3(BUCKET_NAME, path)

    
    return {
        "dataset": dataset_name,
        "date": date,
        "data": data,
        "description": "Aggregated summary across all partitions"
    }


handler = Mangum(app, lifespan="off")






# 