import os, json
import awswrangler as wr 
import pandas as pd
import traceback

from shared.datasets.consts import MEDIAN_INCOME_AND_AMI
from shared.utils.logging.logger import setup_logger
from shared.utils.paths.data_paths import get_ingestion_path, get_reference_dataset_path
from shared.utils.time.time_utils import get_today_str


logger = setup_logger(__name__)
BUCKET_NAME = os.environ["BUCKET_NAME"]


def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    try:
        body = json.loads(event["Records"][0]["body"])
        dataset_name: str = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
        date = body["DATE"] if "date" in body else get_today_str()
        
        df = wr.s3.read_parquet(path=get_ingestion_path(BUCKET_NAME, dataset_name, date))

        normalized_data = normalize(
            dataset_name, 
            df, 
            date 
        )
        normalized_data["normalization_date"] = get_today_str() # can process other dates but correctly note processing date

        metrics_df = pd.DataFrame([normalized_data])
        wr.s3.to_json(
            df=metrics_df,
            path=get_reference_dataset_path(BUCKET_NAME, dataset_name),
        )
    except Exception as e:
        print(f"An error occurred in handler: {e}")
        print(traceback.format_exc())
        

def normalize(dataset_name):
    if dataset_name == MEDIAN_INCOME_AND_AMI:
        normalize_median_income()
    else:
        raise ValueError("not supported yet")

def normalize_median_income():
    pass

