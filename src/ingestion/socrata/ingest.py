import os, json
import awswrangler as wr 
from ingestion.socrata.helper import get_socrata_data
from shared.orchestration.layers import kick_off_analytics_layer
from shared.utils.logging.logger import setup_logger
from shared.utils.paths.data_paths import get_ingestion_path
from shared.utils.time.time_utils import get_today_str

logger = setup_logger(__name__)
BUCKET_NAME = os.environ["BUCKET_NAME"]

def handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")
    try:
        # batch size 1
        body = json.loads(event["Records"][0]["body"])
        format = body["FORMAT"]
        if format not in ["PARQUET"]:
            raise ValueError

        dataset_name: str = body["DATASET_NAME"]
        source = body["SOURCE"]
        partition_col = body["PARTITION_COL"]
        dataset_resource_id = body["DATASET_RESOURCE_ID"]
        date = body["DATE"] if "date" in body else get_today_str()


        data_frame = get_socrata_data(source, dataset_resource_id, partition_col)
        path = get_ingestion_path(BUCKET_NAME, dataset_name, date)
        wr.s3.to_parquet(
            df=data_frame, 
            dataset=True, 
            path=path,
            mode="overwrite",
            compression="snappy",
            partition_cols=[partition_col]
        )
        kick_off_analytics_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id, "snapshot_generation") 
    
    except Exception as e:
        logger.exception(f"Exception: {e}")
        raise