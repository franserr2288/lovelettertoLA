import os, json
import awswrangler as wr 
from helper import get_esri_data, construct_service_url
from shared.orchestration.layers import kick_off_processing_layer
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

        dataset_name: str = body["DATASET_NAME"]
        partition_col = body["PARTITION_COL"]
        dataset_resource_id = body["DATASET_RESOURCE_ID"]

        date = body["DATE"] if "date" in body else get_today_str()
        needs_processing, next_layer = body.get("NEEDS_PROCESSING", False), body.get("NEXT_LAYER", None)
        layer_ids = body.get("LAYERS", [])

        data_frame = get_esri_data(dataset_resource_id, layer_ids, construct_service_url(dataset_resource_id))
        path = get_ingestion_path(format, BUCKET_NAME, dataset_name, date)
        wr.s3.to_parquet(
            df=data_frame, 
            dataset=True, 
            path=path,
            mode="overwrite",
            compression="snappy",
            partition_cols=[partition_col]
        )

        if needs_processing:
            print(next_layer)
            kick_off_processing_layer(data_frame, path, partition_col, dataset_name, dataset_resource_id) 
    
    except Exception as e:
        logger.exception(f"Exception: {e}")
        raise