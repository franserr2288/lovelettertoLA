from datetime import datetime
import json
import os
from typing import List, Optional
import boto3
from fastapi import FastAPI, HTTPException, Query
from mangum import Mangum
from pydantic import BaseModel

app = FastAPI()
s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET_NAME"]

class EventCreate(BaseModel):
    event_type: str
    data: dict

# @app.get("/metrics/{event_id}")
# async def get_single_event(
#     event_id: str,
#     date: str = Query(..., pattern=r'^\d{4}-\d{2}-\d{2}$'),
#     event_type: str = Query(...)
# ):
#     key = f"events/{date}/{event_type}/{event_id}"
    
#     try:
#         response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
#         return {'item': json.loads(response['Body'].read())}
#     except s3.exceptions.NoSuchKey:
#         raise HTTPException(status_code=404, detail="Not found")


handler = Mangum(app, lifespan="off")






# 