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

@app.get("/events/{event_id}")
async def get_single_event(
    event_id: str,
    date: str = Query(..., pattern=r'^\d{4}-\d{2}-\d{2}$'),
    event_type: str = Query(...)
):
    key = f"events/{date}/{event_type}/{event_id}"
    
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return {'item': json.loads(response['Body'].read())}
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="Not found")


@app.post("/events/batch")
async def get_events_batch(keys: List[str]):
    if len(keys) > 100:
        raise HTTPException(status_code=400, detail="Max 100 keys")
    
    events = []
    errors = []
    
    for key in keys:
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            events.append({
                'key': key,
                'data': json.loads(response['Body'].read())
            })
        except Exception as e:
            errors.append({'key': key, 'error': str(e)})
    
    return {'events': events, 'errors': errors}


@app.get("/events")
async def list_events(
    event_type: str,
    date: str = Query(..., pattern=r'^\d{4}-\d{2}-\d{2}$'),
    limit: int = Query(100, le=1000),
    continuation_token: Optional[str] = None
):
    prefix = f"events/{date}/"
    if event_type:
        prefix += f"{event_type}/"
    
    s3_params = {
        'Bucket': BUCKET_NAME,
        'Prefix': prefix,
        'MaxKeys': limit
    }
    if continuation_token:
        s3_params['ContinuationToken'] = continuation_token
    
    response = s3.list_objects_v2(**s3_params)
    
    events = [{
        'key': obj['Key'],
        'size': obj['Size'],
    } for obj in response.get('Contents', [])]
    
    return {
        'events': events,
        'next_token': response.get('NextContinuationToken'),
        'is_truncated': response.get('IsTruncated', False)
    }

handler = Mangum(app, lifespan="off")