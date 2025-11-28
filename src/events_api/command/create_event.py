from datetime import datetime as dt
import datetime
import json
import os
import uuid
import boto3
from fastapi import FastAPI, HTTPException
from mangum import Mangum
from pydantic import BaseModel

app = FastAPI()
s3 = boto3.client('s3')
BUCKET_NAME = os.environ["BUCKET_NAME"]

class EventCreate(BaseModel):
    event_type: str
    # define shape later
    data: dict

@app.post("/events")
async def create_event(event: EventCreate):
    event_id = str(uuid.uuid4())
    date = dt.now(dt).strftime('%Y-%m-%d')
    key = f"events/{date}/{event.event_type}/{event_id}"
    
    event_data = {
        'id': event_id,
        'event_type': event.event_type,
        'created_at': dt.now(datetime.timezone.utc).isoformat(),
        'data': event.data
    }
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(event_data),
            ContentType='application/json'
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create event: {str(e)}")
    
    return {
        'id': event_id,
        'key': key,
        'created_at': event_data['created_at']
    }

handler = Mangum(app, lifespan="off")