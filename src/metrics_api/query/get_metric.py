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



handler = Mangum(app, lifespan="off")






# 