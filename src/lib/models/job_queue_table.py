from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection
import os

class StatusIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'status-index'
        projection = AllProjection()
    
    batch_id = UnicodeAttribute(hash_key=True)
    status = UnicodeAttribute(range_key=True)


class Job(Model):
    class Meta:
        table_name = os.environ.get('TABLE_NAME', 'job-queue')
        region = os.environ.get('AWS_REGION', 'us-east-1')
    
    
    batch_id = UnicodeAttribute(hash_key=True)
    job_id = UnicodeAttribute(range_key=True)
    
    status = UnicodeAttribute(default='pending')  # pending, processing, complete, failed
    district = UnicodeAttribute()
    error = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute(null=True)
    
    status_index = StatusIndex()