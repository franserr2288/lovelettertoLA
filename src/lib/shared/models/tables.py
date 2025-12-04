import os
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute

import uuid
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute

JOB_BATCH_SK = "STATIC"

class OpenDataTable(Model):
    class Meta:
        table_name = 'FSOpenDataTable' 
        region = region = os.environ.get('AWS_REGION', 'us-east-1')
    
    PK = UnicodeAttribute(hash_key=True) 
    SK = UnicodeAttribute(range_key=True) 
    
    entity_type = UnicodeAttribute(attr_name='EntityType', default='ITEM')

class JobBatch(OpenDataTable):
    def __init__(self, job_type, **kwargs):
        kwargs['entity_type'] = 'JOBBATCH'        
        kwargs['PK'] = f"JOBS#{job_type}" 
        kwargs['SK'] = JOB_BATCH_SK # only need one record to keep track of counter
        super().__init__(**kwargs)    
    
    completed = NumberAttribute(default=0, attr_name="completed")
    expected = NumberAttribute(attr_name="expected")
    processed_message_ids = UnicodeSetAttribute(default=set)
