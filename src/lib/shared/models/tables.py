import os
from typing import Optional, Set
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute

import uuid
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from pynamodb.exceptions import UpdateError, GetError

from shared.utils.logging.logger import setup_logger

JOB_BATCH_SK = "STATIC"
logger = setup_logger(__name__)
class OpenDataTable(Model):
    class Meta:
        table_name = 'FSOpenDataTable' 
        region = region = os.environ.get('AWS_REGION', 'us-east-1')
    
    PK = UnicodeAttribute(hash_key=True) 
    SK = UnicodeAttribute(range_key=True) 
    
    entity_type = UnicodeAttribute(attr_name='EntityType', default='ITEM')

class JobBatch(OpenDataTable):
    def __init__(self, **kwargs):
        kwargs.setdefault('entity_type', 'JOBBATCH')
        super().__init__(**kwargs)   
    
    completed = NumberAttribute(default=0, attr_name="completed")
    expected = NumberAttribute(attr_name="expected")
    processed_message_ids = UnicodeSetAttribute(default=set)

    @staticmethod
    def construct_pk(job_type):
        return f"JOBS#{job_type}" 
    
    @staticmethod
    def construct_sk():
        return JOB_BATCH_SK
    
    @classmethod
    def create(
        cls,
        job_type: str,
        expected: int,
        completed: int = 0,
        processed_message_ids: Optional[Set[str]] = None,
        **kwargs
    ) -> 'JobBatch':
        return cls(
            PK=cls.construct_pk(job_type),
            SK=cls.construct_sk(),
            entity_type='JOBBATCH',
            expected=expected,
            completed=completed,
            processed_message_ids=processed_message_ids or set(),
            **kwargs
        )
    
    @classmethod
    def get_batch(cls, job_type)-> 'JobBatch':
        try:
            return cls.get(
                cls.construct_pk(job_type),
                cls.construct_sk()
            )
        except GetError:
            print("User not found")
            raise
    
    @classmethod
    def increment_completed(cls, job_type: str, message_id: str) -> bool:
        try:
            cls(
                PK=cls.construct_pk(job_type),
                SK=cls.construct_sk()
            ).update(actions=[
                cls.completed.add(1),
                cls.processed_message_ids.add({message_id})
            ])
            return True
        except UpdateError as e:
            if e.cause_response_code == "ConditionalCheckFailedException":
                logger.info(f"Message {message_id} already processed (idempotent retry)")
                return False
            else:
                raise
        
    @classmethod
    def set_expected(cls, job_type: str, expected: int) -> None:
        cls(
            PK=cls.construct_pk(job_type),
            SK=cls.construct_sk()
        ).update(actions=[
            cls.expected.set(expected)
        ])
