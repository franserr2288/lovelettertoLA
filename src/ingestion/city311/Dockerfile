FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY ingest_city311.py ${LAMBDA_TASK_ROOT}

CMD ["ingest_city311.handler"]
