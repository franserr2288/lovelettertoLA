FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY ingest.py ${LAMBDA_TASK_ROOT}
COPY helper.py ${LAMBDA_TASK_ROOT}

CMD ["ingest.handler"]
