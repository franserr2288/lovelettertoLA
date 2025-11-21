FROM public.ecr.aws/lambda/python:3.12

COPY ingest_city311.py requirements.txt ./

RUN pip install -r requirements.txt

CMD ["ingest_city311.handler"]
