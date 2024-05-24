FROM bitnami/spark:latest

COPY ./requirements.txt ./spark-requirements.txt

RUN pip install -r spark-requirements.txt
