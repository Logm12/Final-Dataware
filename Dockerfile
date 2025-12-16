# ./Dockerfile
ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.0
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow

# Chỉ cài 2 gói này
RUN pip install --no-cache-dir pandas sqlalchemy

USER root