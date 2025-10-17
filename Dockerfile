FROM apache/airflow:2.8.1

RUN pip install --no-cache-dir \
    "apache-airflow-providers-postgres==5.5.1" \
    "psycopg2-binary==2.9.9" \
    "pandas==2.0.3" \
    "SQLAlchemy==1.4.39" \
    "dbt-core==1.6.0" \
    "dbt-postgres==1.6.0" \
    "apache-airflow-providers-openlineage>=1.8.0"