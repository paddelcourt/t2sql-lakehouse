import os
import shutil
import time
from datetime import datetime, timedelta

import boto3
import duckdb

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

warehouse = "warehouse"  # Define your bucket name here


@dag(
    dag_id="ingest_dag",
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1) # Add a start_date
)
def my_dag():
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=warehouse
    )

    goodwiki_to_s3 = LocalFilesystemToS3Operator(
        task_id="goodwiki_to_s3",
        filename="/opt/airflow/data/goodwiki.parquet",
        dest_key="raw/goodwiki.parquet",
        dest_bucket=warehouse,
        replace=True,
    )

    run_ingestion_pipeline = BashOperator(
        task_id='run_ingestion_pipeline',
        bash_command="python /opt/airflow/dags/scripts/ingestion-pipeline.py",
    )

    create_s3_bucket >> goodwiki_to_s3 >> run_ingestion_pipeline

my_dag()