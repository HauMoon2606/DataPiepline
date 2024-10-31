import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/dags/Scripts")


from extract_load import extract_load
from transform_data import transform_data
from convert_to_delta import delta_convert
from download_data import download_file_by_month_year

default_args ={
    "owner":"HauMoon",
    "email_on_failure": False,
    "email_on_retry" :False,
    "email":"admin@localhost.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESSKEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

path_data = os.path.abspath(os.path.join(os.path.dirname(__file__),"../../Data/"))

with DAG(
    "etl_pipeline",
    start_date=datetime(2023,1,1),
    end_date=datetime(2023,12,1),
    schedule_interval="@monthly",
    default_args=default_args) as dag:

    download_yellow_data = PythonOperator(
        task_id = "download_yellow_data",
        python_callable=download_file_by_month_year,
        op_kwargs={
            "type":"yellow",
            "year":"{{execution_date.year}}",
            "month":"{{execution_date.strftime('%m')}}"
        }
    )

    download_green_data = PythonOperator(
        task_id = "download_green_data",
        python_callable=download_file_by_month_year,
        op_kwargs={
            "type":"green",
            "year":"{{execution_date.year}}",
            "month":"{{execution_date.strftime('%m')}}"
        }
    )

    extract_load = PythonOperator(
        task_id = "extract_load",
        python_callable= extract_load,
        op_kwargs={
            "endpoint_url":MINIO_ENDPOINT,
            "access_key":MINIO_ACCESSKEY,
            "secret_key": MINIO_SECRET_KEY,
            "year":"{{execution_date.year}}",
            "month":"{{execution_date.strftime('%m')}}"
        }
    )

    transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable=transform_data,
        op_kwargs={
            'endpoint_url':MINIO_ENDPOINT,
            "access_key":MINIO_ACCESSKEY,
            "secret_key":MINIO_SECRET_KEY,
            "year":"{{execution_date.year}}",
            "month":"{{execution_date.strftime('%m')}}"
        }
    )
    
    delta_convert = PythonOperator(
        task_id = "delta_convert",
        python_callable= delta_convert,
        op_kwargs={
            "endpoint_url":MINIO_ENDPOINT,
            "access_key":MINIO_ACCESSKEY,
            "secret_key":MINIO_SECRET_KEY,
            "year":"{{execution_date.year}}",
            "month":"{{execution_date.strftime('%m')}}"
        }
    )

    download_yellow_data >> extract_load
    download_green_data >> extract_load

    extract_load >> transform_data >> delta_convert