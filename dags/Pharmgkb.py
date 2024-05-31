import logging

import utils.Pharmgkb as utils

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


with DAG(
    "Pharmgkb",
    start_date=timezone.datetime(2024, 5, 30),
    schedule=None,
    tags=["T2DM"],
):
    start = EmptyOperator(task_id="start")
    step = EmptyOperator(task_id="step")
    end = EmptyOperator(task_id="end")

    download_and_extract = PythonOperator(
        task_id="download_and_extract",
        python_callable=utils.download_and_extract,
        op_kwargs={"url": utils.RELATIONSHIPS_DATA_URL, "filepath": utils.RELATIONSHIPS_FILE_PATH},
    )

    list_files = BashOperator(
        task_id="list_files",
        bash_command="ls -l /opt/airflow/dags/",
    )

    start >> list_files >> end