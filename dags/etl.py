import glob
import logging
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


with DAG(
    "SNPedia_ETL",
    start_date=timezone.datetime(2024, 5, 30),
    schedule=None,
    tags=["TEST"],
):
    start = EmptyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )


    end = EmptyOperator(task_id="end")

    start >> end 
