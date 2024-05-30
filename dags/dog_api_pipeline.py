import json
import logging

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _get_dog_api():
    response = requests.get("https://dog.ceo/api/breeds/image/random")
    data = response.json()
    print(data)
    logging.info(data)
    with open("/opt/airflow/dags/dog.json", "a") as f:
        json.dump(data, f)


with DAG(
    "dog_api_pipeline",
    start_date=timezone.datetime(2024, 5, 30),
    schedule="@daily",  # cron expression you can see more information in https://contrab.guru
    tags=["TEST"],
):
    start = EmptyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )

    get_dog_api = PythonOperator(
        task_id="get_dog_api",
        python_callable=_get_dog_api,  # function name should be _<function-name> so that you can distinguish the different between task and function name
    )

    end = EmptyOperator(task_id="end")

    start >> echo_hello >> end
    start >> get_dog_api >> end
