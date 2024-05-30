import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _say_hello():
    logging.info("hello")
    logging.debug("This is DEBUG log")


with DAG(
    "hello",
    start_date=timezone.datetime(2024, 5, 30),
    schedule=None,
    tags=["TEST"],
):
    start = EmptyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,  # function name should be _<function-name> so that you can distinguish the different between task and function name
    )

    end = EmptyOperator(task_id="end")

    start >> echo_hello >> end
    start >> say_hello >> end
