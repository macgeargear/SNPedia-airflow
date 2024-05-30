import logging
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from bs4 import BeautifulSoup

def _get_html_content(doc_link: str) -> str:
    '''
    Get the HTML content from the given URL
    '''

    content = requests.get(doc_link)
    if content.ok:
        content_type = content.headers['Content-Type']
        if content_type == 'text/html':
            content =  content.text
        else:
            logging.warning('Content-Type is not text/html')
            content = None
    else:
        logging.warning('Failed to get the content from the URL')
        content = None

    return content


def _say_hello():
    logging.info("hello from INFO log")
    logging.debug("This is DEBUG log")


with DAG(
    "SNPedia",
    start_date=timezone.datetime(2024, 5, 30),
    schedule=None,
    tags=["TEST"],
):
    start = EmptyOperator(task_id="start")


    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,  # function name should be _<function-name> so that you can distinguish the different between task and function name
    )

    get_html_content = PythonOperator(
        task_id="get_html_content",
        python_callable=_get_html_content,
        op_args=["https://www.snpedia.com/index.php/SNPedia"],
    )

    end = EmptyOperator(task_id="end")

    start >> say_hello >> get_html_content >> end
