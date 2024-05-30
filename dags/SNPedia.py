import logging
from typing import List
import json
import os

import pymongo

import utils.SNPedia as utils

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv



dotenv_path = '/opt/airflow/.env'
load_dotenv(dotenv_path)
MONGODB_CONNECTION_STRING = os.environ.get("MONGODB_CONNECTION_STRING") or "mongodb://root:rootpassword@mongodb:27017"

# function name should be _<function-name> so that you can distinguish the different between task and function name
def _say_hello():
    logging.info("hello from INFO log")

def _get_rs_links(disease: str) -> List[str]:
    '''
    Get the list of rs links from the SNPedia/disease page
    '''
    
    doc_link = f'{utils.SNPedia_URL}/{disease}'
    logging.info('get data from doc_link: %s', doc_link)
    content = utils.get_html_content(doc_link)
    if not content:
        return None
    soup = BeautifulSoup(content, 'html.parser')
    rs_links = []
    for link in soup.find_all('a'):
        if not link.get('href') or 'Rs' not in link.get('href'):
            continue
        rs_link = link.get('href').split('/')[-1].lower()
        rs_links.append(rs_link)
    logging.info('List of rs links: %s', rs_links)
    return rs_links


def _get_all_data_from_rs_links(**context):
    '''
    Get the data from the list of rs links
    '''

    ti = context['ti']
    data = []
    links = ti.xcom_pull(task_ids='get_rs_links', key='return_value')
    for link in links:
        data.append(utils.get_data_from_rs_link(link))
    
    with open('/opt/airflow/dags/SNPedia.json', 'w') as f:
        json.dump(data, f)
    
def _svae_to_mongodb(jsonData):
    '''
    Save the data to MongoDB
    '''

    data = []
    with open(jsonData) as f:
        data = json.load(f)
    print(f'SNPedia Data Length: {len(data)}')

    if (len(data) == 0):
        print('No data to save')
        return
    
    print(MONGODB_CONNECTION_STRING)
    mongoClient = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    db = mongoClient['tumorBoard']
    SNPediaCollection = db['SNPedia']

    if (SNPediaCollection.count_documents({}) == 0):
        SNPediaCollection.insert_many(data)
        mongoClient.close()
        print('Data saved successfully')
        return

    latestDocuments = SNPediaCollection.find().sort('updatedAt', pymongo.DESCENDING).limit(1)
    lastUpdatedTime = latestDocuments[0]['updatedAt']
    SNPediaCollection.insert_many(data)
    SNPediaCollection.delete_many({ "updatedAt": { "$lte": lastUpdatedTime } })

with DAG(
    "SNPedia",
    start_date=timezone.datetime(2024, 5, 30),
    schedule=None,
    tags=["SNPedia"],
):
    start = EmptyOperator(task_id="start")

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,  
    )

    get_rs_links = PythonOperator(
        task_id="get_rs_links",
        python_callable=_get_rs_links,
        op_kwargs={"disease": "Stroke"},
    )

    get_all_data_from_rs_links = PythonOperator(
        task_id="get_all_data_from_rs_links",
        python_callable=_get_all_data_from_rs_links,
        op_kwargs={"rs_link": "rs17696736"},
    )

    save_to_mongo_db = PythonOperator(
        task_id="save_to_mongo_db",
        python_callable=_svae_to_mongodb,
        op_kwargs={"jsonData": "/opt/airflow/dags/SNPedia.json"},
    )

    end = EmptyOperator(task_id="end")

    start >> get_rs_links >> get_all_data_from_rs_links >> save_to_mongo_db >> end
