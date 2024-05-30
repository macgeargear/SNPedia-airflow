# SNPedia-airflow

this repo is for learning how data pipeline works using apache airflow

**Setup airflow**
download `docker-compose.yaml` file from this command
```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
```

**Import the librarys**

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
```

**define DAG(direct acyclic graph)**

```python
start = EmptyOperator(task_id="start") // Just for testing
say_hello = PythonOperator(
  task_id="say_hello",
  python_callable=_say_hello)

get_rs_links = PythonOperator(
  task_id="get_rs_links",
  python_callable=_get_rs_links,
  op_kwargs={"disease": "Stroke"})

get_all_data_from_rs_links = PythonOperator(
  task_id="get_all_data_from_rs_links",
  python_callable=_get_all_data_from_rs_links,
  op_kwargs={"rs_link": "rs17696736"})

ave_to_mongo_db = PythonOperator(
  task_id="save_to_mongo_db",
  python_callable=_svae_to_mongodb,
  op_kwargs={"jsonData": "/opt/airflow/dags/SNPedia.json"})

end = EmptyOperator(task_id="end") // Just for testing
```

**complete the DAG**
```python
start >> get_rs_links >> get_all_data_from_rs_links >> save_to_mongo_db >> end
```


