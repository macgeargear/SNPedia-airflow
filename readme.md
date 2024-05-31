# SNPedia-airflow

- this repo is for learning how data pipeline works using apache airflow

- the disease lists: ['Heart_disease', 'Stroke', 'High_blood_pressure', 'Chronic_kidney_disease']

**Setup airflow**
download `docker-compose.yaml` file from this command

```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'
```

create `Dockerfile` (for install external python libraryies )

```Dockerfile
FROM apache/airflow:latest
RUN pip install --no-cache-dir pymongo python-dotenv bs4
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
start = EmptyOperator(task_id="start")
step = EmptyOperator(task_id="step")
end = EmptyOperator(task_id="end")

get_rs_links_from_diseases = [
    PythonOperator(
        task_id=f"get_rs_links_from_{disease}",
        python_callable=_get_rs_links_from_diseases,
        op_kwargs={"disease": disease},
    ) for disease in utils.DISEASE_LIST]

get_all_data_from_diseases = [
    PythonOperator(
        task_id=f"get_all_data_from_{disease}",
        python_callable=_get_all_data_from_diseases,
        op_kwargs={'affecting_disease': disease},
    ) for disease in utils.DISEASE_LIST]

combine_data = PythonOperator(
    task_id="combine_data",
    python_callable=_combine_data,
)

save_to_mongo_db = PythonOperator(
    task_id="save_to_mongo_db",
    python_callable=_svae_to_mongodb,
    op_kwargs={"jsonData": "/opt/airflow/dags/SNPedia.json"},
)
```

**complete the DAG**

```python
start >> get_rs_links_from_diseases >> step >>  get_all_data_from_diseases >> combine_data >> save_to_mongo_db >> end
```

**the shape of data**

```ts
{
  affecting_disease: str,
  data: {
    GenoMagSummary:
      { Geno: str,
        Mag: str,
        Summary: str
      }[],
    Reference: str,
    Chromosome: str,
    Position: str,
    Gene: str,
    "isA": str,
    "RelatedPublications": {
      PMID: str,
      description: str,
    }[],
  }[],
  updatedAt: DateTime,
}[]
```
