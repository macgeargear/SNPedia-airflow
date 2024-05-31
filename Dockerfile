FROM apache/airflow:latest
RUN pip install --no-cache-dir pymongo python-dotenv bs4 pandas