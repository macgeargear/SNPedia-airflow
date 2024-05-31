import requests
import zipfile
import os
import pandas as pd

PHARMGKB_BASE_URL = 'https://api.pharmgkb.org/v1/download/file/data'
PRIMARY_DATA_URL = [f'{PHARMGKB_BASE_URL}/{i}.zip' for i in ['drugs', 'genes', 'variants', 'chemicals', 'phenotypes']]
RELATIONSHIPS_DATA_URL = f'{PHARMGKB_BASE_URL}/relationships.zip'

DAGS_PATH = '/opt/airflow/dags'
RELATIONSHIPS_FILE_PATH = f'{DAGS_PATH}/relationships.zip'

def download_and_extract(url: str, filepath: str):
    '''
    Download and extract the data
    '''

    response = requests.get(url)
    with open(filepath, 'wb') as f:
        f.write(response.content)

    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(f'{DAGS_PATH}/pharmgkb')

def convert_tsv_to_dataframe(filepath: str):
    df = pd.read_csv(filepath, header=0, sep='\t')

def convert_tsv_to_csv(filepath: str):
    '''
    Read the csv file
    '''
    df = pd.read_csv(filepath, sep='\t') 
    df.head()

if __name__ == '__main__':
    download_and_extract(RELATIONSHIPS_DATA_URL, RELATIONSHIPS_FILE_PATH)
    convert_tsv_to_csv(f'{DAGS_PATH}/pharmgkb/relationships.tsv')
    convert_tsv_to_dataframe(f'{DAGS_PATH}/pharmgkb/relationships.tsv')