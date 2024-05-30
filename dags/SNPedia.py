import logging
from typing import List
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from bs4 import BeautifulSoup

SNPedia_URL = 'https://www.snpedia.com/index.php'

def checkIfNone(thingToCheck):
    return True if thingToCheck in ['N/A', 'NA', '', 'nan', "None", "null", "Null", None] else False

def _say_hello():
    logging.info("hello from INFO log")
    logging.debug("This is DEBUG log")

def _get_html_content(doc_link: str=SNPedia_URL) -> str:
    '''
    Get the HTML content from the given URL
    '''

    content = requests.get(doc_link)
    if content.ok:
        content_type = content.headers['Content-Type']
        logging.info('Content-Type: %s', content_type)
        if content_type == 'text/html; charset=UTF-8':
            content =  content.text
        else:
            logging.warning('Content-Type is not text/html')
            content = None
    else:
        logging.warning('Failed to get the content from the URL')
        content = None

    return content

def _get_rs_links(disease: str) -> List[str]:
    '''
    Get the list of rs links from the SNPedia/disease page
    '''
    
    doc_link = f'{SNPedia_URL}/{disease}'
    logging.info('get data from doc_link: %s', doc_link)
    content = _get_html_content(doc_link)
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

def _get_data_from_rs_link(rs_link: str) -> dict:
    '''
    Get the content from the rs link
    'sortable smwtable jquery-tablesorter' is the class name of the Geno Mag Summary table
    '''
    valid_rs_link = rs_link[0].upper() + rs_link[1:]
    content = _get_html_content(f'{SNPedia_URL}/{valid_rs_link}')
    if not content:
        return None
    
    soup = BeautifulSoup(content, 'html.parser')

    tables =  soup.find_all('table')
    data = {'geno_mag_summary': []}

    for table in tables:
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if not cells:
                continue
            print(f'len cells: {len(cells)}')

            if len(cells) == 2:
                first_col = cells[0].text.strip()
                second_col = cells[1].text.strip()
                if checkIfNone(first_col):
                    first_col = None
                if checkIfNone(second_col):
                    second_col = None
                data[first_col] = second_col
        
            data[first_col] = second_col
        
    geno_mag_summary_table =  soup.find('table', {'class':'smwtable'})
    for row in geno_mag_summary_table.find_all('tr'):
        cells = row.find_all('td')
        if not cells:
            continue

        geno = cells[0].text.strip()
        mag = cells[1].text.strip()
        summary = cells[2].text.strip()

        data['geno_mag_summary'].append({'geno': geno, 'mag': mag, 'summary': summary})
        
    logging.info(f'data from rs link {rs_link}: {data}')
    return data

def _convert_json_to_monogodb(json_data: dict):
    '''
    Convert the json data to mongodb
    '''
    pass


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
        op_kwargs={"doc_link": SNPedia_URL},
    )

    get_rs_links = PythonOperator(
        task_id="get_rs_links",
        python_callable=_get_rs_links,
        op_kwargs={"disease": "Stroke"},
    )

    get_rs_data = PythonOperator(
        task_id="get_rs_data",
        python_callable=_get_data_from_rs_link,
        op_kwargs={"rs_link": "rs17696736"},
    )

    end = EmptyOperator(task_id="end")

    start >> get_rs_links >> end
