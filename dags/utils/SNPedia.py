import logging
import requests
import re

from bs4 import BeautifulSoup
from datetime import datetime

SNPEDIA_BASE_URL = 'http://www.snpedia.com/index.php'
DISEASE_LIST = ['Type_2_diabetes', 'Heart_disease', 'Stroke', 'High_blood_pressure', 'Chronic_kidney_disease']

def checkIfNone(thingToCheck):
    return True if thingToCheck in ['N/A', 'NA', '', 'nan', "None", "null", "Null", None] else False

def get_html_content(doc_link: str=SNPEDIA_BASE_URL) -> str:
    '''
    Get the HTML content from the given URL
    '''

    content = requests.get(doc_link)
    print(f'getting content from {doc_link}')
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

def get_data_from_rs_link(rs_link: str, affecting_disease: str) -> dict:
    '''
    Get the data from the rs link
    '''

    valid_rs_link = rs_link[0].upper() + rs_link[1:]
    content = get_html_content(f'{SNPEDIA_BASE_URL}/{valid_rs_link}')
    if not content:
        return None
    
    soup = BeautifulSoup(content, 'html.parser')

    tables =  soup.find_all('table')
    data = {'id': rs_link, 
            'affecting_disease': affecting_disease,
            'GenoMagSummary': [], 
            'relatedPublications': [], 
            }

    for table in tables:
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if not cells or len(cells) != 2:
                continue

            first_col = cells[0].text.strip()
            if not (first_col == 'Reference' or 
                    first_col == 'Chromosome' or 
                    first_col == 'Position' or 
                    first_col == 'Gene' or 
                    first_col == 'pharmgkb' or
                    first_col == 'Gnomad' or 
                    first_col == 'is a'):
                continue
            second_col = cells[1].text.strip()
            if checkIfNone(first_col):
                first_col = None
            if checkIfNone(second_col):
                second_col = None
            data[first_col] = second_col
        
    geno_mag_summary_table =  soup.find('table', {'class':'smwtable'})
    if not geno_mag_summary_table:
        return data
    
    for row in geno_mag_summary_table.find_all('tr'):
        cells = row.find_all('td')
        if not cells:
            continue

        geno = cells[0].text.strip()
        mag = cells[1].text.strip()
        summary = cells[2].text.strip()

        data['GenoMagSummary'].append({'Geno': geno[1:-1], 'Mag': mag, 'Summary': summary})

    data['relatedPublications'] += extract_related_publications(soup.find_all('p')) + extract_related_publications(soup.find_all('li'))

    logging.info(f'data from rs link {rs_link}: {data}')
    return data

def extract_pmid_and_description(text):
    '''
    Extracts the PMID (PubMed ID) and description from the given text.
    '''

    PMID_PATTERN = r'\[PMID (\d+)\]'
    DESCRIPTION_PATTERN = r'(.*?)\[PMID \d+\](.*)'

    pmid_match = re.search(PMID_PATTERN, text)
    pmid = pmid_match.group(1) if pmid_match else None

    description_match = re.search(DESCRIPTION_PATTERN, text)
    if description_match:
        description = (description_match.group(1) + description_match.group(2)).strip()
    else:
        description = text.strip()  # If no PMID is found, return the entire text as description

    return pmid, description

def extract_related_publications(contents): # HTML contents
    '''
    Extracts the related publications from the given contents.
    '''

    related_publications = []
    for content in contents:
        if 'PMID' not in content.text:
            continue
        pmid, description = extract_pmid_and_description(content.text)
        related_publications.append({
            'PMID': pmid,
            'description': description
        })
    print(related_publications)
    return related_publications