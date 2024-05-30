import logging
import requests

from bs4 import BeautifulSoup
from datetime import datetime

SNPedia_URL = 'https://www.snpedia.com/index.php'

def checkIfNone(thingToCheck):
    return True if thingToCheck in ['N/A', 'NA', '', 'nan', "None", "null", "Null", None] else False

def get_html_content(doc_link: str=SNPedia_URL) -> str:
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

def get_data_from_rs_link(rs_link: str) -> dict:
    '''
    Get the data from the rs link
    '''

    valid_rs_link = rs_link[0].upper() + rs_link[1:]
    content = get_html_content(f'{SNPedia_URL}/{valid_rs_link}')
    if not content:
        return None
    
    soup = BeautifulSoup(content, 'html.parser')

    tables =  soup.find_all('table')
    data = {'GenoMagSummary': [], 'updatedAt': datetime.now().isoformat()}

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
        
    logging.info(f'data from rs link {rs_link}: {data}')
    return data