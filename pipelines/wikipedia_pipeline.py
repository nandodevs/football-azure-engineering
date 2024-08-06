from geopy.geocoders import Nominatim
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import requests
from lxml import html
import json
import pandas as pd
import os


# Variavéis de Conexão Azure
storage_account_name = "dataengfootballproject" 
container_name = "footballdataeng"  
account_key = os.getenv('AZURE_ACCOUNT_KEY')  

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'

def get_wikipedia_page(url): # 
    print("Getting wikipedia page...", url)
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def get_wikipedia_data(html_content):
    # Usando lxml para fazer o parse do HTML
    tree = html.fromstring(html_content)
    # Usando XPath para encontrar a tabela desejada
    table = tree.xpath('//*[@id="mw-content-text"]/div[1]/table[3]')  # Ajuste o índice se necessário

    all_table_rows = []
    if table:
        rows = table[0].xpath('.//tr')  # Busca todas as linhas dentro da tabela
        all_table_rows.extend(rows)
    else:
        print("Tabela não encontrada.")

    return all_table_rows

def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')

def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html_content = get_wikipedia_page(url)
    if html_content:  # Verifica se o HTML foi obtido com sucesso
        rows = get_wikipedia_data(html_content)

        data = []

        for i in range(1, len(rows)):  # Começa a partir de 1 para ignorar o cabeçalho
            tds = rows[i].xpath('.//td')  # Busca todas as células da linha
            if len(tds) < 7:  # Verifica se há células suficientes
                continue
            
            values = {
                'rank': i,
                'stadium': clean_text(tds[0].text_content()),
                'capacity': clean_text(tds[1].text_content()).replace(',', '').replace('.', ''),
                'region': clean_text(tds[2].text_content()),
                'country': clean_text(tds[3].text_content()),
                'city': clean_text(tds[4].text_content()),
                'images': 'https://' + tds[5].xpath('.//img/@src')[0].split("//")[1] if tds[5].xpath('.//img/@src') else "NO_IMAGE",
                'home_team': clean_text(tds[6].text_content()),
            }
            data.append(values)

        json_rows = json.dumps(data)    
        kwargs['ti'].xcom_push(key='rows', value=json_rows)

    # Code OK in Here  
        # data_df = pd.DataFrame(data)
        # data_df.to_csv("data/output.csv", index=False)
        return "OK"

def get_lat_long(country, city):
    geolocator = Nominatim(user_agent="Mozilla/5.0")
    location = geolocator.geocode(f'{city}, {country}')

    if location:
        return location.latitude, location.longitude

    return None

def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1) # Replaced line

    #Function for no Image
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE) 
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    #duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"

def write_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('data/' + 'stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    # Salvar o DataFrame em um buffer em memória
    csv_buffer = data.to_csv(index=False)

    # Cria o BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net/", credential=account_key)
    container_client = blob_service_client.get_container_client(container_name)

    # Carrega o arquivo para o container
    blob_client = container_client.get_blob_client(file_name)
    blob_client.upload_blob(csv_buffer, blob_type="BlockBlob")

    print(f"Arquivo {file_name} enviado para o Azure Data Lake Gen2.")