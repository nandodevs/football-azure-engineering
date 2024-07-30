import requests
from lxml import html
import json
import pandas as pd

def get_wikipedia_page(url):
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
    return text.strip()  # Limpa o texto removendo espaços em branco

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
        data_df = pd.DataFrame(data)
        data_df.to_csv("data/output.csv", index=False)
        return data


        # json_rows = json.dumps(data)
        # kwargs['ti'].xcom_push(key='rows', value=json_rows)

    #return "OK"

