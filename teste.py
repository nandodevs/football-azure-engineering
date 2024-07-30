import requests
from lxml import html

def get_wikipedia_table(url):
    # Fazendo a requisição HTTP com um cabeçalho de User-Agent
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    
    # Verificando se a requisição foi bem-sucedida
    if response.status_code != 200:
        print(f"Erro ao acessar a página: {response.status_code}")
        return []

    # Parseando o conteúdo HTML
    tree = html.fromstring(response.content)

    # Usando XPath para encontrar a tabela específica
    table = tree.xpath('//*[@id="mw-content-text"]/div[1]/table[3]')

    # Extraindo as linhas da tabela
    all_table_rows = []
    if table:
        rows = table[0].xpath('.//tr')  # .//tr busca todas as linhas dentro da tabela
        all_table_rows.extend(rows)
    else:
        print("Tabela não encontrada.")

    print(f"Número de linhas encontradas: {len(all_table_rows)}")
    return all_table_rows

# Exemplo de uso
url = 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'  # Substitua pelo URL desejado
data = get_wikipedia_table(url)

# Exibindo as linhas da tabela (opcional)
for row in data:
    print([cell.text_content().strip() for cell in row.xpath('.//td')])  # Exibindo o conteúdo das células
