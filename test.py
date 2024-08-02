import os
from azure.storage.filedatalake import DataLakeServiceClient

# Defina suas variáveis
# Defina suas variáveis
storage_account_name = "dataengfootballproject"  # Substitua pelo nome da sua conta de armazenamento
container_name = "footballdataeng"  # Substitua pelo nome do seu contêiner
access_token = "JrM6mOv9o47VcBx1x/yWWvBHq1qfZFp6EeDqNgjPGR5ywl34XKLaeCrePzOJt3DS2TWVW9ynoszJ+AStp1bSBg=="  # Substitua pelo seu token de acesso

# Crie a URL da conta de armazenamento
account_url = f"https://{storage_account_name}.dfs.core.windows.net"

# Crie um cliente de serviço do Data Lake
try:
    # Use o token de acesso para autenticação
    service_client = DataLakeServiceClient(account_url=account_url, credential=access_token)

    # Liste os sistemas de arquivos (contêineres)
    file_systems = service_client.list_file_systems()
    
    print("Lista de Contêineres:")
    for fs in file_systems:
        print(f" - {fs.name}")

    # Acesse um contêiner específico
    file_system_client = service_client.get_file_system_client(file_system=container_name)

    # Liste os arquivos e diretórios no contêiner
    paths = file_system_client.get_paths()
    
    print(f"\nArquivos e Diretórios em '{container_name}':")
    for path in paths:
        print(f" - {path.name}")

except Exception as e:
    print(f"Ocorreu um erro: {e}")
