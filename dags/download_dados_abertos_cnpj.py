"""
Realiza o processo completo de web scrapping da pagina de links de
CNPJs da receita federal e faz o download dos arquivos que serão
tratados e armazenados em outras DAGs
"""

import logging
import os
import re
from datetime import datetime, timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from bs4 import BeautifulSoup

from dados_abertos_constants import DS_DADOS_ABERTOS_CNPJ, DATA_OUTPUT_DIR

# URL raiz de onde ficam as pastas com que contém os links para downloads
ROOT_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# Tamanho do bloco de arquivos para realizar o download (1MB)
CHUNK_SIZE = 1048576


@task()
def create_output_dir():
    """Creates the output directory if it doesn't exist."""
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)


@task(retries=3,
      retry_delay=timedelta(seconds=1))
def get_latest_url():
    """Fetches the latest URL for downloading data."""

    try:
        response = requests.get(ROOT_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch ROOT_URL: {e}")
        raise

    soup = BeautifulSoup(response.content, 'html.parser')

    # Regex para extrair ano e mês do texto do link
    date_pattern = re.compile(r"(\d{4})-(\d{2})")

    current_latest_date = None
    latest_url = None

    # Itera sobre todos os links encontrados na página
    for link in soup.find_all('a', href=True):
        href = link['href']

        match = date_pattern.search(href)
        if match:
            year, month = match.groups()
            date = datetime(int(year), int(month), 1)

            # Atualiza para o link mais recente
            if current_latest_date is None or date > current_latest_date:
                current_latest_date = date
                latest_url = ROOT_URL + href

    return latest_url


@task()
def filter_links_to_download(latest_url: str):
    """Filters the links to download based on predefined entities."""

    # Define as entidades que devem ser baixadas pois
    # nem todas que estão disponiveis precisam ser
    entities_to_download = ["cnaes", "motivos", "estabelecimentos"]

    try:
        response = requests.get(latest_url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch latest_url: {e}")
        raise

    soup = BeautifulSoup(response.content, 'html.parser')

    links = [link['href'] for link in soup.find_all('a', href=True)]

    filtered_links = [f"{latest_url}{link}" for link in links if link.endswith('.zip') and
                      any(entity in link.lower() for entity in entities_to_download)]

    return filtered_links


@task(retries=3,
      retry_delay=timedelta(seconds=3))
def download_file(link):
    """Baixa um arquivo do link fornecido com suporte para tentativas, timeout e download em partes."""
    file_name = link.split("/")[-1]
    file_path = os.path.join(DATA_OUTPUT_DIR, file_name)

    logging.info(f"Preparing to download {file_name} to {file_path}...")

    # Realiza a requisição inicial
    with requests.get(link, stream=True, timeout=60) as file_response:
        file_response.raise_for_status()

        total_size = int(file_response.headers.get('content-length', 0))  # Tamanho total do arquivo
        downloaded_size = 0

        # Abre o arquivo local para gravar o conteúdo baixado
        with open(file_path, 'wb') as file:
            # Baixa em blocos de chunk_size e escreve no arquivo
            for chunk in file_response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:  # Filtro para ignorar chunks vazios
                    file.write(chunk)
                    downloaded_size += len(chunk)

                    # Calcula o percentual baixado
                    percent_downloaded = (downloaded_size / total_size) * 100
                    logging.info(f"\rDownloading {file_path}: {percent_downloaded:.2f}% complete")

        logging.info(f"\n{file_path} downloaded successfully!")
        return  # Sucesso no download, sai da função


@task(outlets=[DS_DADOS_ABERTOS_CNPJ])
def dummy_task() -> None:
    """
    Utilizado para prevenir que o Dataset seja atualizado
    sem que todos os arquivos tenham sido corretamente baixados
    """
    pass


@dag(
    schedule_interval=None,  # sera executado manualmente apenas
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(hours=5),
    doc_md=__doc__,
    tags=['dados_abertos_cnpjs', 'manual'],
    concurrency=1
)
def download_dados_abertos_cnpj():
    latest_url = get_latest_url()
    create_output_dir() >> latest_url
    filtered_links = filter_links_to_download(latest_url=latest_url)

    download_file.expand(link=filtered_links) >> dummy_task()


dag = download_dados_abertos_cnpj()
