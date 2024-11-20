"""
Realiza o processo de descompactacao do arquivos ZIP com dados
abertos do Ministerio da Fazendo que foram baixados pelo processo download_dados_abertos
"""
import logging
import os
import re
import zipfile
from datetime import datetime
from datetime import timedelta

from airflow.datasets import Dataset
from datasets import DS_DADOS_ABERTOS_CNPJ

import pendulum
from airflow.decorators import dag, task

# URL raiz de onde ficam as pastas com que contém os links para downloads
ROOT_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# Diretório onde os arquivos serão salvos
DATA_OUTPUT_DIR = "/opt/airflow/data"


@task()
def extract_zip_files():
    # Percorre todos os arquivos no diretório
    for file_name in os.listdir(DATA_OUTPUT_DIR):
        logging.info(f"Extracing file {file_name}")
        if file_name.endswith(".zip"):
            # Caminho completo do arquivo .zip
            zip_file_path = os.path.join(DATA_OUTPUT_DIR, file_name)

            # Usa regex para remover números e extensão do nome do arquivo
            base_name = re.sub(r'\d*\.zip$', '', file_name)
            output_folder = os.path.join(DATA_OUTPUT_DIR, base_name)

            # Cria o diretório de saída se ele não existir
            os.makedirs(output_folder, exist_ok=True)

            # Descompacta o arquivo .zip para o diretório de saída
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                print(f"Unziping {file_name} to {output_folder}...")
                zip_ref.extractall(output_folder)
                print(f"{file_name} unziped successfully!")


@dag(
    schedule=[DS_DADOS_ABERTOS_CNPJ],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    doc_md=__doc__,
    tags=['dados_abertos_cnpjs', 'dataset'],
    default_args={
        'email': [''],
    }
)
def transform_dados_abertos_cnpj():
    extract_zip_files()


dag = transform_dados_abertos_cnpj()
