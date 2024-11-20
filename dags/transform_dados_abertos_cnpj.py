"""
Realiza o processo completo de web scrapping da pagina de links de
CNPJs da receita federal e salva os dados em um banco de dados PostgreSQL
para serem utilizados em analises
"""

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
    @task()
    def print_start():
        print('Iniciando o processo de download dos dados abertos de CNPJs')

    print_start()


dag = transform_dados_abertos_cnpj()
