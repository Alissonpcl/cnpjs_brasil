from enum import Enum

from airflow.datasets import Dataset

# Dataset que sera atualizado quando novos arquivos forem baixados
DS_DADOS_ABERTOS_CNPJ = Dataset("ds://dados_abertos_cnpj/")

# Dataset atualizado quando os arquivos baixados forem descompactados
DS_DADOS_ABERTOS_CNPJ_UNZIPED = Dataset("ds://dados_abertos_cnpj_unziped/")

# Diretório onde os arquivos serão salvos
DATA_OUTPUT_DIR = "/opt/airflow/data"


class EntitiesSynced(Enum):
    ESTABELECIMENTOS = 'Estabelecimentos'
    CNAES = 'Cnaes'
    MOTIVOS = 'Motivos'
