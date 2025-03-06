"""
Realiza o processo de tratamento dos dados descompactados da receita
para um formato pronto para ser utilizado em analises (silver layer)
"""
import logging
import os
from datetime import timedelta

import pendulum
import pyspark.sql.functions as F
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from dados_abertos_constants import DS_DADOS_ABERTOS_CNPJ_UNZIPED, DATA_OUTPUT_DIR, EntitiesSynced


def list_files_to_transform(entity_name: str):
    entity_folder = f"{DATA_OUTPUT_DIR}/{entity_name}/"
    logging.info(f"Listing files to transform in {entity_folder}")

    files = os.listdir(entity_folder)
    files_to_transform = [f"{entity_folder}{f}" for f in files]
    logging.info(f"Files to tranform {len(files_to_transform)}")

    return files_to_transform


@task()
def transform_data():
    spark = SparkSession.builder \
        .appName("jira_extract_issues") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()

    try:
        do_spark_operations(spark=spark)
    finally:
        if spark is not None:
            spark.stop()


def do_spark_operations(spark: SparkSession):
    logging.info("Creating schema for estabelecimentos")
    csv_estabelecimentos_schema = StructType([
        StructField("cnpj_basico", StringType(), nullable=True),
        StructField("cnpj_ordem", StringType(), nullable=True),
        StructField("cnpj_dv", StringType(), nullable=True),
        StructField("cd_matriz_filial", IntegerType(), nullable=True),
        StructField("nome_fantasia", StringType(), nullable=True),
        StructField("cd_situacao_cadastral", StringType(), nullable=True),
        StructField("data_situacao_cadastral", StringType(), nullable=True),
        StructField("cd_motivo_situacao_cadastral", StringType(), nullable=True),
        StructField("nome_cidade_exterior", StringType(), nullable=True),
        StructField("pais", StringType(), nullable=True),
        StructField("data_inicio_atividades", StringType(), nullable=True),
        StructField("cd_cnae_fiscal", StringType(), nullable=True),
        StructField("cd_cnae_fiscal_secundario", StringType(), nullable=True),
        StructField("tipo_logradouro", StringType(), nullable=True),
        StructField("logradouro", StringType(), nullable=True),
        StructField("numero", IntegerType(), nullable=True),
        StructField("complemento", StringType(), nullable=True),
        StructField("bairro", StringType(), nullable=True),
        StructField("cep", StringType(), nullable=True),
        StructField("uf", StringType(), nullable=True),
        StructField("municipio", StringType(), nullable=True),
        StructField("ddd1", StringType(), nullable=True),
        StructField("telefone1", StringType(), nullable=True),
        StructField("ddd2", StringType(), nullable=True),
        StructField("telefone2", StringType(), nullable=True),
        StructField("ddd_fax", StringType(), nullable=True),
        StructField("fax", StringType(), nullable=True),
        StructField("correio_eletronico", StringType(), nullable=True),
        StructField("cd_situacao_especial", StringType(), nullable=True),
        StructField("data_situacao_especial", StringType(), nullable=True),
    ])
    files_estab_to_transform = list_files_to_transform(EntitiesSynced.ESTABELECIMENTOS.value)
    df_estabelecimentos = spark.read.csv(files_estab_to_transform, header=False, sep=";",
                                         schema=csv_estabelecimentos_schema)
    # logging.info(f"Estabelecimentos rows: {df_estabelecimentos.count()}")
    logging.info("Applying transformations and castings")
    df_estab_tratado = (
        df_estabelecimentos
        .withColumn("cnpj", F.concat("cnpj_basico", "cnpj_ordem", "cnpj_dv"))
        .withColumn("matriz_filial", F.when(F.col("cd_matriz_filial").eqNullSafe(1), "Matriz").otherwise("Filial"))
        .withColumn("situacao_cadastral",
                    F.when(F.col("cd_situacao_cadastral") == "01", "Nula")
                    .when(F.col("cd_situacao_cadastral") == "02", "Ativa")
                    .when(F.col("cd_situacao_cadastral") == "03", "Suspensa")
                    .when(F.col("cd_situacao_cadastral") == "04", "Inapta")
                    .otherwise("Baixada"))
        .withColumn("data_situacao_cadastral", F.to_date("data_situacao_cadastral", 'yyyyMMdd'))
        .withColumn("data_inicio_atividades", F.to_date("data_inicio_atividades", 'yyyyMMdd'))
        .withColumn("data_situacao_especial", F.to_date("data_situacao_especial", 'yyyyMMdd'))
    )
    # O schema de codigos sera utilizado para as entidades de Motivos e Cnaes
    csv_code_description_schema = StructType([
        StructField("codigo", StringType(), nullable=True),
        StructField("descricao", StringType(), nullable=True)
    ])
    files_motivos_to_transform = list_files_to_transform(entity_name=EntitiesSynced.MOTIVOS.value)
    df_motivos = spark.read.csv(files_motivos_to_transform, header=False, sep=";", schema=csv_code_description_schema)
    logging.info("Joining fields from MOTIVOS entity")
    # noinspection PyTypeChecker
    df_estab_motivos = (
        df_estab_tratado
        .join(df_motivos,
              df_estab_tratado.cd_motivo_situacao_cadastral == df_motivos.codigo, "left")
        .select(
            df_estab_tratado["*"],  # Todas as colunas do df_estab_tratado
            df_motivos["descricao"].alias("motivo_situacao_cadastral")  # Apenas a coluna "descricao" do df_motivos
        ))
    files_cnaes_to_transform = list_files_to_transform(EntitiesSynced.CNAES.value)
    df_cnaes = spark.read.csv(files_cnaes_to_transform, header=False, sep=";", schema=csv_code_description_schema,
                              encoding="ISO-8859-1")
    logging.info("Joining fields from CNAES entity")
    # noinspection PyTypeChecker
    df_estab_motivos_cnaes = (
        df_estab_motivos
        .join(df_cnaes,
              df_estab_motivos.cd_cnae_fiscal == df_cnaes.codigo, "left")
        .select(
            df_estab_motivos["*"],  # Todas as colunas do df_estab_tratado
            df_cnaes["descricao"].alias("cnae_fiscal")  # Apenas a coluna "descricao" do df_motivos
        ))
    # O Select apenas ajusta a ordem das colunas para que sejam persistidas
    # de uma maneira mais facil de visualizar, colocando as descricoes
    # ao lado dos codigos
    df_estabelecimentos_final = df_estab_motivos_cnaes.select('cnpj_basico',
                                                              'cnpj_ordem',
                                                              'cnpj_dv',
                                                              'cnpj',
                                                              'cd_matriz_filial',
                                                              'matriz_filial',
                                                              'nome_fantasia',
                                                              'cd_situacao_cadastral',
                                                              'situacao_cadastral',
                                                              'data_situacao_cadastral',
                                                              'cd_motivo_situacao_cadastral',
                                                              'motivo_situacao_cadastral',
                                                              'nome_cidade_exterior',
                                                              'pais',
                                                              'data_inicio_atividades',
                                                              'cd_cnae_fiscal',
                                                              'cnae_fiscal',
                                                              'cd_cnae_fiscal_secundario',
                                                              'tipo_logradouro',
                                                              'logradouro',
                                                              'numero',
                                                              'complemento',
                                                              'bairro',
                                                              'cep',
                                                              'uf',
                                                              'municipio',
                                                              'ddd1',
                                                              'telefone1',
                                                              'ddd2',
                                                              'telefone2',
                                                              'ddd_fax',
                                                              'fax',
                                                              'correio_eletronico',
                                                              'cd_situacao_especial',
                                                              'data_situacao_especial')
    output_data = f"{DATA_OUTPUT_DIR}/silver"
    logging.info(f"Writing parquet file to {output_data}")
    df_estabelecimentos_final.write.mode("overwrite").parquet(output_data)


@task()
def start_spark_session():
    return SparkSession.builder.appName("airflow_dados_abertos_cnpj").getOrCreate()


@dag(
    schedule=[DS_DADOS_ABERTOS_CNPJ_UNZIPED],
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    doc_md=__doc__,
    tags=['dados_abertos_cnpjs', 'dataset'],
)
def transform_to_silver_dados_abertos():
    transform_data()


dag = transform_to_silver_dados_abertos()
