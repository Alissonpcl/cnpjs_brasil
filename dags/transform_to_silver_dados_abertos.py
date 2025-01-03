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
        .appName("cnpjs_abertos") \
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
        StructField("cd_pais", StringType(), nullable=True),
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
        StructField("cd_municipio", StringType(), nullable=True),
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
                                         schema=csv_estabelecimentos_schema,
                                         encoding="ISO-8859-1")

    # Mantém apenas estabelecimentos de atividade odontologica no CNAE
    # principal pois não precisamos das demais empresas e a quantidade
    # de registros e muito grande
    # "8630504";"Atividade odontológica"
    # "8630505";"Atividade odontológica sem recursos para realização de procedimentos cirúrgicos"
    df_estabelecimentos_odonto = df_estabelecimentos.filter(F.col("cd_cnae_fiscal").isin("8630504", "8630504"))

    logging.info("Applying transformations and castings")
    df_estab_tratado = (
        df_estabelecimentos_odonto
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
        .fillna({"cd_pais": "105"}) # Se o codigo do pais for nulo, seta para o codigo do Brasil
    )
    # O schema de codigos sera utilizado para as entidades de Motivos e Cnaes
    csv_code_description_schema = StructType([
        StructField("codigo", StringType(), nullable=True),
        StructField("descricao", StringType(), nullable=True)
    ])

    logging.info("Joining fields from MOTIVOS entity")
    files_motivos_to_transform = list_files_to_transform(entity_name=EntitiesSynced.MOTIVOS.value)
    df_motivos = spark.read.csv(files_motivos_to_transform, header=False, sep=";",
                                schema=csv_code_description_schema,
                                encoding="ISO-8859-1")

    # noinspection PyTypeChecker
    df_estab_motivos = (
        df_estab_tratado
        .join(df_motivos,
              df_estab_tratado.cd_motivo_situacao_cadastral == df_motivos.codigo, "left")
        .select(
            df_estab_tratado["*"],  # Todas as colunas do df_estab_tratado
            df_motivos["descricao"].alias("motivo_situacao_cadastral")  # Apenas a coluna "descricao" do df_motivos
        ))

    logging.info("Joining fields from CNAES entity")
    files_cnaes_to_transform = list_files_to_transform(EntitiesSynced.CNAES.value)
    df_cnaes = spark.read.csv(files_cnaes_to_transform, header=False, sep=";",
                              schema=csv_code_description_schema,
                              encoding="ISO-8859-1")

    # noinspection PyTypeChecker
    df_estab_motivos_cnaes = (
        df_estab_motivos
        .join(df_cnaes,
              df_estab_motivos.cd_cnae_fiscal == df_cnaes.codigo, "left")
        .select(
            df_estab_motivos["*"],  # Todas as colunas do df_estab_tratado
            df_cnaes["descricao"].alias("cnae_fiscal")  # Apenas a coluna "descricao" do df_motivos
        ))

    logging.info("Joining fields from MUNICIPIOS entity")
    files_municipios_to_transform = list_files_to_transform(EntitiesSynced.MUNICIPIOS.value)
    df_municipios = spark.read.csv(files_municipios_to_transform, header=False, sep=";",
                                   schema=csv_code_description_schema,
                                   encoding="ISO-8859-1")

    # noinspection PyTypeChecker
    df_estab_motivos_cnaes_municipios = (
        df_estab_motivos_cnaes
        .join(df_municipios,
              df_estab_motivos_cnaes.cd_municipio == df_municipios.codigo, "left")
        .select(
            df_estab_motivos_cnaes["*"],  # Todas as colunas do df_estab_tratado
            df_municipios["descricao"].alias("municipio")  # Apenas a coluna "descricao" do df_municipios
        ))

    logging.info("Joining fields from PAISES entity")
    files_paises_to_transform = list_files_to_transform(EntitiesSynced.PAISES.value)
    df_paises = spark.read.csv(files_paises_to_transform, header=False, sep=";",
                               schema=csv_code_description_schema,
                               encoding="ISO-8859-1")

    # noinspection PyTypeChecker
    df_estab_motivos_cnaes_municipios_paises = (
        df_estab_motivos_cnaes_municipios
        .join(df_paises,
              df_estab_motivos_cnaes_municipios.cd_pais == df_paises.codigo, "left")
        .select(
            df_estab_motivos_cnaes_municipios["*"],  # Todas as colunas do df_estab_tratado
            df_paises["descricao"].alias("pais")  # Apenas a coluna "descricao" do df_paises
        ))

    # O Select apenas ajusta a ordem das colunas para que sejam persistidas
    # de uma maneira mais facil de visualizar, colocando as descricoes
    # ao lado dos codigos
    df_estabelecimentos_final = df_estab_motivos_cnaes_municipios_paises.select('cnpj_basico',
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
                                                                                'cd_pais',
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
                                                                                'cd_municipio',
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
    # Para salvar apenas 1 arquivo Parquet mudamos
    # para apenas 1 particao
    df_estabelecimentos_final.repartition(1).write.mode("overwrite").parquet(output_data)


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
