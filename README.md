# Configurando ambiente de desenvolvimento

## Criação do env conda

```shell
conda create --prefix ./condaenv python=3.12 pip
conda activate ./condaenv
```

## Instalando Airflow (docker-compose)

```shell
docker-compose build
docker-compose up airflow-init
```

Se o Airflow já tiver sido instalado basta executar com o comando abaixo:

```shell
docker-compose up
```