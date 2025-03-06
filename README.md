# Configurando ambiente de desenvolvimento

## Criação do env conda

```shell
conda create --prefix ./condaenv python=3.12 pip
conda activate ./condaenv
```

## Instalando as dependências do projeto

Para executar os Notebook do projeto é necessário também instalar o Jupyter no novo env criado:

```shell
conda install jupyter
```

As libs abaixo são necessárias para execução do scripts

```shell
pip install -r requirements.txt
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