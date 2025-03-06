FROM apache/airflow:2.10.3

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \

# Windows
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# MacOs
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3 pyspark==3.5.3