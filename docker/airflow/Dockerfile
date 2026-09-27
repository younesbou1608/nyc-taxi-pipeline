# Image Airflow du projet.
#  - Java 17 : requis par PySpark
#  - PySpark dans l'environnement Airflow (les taches Python l'importent)
#  - dbt dans un venv separe : ses dependances entrent en conflit avec celles d'Airflow
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyspark==3.5.1 \
 && python -c "import pyspark, google.cloud.bigquery"

RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.8.6 dbt-bigquery==1.8.2 \
 && /opt/airflow/dbt-venv/bin/dbt --version

RUN java -version
