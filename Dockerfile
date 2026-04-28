FROM apache/airflow:2.8.1-python3.11

USER root
COPY airflow/dags/ /opt/airflow/dags/
COPY airflow/scripts/ /opt/airflow/scripts/
COPY anidata-scraper/anidata_scraper/ /opt/airflow/anidata_scraper/

USER airflow
RUN pip install --no-cache-dir \
    pandas \
    elasticsearch \
    apache-airflow-providers-elasticsearch \
    requests \
    beautifulsoup4