import json
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

RAW_DIR = "/opt/airflow/data/raw"
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "anidex_animes"


def get_latest_json():
    """Trouve le fichier JSON le plus récent dans data/raw/"""
    files = list(Path(RAW_DIR).glob("anime_*.json"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier JSON dans {RAW_DIR}")
    latest = max(files, key=lambda f: f.stat().st_mtime)
    logger.info(f"Fichier sélectionné : {latest}")
    return str(latest)


def load_and_index(**context):
    """Lit le JSON scrapé et l'indexe dans Elasticsearch"""
    from elasticsearch import Elasticsearch, helpers

    filepath = context["task_instance"].xcom_pull(task_ids="get_latest_json")
    
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    animes = data.get("animes", [])
    logger.info(f"Indexation de {len(animes)} animes dans ES...")

    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError("Elasticsearch inaccessible")

    # Crée ou recrée l'index
    if es.indices.exists(index=ES_INDEX):
        es.indices.delete(index=ES_INDEX)
    es.indices.create(index=ES_INDEX, body={
        "settings": {"number_of_shards": 1, "number_of_replicas": 0}
    })

    def generate_docs():
        for anime in animes:
            yield {
                "_index": ES_INDEX,
                "_id": str(anime.get("id")),
                "_source": anime,
            }

    success, errors = helpers.bulk(es, generate_docs(), raise_on_error=False)
    logger.info(f"Indexés : {success}, Erreurs : {len(errors)}")
    
    count = es.count(index=ES_INDEX)["count"]
    logger.info(f"Total dans ES : {count} documents")
    return count


with DAG(
    dag_id="etl_dag",
    schedule=None,
    start_date=datetime(2026, 4, 27),
    catchup=False,
    tags=["etl", "anidata"],
) as dag:

    get_file = PythonOperator(
        task_id="get_latest_json",
        python_callable=get_latest_json,
    )

    index_task = PythonOperator(
        task_id="load_and_index",
        python_callable=load_and_index,
        provide_context=True,
    )

    get_file >> index_task
    #final