import sys
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow")
logger = logging.getLogger(__name__)


def run_scraper(**context):
    from anidata_scraper import scrape_to_file
    output_path = scrape_to_file(
        output_dir="/opt/airflow/data/raw",
        base_url="http://mock-site",
    )
    logger.info("Scraping terminé → %s", output_path)
    return output_path


with DAG(
    dag_id="scraper_dag",
    schedule="@onces",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    tags=["scraping", "anidata"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_anidex",
        python_callable=run_scraper,
    )

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_dag",
        trigger_dag_id="etl_dag",
        wait_for_completion=False,
    )

    scrape_task >> trigger_etl
#ok 
