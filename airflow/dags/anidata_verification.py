from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
from pathlib import Path
import sys
import logging
logger = logging.getLogger(__name__)

INPUT_DIR = "/opt/airflow/data/archive"
TMP_DIR = "/opt/airflow/data/raw"
sys.path.append('/opt/airflow/scripts')

# Maintenant, tu peux faire ton import
from converters import identify_and_convert

def check_and_convert(**context):
    import os
    from converters import identify_and_convert
    
    INPUT_DIR = "/opt/airflow/data/archive"
    RAW_DIR = "/opt/airflow/data/raw"
    os.makedirs(RAW_DIR, exist_ok=True)

    # 1. Lister les fichiers
    files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) 
    if os.path.isfile(os.path.join(INPUT_DIR, f))]
    
    if not files:
        raise Exception("Aucun fichier trouvé dans archive/")

    # 2. Prendre le plus récent
    latest_file = max(files, key=os.path.getmtime)
    
    # 3. Identifier et convertir via la nouvelle fonction
    csv_path = identify_and_convert(latest_file, RAW_DIR)
    
    logger.info(f"Fichier prêt pour le traitement : {csv_path}")
    return csv_path

with DAG("dag_verification", start_date=datetime(2026, 1, 1), schedule_interval="@hourly", catchup=False) as dag:
    
    check_task = PythonOperator(
        task_id="check_latest_and_convert",
        python_callable=check_and_convert
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_traitement",
        trigger_dag_id="anidata_extract_simple",
        # On envoie le chemin du fichier dans la conf du Trigger
        conf={"input_csv": "{{ task_instance.xcom_pull(task_ids='check_latest_and_convert') }}"},
        reset_dag_run=True,
        wait_for_completion=False
    )

    check_task >> trigger_processing