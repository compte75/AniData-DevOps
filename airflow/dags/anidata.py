
"""
DAG Airflow - AniData Lab
Pipeline : Audit → Nettoyage → Features → Indexation

"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import json

import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# ============================================================
# CONFIG
# ============================================================

logger = logging.getLogger(__name__)

DATA_DIR   = "/opt/airflow/data/archive"
OUTPUT_DIR = "/opt/airflow/data/extracted"

# Fichiers sources
ANIME_FILE    = f"{DATA_DIR}/anime.csv"
RATING_FILE   = f"{DATA_DIR}/rating_complete.csv"
SYNOPSIS_FILE = f"{DATA_DIR}/anime_with_synopsis.csv"

# Fichiers intermédiaires et finaux
ANIME_RAW     = f"{OUTPUT_DIR}/anime_raw.csv"
ANIME_CLEANED = f"{OUTPUT_DIR}/anime_cleaned.csv"
RATING_RAW    = f"{OUTPUT_DIR}/rating_raw.csv"
SYNOPSIS_RAW  = f"{OUTPUT_DIR}/synopsis_raw.csv"
SYNOPSIS_GOLD = f"{OUTPUT_DIR}/synopsis_gold.csv"
SYNOPSIS_JSON = f"{OUTPUT_DIR}/synopsis_gold.json"

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "anidata_extract_simple",
    default_args=default_args,
    description="Pipeline AniData - rating + synopsis uniquement",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["anidata", "extract"],
    max_active_runs=1,
)

# ============================================================
# UTILITAIRES
# ============================================================

def audit_extraire_anime(**context):
    # Récupération du chemin envoyé par le DAG de vérification
    conf = context['dag_run'].conf
    target_file = conf.get('input_csv')
    
    if not target_file:
         # Fallback sur ton fichier par défaut si lancé manuellement
         target_file = ANIME_FILE 

def bloquer(message: str):
    """Arrête le pipeline avec un message d'erreur clair."""
    logger.error(f"PIPELINE BLOQUÉ — {message}")
    raise AirflowException(f"PIPELINE BLOQUÉ — {message}")


def notification_echec(context):
    """Appelée automatiquement à chaque échec de tâche."""
    task_id = context["task_instance"].task_id
    dag_id  = context["dag"].dag_id
    err     = context.get("exception", "Erreur inconnue")
    logger.error("=" * 60)
    logger.error("ECHEC DE TACHE")
    logger.error(f"  DAG   : {dag_id}")
    logger.error(f"  Tâche : {task_id}")
    logger.error(f"  Erreur: {err}")
    logger.error("=" * 60)


# ============================================================
# TÂCHE 1 — AUDIT
# ============================================================

def audit_valider_fichiers():
    """Vérifie que les fichiers sources existent."""
    logger.info("Vérification des fichiers sources...")
    fichiers = {
        "anime.csv":               ANIME_FILE,
        "rating_complete.csv":     RATING_FILE,
        "anime_with_synopsis.csv": SYNOPSIS_FILE,
    }
    for nom, chemin in fichiers.items():
        if not Path(chemin).exists():
            bloquer(f"Fichier introuvable : {chemin}")
        taille_mb = Path(chemin).stat().st_size / (1024 * 1024)
        logger.info(f"OK — {nom} ({taille_mb:.1f} MB)")
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    logger.info("Validation OK, dossier de sortie prêt.")


def audit_extraire_synopsis(**context):
    """Lit anime_with_synopsis.csv par chunks de 5000 lignes."""
    logger.info("Extraction synopsis...")
    try:
        chunks = []
        for chunk in pd.read_csv(SYNOPSIS_FILE, chunksize=5_000,
                                 on_bad_lines="skip", encoding="utf-8"):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        if "sypnopsis" in df.columns:
            df.rename(columns={"sypnopsis": "synopsis"}, inplace=True)
            logger.info("Typo 'sypnopsis' corrigée → 'synopsis'")
        df["synopsis"] = df["synopsis"].fillna("").str.strip()
        df.to_csv(SYNOPSIS_RAW, index=False, encoding="utf-8")
        logger.info(f"Synopsis : {len(df)} lignes extraites")
        context["task_instance"].xcom_push(key="synopsis_rows", value=len(df))
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur synopsis — {e}")



def audit_extraire_anime(**context):
    """Lit anime.csv par chunks de 5000 lignes."""
    logger.info("Extraction anime...")
    try:
        chunks = []
        for chunk in pd.read_csv(ANIME_FILE, chunksize=5_000,
                                 on_bad_lines="skip", encoding="utf-8"):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        df.columns = [c.lower().replace(" ", "_").replace("-", "_")
                      for c in df.columns]
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].replace(["Unknown", "N/A", "-", ""], np.nan)
        df.to_csv(ANIME_RAW, index=False, encoding="utf-8")
        logger.info(f"Anime : {len(df)} lignes extraites")
        context["task_instance"].xcom_push(key="anime_rows", value=len(df))
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur anime — {e}")

def audit_extraire_rating(**context):
    """Lit rating_complete.csv ."""
    logger.info("Extraction rating...")
    try:
        df = pd.read_csv(RATING_FILE, nrows=500_000, on_bad_lines="skip",
                         encoding="utf-8", low_memory=False)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        avant = len(df)
        df = df[(df["rating"] >= 0) & (df["rating"] <= 10)]
        if len(df) < avant:
            logger.warning(f"{avant - len(df)} ratings invalides supprimés")
        df.to_csv(RATING_RAW, index=False, encoding="utf-8")
        logger.info(f"Rating : {len(df)} lignes extraites")
        context["task_instance"].xcom_push(key="rating_rows", value=len(df))
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur rating — {e}")


def audit_resume(**context):
    """Bilan de l'audit."""
    ti = context["task_instance"]
    anime_rows    = ti.xcom_pull(task_ids="audit_extraire_anime",    key="anime_rows")
    rating_rows   = ti.xcom_pull(task_ids="audit_extraire_rating",   key="rating_rows")
    synopsis_rows = ti.xcom_pull(task_ids="audit_extraire_synopsis", key="synopsis_rows")
    logger.info("=" * 50)
    logger.info("T1 AUDIT TERMINÉ")
    logger.info(f"  Anime    : {anime_rows:>10,} lignes")
    logger.info(f"  Ratings  : {rating_rows:>10,} lignes")
    logger.info(f"  Synopsis : {synopsis_rows:>10,} lignes")
    logger.info("=" * 50)


# ============================================================
# TÂCHE 2 — NETTOYAGE
# ============================================================


def nettoyage_anime(**context):
    """Nettoie anime.csv : doublons, types, outliers."""
    logger.info("Nettoyage anime...")
    try:
        if not Path(ANIME_RAW).exists():
            bloquer(f"Fichier audit introuvable : {ANIME_RAW}")
        df = pd.read_csv(ANIME_RAW)
        n_initial = len(df)
        df = df.drop_duplicates()
        id_col = next((c for c in ["mal_id", "anime_id"] if c in df.columns), None)
        if id_col:
            df = df.drop_duplicates(subset=[id_col], keep="first")
        for col in ["episodes", "members", "favorites", "score", "scored_by"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "score" in df.columns:
            df.loc[df["score"] == 0, "score"] = np.nan
        df["is_outlier"] = False
        if "score" in df.columns:
            df.loc[df["score"].notna() & ((df["score"] < 1) | (df["score"] > 10)), "is_outlier"] = True
        logger.info(f"Nettoyage : {n_initial} → {len(df)} lignes")
        df.to_csv(ANIME_CLEANED, index=False, encoding="utf-8")
        context["task_instance"].xcom_push(key="anime_cleaned_rows", value=len(df))
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur nettoyage anime — {e}")

def nettoyage_synopsis(**context):
    """Nettoie synopsis : doublons, NaN, normalisation texte."""
    logger.info("Nettoyage synopsis...")
    try:
        if not Path(SYNOPSIS_RAW).exists():
            bloquer(f"Fichier audit introuvable : {SYNOPSIS_RAW}")
        df = pd.read_csv(SYNOPSIS_RAW)
        n_initial = len(df)

        # Doublons
        df = df.drop_duplicates()
        if "mal_id" in df.columns:
            df = df.drop_duplicates(subset=["mal_id"], keep="first")

        # Nettoyage texte synopsis
        if "synopsis" in df.columns:
            df["synopsis"] = df["synopsis"].str.strip()
            df["synopsis"] = df["synopsis"].str.replace(r"\s+", " ", regex=True)
            df.loc[df["synopsis"] == "", "synopsis"] = np.nan

        # Score numérique
        if "score" in df.columns:
            df["score"] = pd.to_numeric(df["score"], errors="coerce")
            df.loc[df["score"] == 0, "score"] = np.nan

        logger.info(f"Nettoyage : {n_initial} → {len(df)} lignes")
        df.to_csv(SYNOPSIS_RAW, index=False, encoding="utf-8")
        context["task_instance"].xcom_push(key="synopsis_cleaned_rows", value=len(df))

    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur nettoyage synopsis — {e}")


def nettoyage_rating(**context):
    """Nettoie rating : doublons, valeurs hors bornes."""
    logger.info("Nettoyage rating...")
    try:
        if not Path(RATING_RAW).exists():
            bloquer(f"Fichier audit introuvable : {RATING_RAW}")
        df = pd.read_csv(RATING_RAW)
        n_initial = len(df)

        df = df.drop_duplicates()
        df = df[(df["rating"] >= 0) & (df["rating"] <= 10)]

        logger.info(f"Nettoyage : {n_initial} → {len(df)} lignes")
        df.to_csv(RATING_RAW, index=False, encoding="utf-8")
        context["task_instance"].xcom_push(key="rating_cleaned_rows", value=len(df))

    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur nettoyage rating — {e}")


# ============================================================
# TÂCHE 3 — FEATURES
# ============================================================

def features_synopsis(**context):
    """Enrichit le synopsis : score_category, n_genres, main_genre."""
    logger.info("Feature engineering synopsis...")
    try:
        if not Path(SYNOPSIS_RAW).exists():
            bloquer(f"Fichier nettoyé introuvable : {SYNOPSIS_RAW}")
        df = pd.read_csv(SYNOPSIS_RAW)
        n_cols_before = df.shape[1]

        # Catégorie de score
        if "score" in df.columns:
            df["score"] = pd.to_numeric(df["score"], errors="coerce")
            df["score_category"] = pd.cut(
                df["score"], bins=[0, 5, 6.5, 8, 10],
                labels=["Mauvais", "Moyen", "Bon", "Excellent"],
                include_lowest=True,
            )
            logger.info("Feature 'score_category' créée")

        # Genres
        genre_col = next((c for c in ["genres", "genre"] if c in df.columns), None)
        if genre_col:
            df["n_genres"] = df[genre_col].str.split(", ").str.len()
            df.loc[df[genre_col].isna(), "n_genres"] = np.nan
            df["main_genre"] = df[genre_col].str.split(", ").str[0]
            logger.info("Features 'n_genres' et 'main_genre' créées")

        # Longueur du synopsis
        if "synopsis" in df.columns:
            df["synopsis_length"] = df["synopsis"].str.len().fillna(0).astype(int)
            logger.info("Feature 'synopsis_length' créée")

        logger.info(f"Features : {df.shape[1] - n_cols_before} nouvelles colonnes")
        df.to_csv(SYNOPSIS_GOLD, index=False, encoding="utf-8")
        context["task_instance"].xcom_push(key="synopsis_gold_rows", value=len(df))

    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur features synopsis — {e}")


# ============================================================
# TÂCHE 4 — INDEXATION
# ============================================================

def indexation_valider(**context):
    """Valide le dataset gold avec des assertions."""
    logger.info("Validation dataset gold...")
    try:
        if not Path(SYNOPSIS_GOLD).exists():
            bloquer(f"Fichier gold introuvable : {SYNOPSIS_GOLD}")
        df = pd.read_csv(SYNOPSIS_GOLD)
        echecs = []

        if len(df) == 0:
            echecs.append("Dataset vide")
        if len(df) < 1_000:
            echecs.append(f"Moins de 1000 lignes ({len(df)})")
        if "mal_id" in df.columns and df["mal_id"].duplicated().sum() > 0:
            echecs.append("mal_id non unique")
        if "score" in df.columns:
            scores = pd.to_numeric(df["score"], errors="coerce").dropna()
            if len(scores) > 0 and not ((scores >= 1).all() and (scores <= 10).all()):
                echecs.append("Scores hors [1,10]")

        nan_pct = df.isna().sum().sum() / (df.shape[0] * df.shape[1]) * 100
        if nan_pct >= 50:
            echecs.append(f"Taux NaN trop élevé : {nan_pct:.1f}%")

        if echecs:
            bloquer(f"Validation échouée — {' | '.join(echecs)}")

        logger.info(f"Validation OK — {len(df)} lignes, NaN : {nan_pct:.1f}%")
        context["task_instance"].xcom_push(key="validation_rows", value=len(df))

    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur validation — {e}")


def indexation_export_json():
    """Exporte le dataset gold en JSON pour Elasticsearch."""
    logger.info("Export JSON...")
    try:
        df = pd.read_csv(SYNOPSIS_GOLD)
        records = df.where(df.notna(), None).to_dict(orient="records")
        with open(SYNOPSIS_JSON, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(
                    {k: v for k, v in record.items() if v is not None},
                    ensure_ascii=False
                ) + "\n")
        taille = Path(SYNOPSIS_JSON).stat().st_size / (1024 * 1024)
        logger.info(f"JSON : {len(records)} documents — {taille:.1f} MB")
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur export JSON — {e}")


def indexation_elasticsearch():
    """Indexe dans Elasticsearch (ignoré si non disponible)."""
    logger.info("Indexation Elasticsearch...")
    try:
        from elasticsearch import Elasticsearch, helpers
        es = Elasticsearch("http://elasticsearch:9200")
        if not es.ping():
            bloquer("Elasticsearch inaccessible")
        if es.indices.exists(index="anime_synopsis"):
            es.indices.delete(index="anime_synopsis")
        es.indices.create(index="anime_synopsis", body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0}
        })
        def generer_docs():
            with open(SYNOPSIS_JSON, encoding="utf-8") as f:
                for line in f:
                    yield {"_index": "anime_synopsis",
                           "_source": json.loads(line.strip())}
        success, errors = helpers.bulk(es, generer_docs(), chunk_size=500,
                                       raise_on_error=False)
        logger.info(f"Indexation : {success} docs, {len(errors)} erreurs")
        logger.info(f"Total ES : {es.count(index='anime_synopsis')['count']} docs")
    except ImportError:
        logger.warning("Module elasticsearch non installé — ignoré")
    except AirflowException:
        raise
    except Exception as e:
        bloquer(f"Erreur ES — {e}")


def resume_final(**context):
    """Résumé final du pipeline."""
    ti = context["task_instance"]
    anime_rows    = ti.xcom_pull(task_ids="audit_extraire_anime",    key="anime_rows")
    rating_rows   = ti.xcom_pull(task_ids="audit_extraire_rating",   key="rating_rows")
    synopsis_rows = ti.xcom_pull(task_ids="audit_extraire_synopsis", key="synopsis_rows")
    gold_rows     = ti.xcom_pull(task_ids="features_synopsis",       key="synopsis_gold_rows")
    valid_rows    = ti.xcom_pull(task_ids="indexation_valider",      key="validation_rows")
    logger.info("=" * 60)
    logger.info("PIPELINE ANIDATA — RÉSUMÉ COMPLET")
    logger.info("=" * 60)
    logger.info(f"  [T1] Anime bruts     : {anime_rows:>10,} lignes")
    logger.info(f"  [T1] Ratings bruts   : {rating_rows:>10,} lignes")
    logger.info(f"  [T1] Synopsis bruts  : {synopsis_rows:>10,} lignes")
    logger.info(f"  [T3] Gold            : {gold_rows:>10,} lignes enrichies")
    logger.info(f"  [T4] Validé          : {valid_rows:>10,} lignes validées")
    logger.info(f"  Timestamp : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("=" * 60)
    logger.info("STATUT : SUCCÈS")


# ============================================================
# DÉFINITION DES TÂCHES
# ============================================================

kw = {"on_failure_callback": notification_echec}

# T1 — Audit
t1_valider  = PythonOperator(task_id="audit_valider_fichiers",  python_callable=audit_valider_fichiers,  dag=dag, **kw)
t1_anime    = PythonOperator(task_id="audit_extraire_anime",    python_callable=audit_extraire_anime,    dag=dag, provide_context=True, **kw)
t1_synopsis = PythonOperator(task_id="audit_extraire_synopsis", python_callable=audit_extraire_synopsis, dag=dag, provide_context=True, **kw)
t1_rating   = PythonOperator(task_id="audit_extraire_rating",   python_callable=audit_extraire_rating,   dag=dag, provide_context=True, **kw)
t1_resume   = PythonOperator(task_id="audit_resume",            python_callable=audit_resume,            dag=dag, provide_context=True, **kw)

# T2 — Nettoyage
t2_anime     = PythonOperator(task_id="nettoyage_anime",    python_callable=nettoyage_anime,    dag=dag, provide_context=True, **kw)
t2_synopsis  = PythonOperator(task_id="nettoyage_synopsis", python_callable=nettoyage_synopsis, dag=dag, provide_context=True, **kw)
t2_rating    = PythonOperator(task_id="nettoyage_rating",   python_callable=nettoyage_rating,   dag=dag, provide_context=True, **kw)

# T3 — Features
t3_features = PythonOperator(task_id="features_synopsis", python_callable=features_synopsis, dag=dag, provide_context=True, **kw)

# T4 — Indexation
t4_valider = PythonOperator(task_id="indexation_valider",       python_callable=indexation_valider,       dag=dag, provide_context=True, **kw)
t4_json    = PythonOperator(task_id="indexation_export_json",   python_callable=indexation_export_json,   dag=dag, **kw)
t4_es      = PythonOperator(task_id="indexation_elasticsearch", python_callable=indexation_elasticsearch, dag=dag, **kw)
t4_resume  = PythonOperator(task_id="resume_final",             python_callable=resume_final,             dag=dag, provide_context=True, **kw)

# ============================================================
# ORDRE D'EXÉCUTION
# ============================================================
#
#  audit_valider_fichiers
#         │
#  audit_extraire_synopsis   ← chunks 5K
#         │
#  audit_extraire_rating     ← 500K max
#         │
#  audit_resume
#         │
#  nettoyage_synopsis        ← T2
#         │
#  nettoyage_rating
#         │
#  features_synopsis         ← T3
#         │
#  indexation_valider
#         │
#  indexation_export_json
#         │
#  indexation_elasticsearch
#         │
#  resume_final
#

(t1_valider >> t1_synopsis >> t1_rating >> t1_anime >> t1_resume
 >> t2_synopsis >> t2_rating >> t2_anime
 >> t3_features
 >> t4_valider >> t4_json >> t4_es >> t4_resume)
