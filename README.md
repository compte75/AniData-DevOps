# AniData-DevOps
## Stack

| Composant | Technologie |
|-----------|-------------|
| Versioning | Git, GitHub |
| CI/CD | GitHub Actions, GHCR |
| Scraping | Python, BeautifulSoup4 |
| Orchestration | Apache Airflow 2.8.1 |
| Stockage | Elasticsearch 8.x |
| Visualisation | Grafana |
| Conteneurisation | Docker, Docker Compose |

## Lancer le projet

**Prérequis :** Docker Desktop, Git

```bash
git clone https://github.com/compte75/AniData-DevOps.git
cd AniData-DevOps
cp .env.example .env  # remplir les variables
docker build -t anidata-airflow .
docker compose up -d
```

Airflow : http://localhost:8080 (admin / admin)  
Grafana : http://localhost:3000 (admin / anidata)  
Elasticsearch : http://localhost:9200

## CI/CD

Chaque push sur `main` déclenche automatiquement :
1. `lint` — vérification du code avec ruff
2. `tests` — tests unitaires avec pytest
3. `build-and-push` — build de l'image Docker et push sur GHCR

L'image est publiée sur `ghcr.io/compte75/anidata-airflow`.

## DAGs

**scraper_dag** — scrape le mock-site AniDex (nginx local) avec BeautifulSoup4 et sauvegarde un fichier JSON dans `/opt/airflow/data/raw/`. Déclenche automatiquement `etl_dag` à la fin.

**etl_dag** — lit le JSON produit par le scraper et indexe les documents dans Elasticsearch (index `anidex_animes`).

## Structure du projetPipeline complète opérationnelle.
