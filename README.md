# ETL Hackathon DataBeez – Pipeline Météo & Agro

## Présentation du projet

Ce projet implémente un **pipeline ETL** (Extract, Transform, Load) pour centraliser, transformer et analyser des données météorologiques et agricoles (FAO) du Sénégal.

L'objectif est de créer une **base de données exploitable** pour le suivi agro-climatique et la prédiction de rendements.

## Architecture Docker Compose

```yaml
version: '3.8'

services:
  # PostgreSQL pour metadata Airflow & données ETL
  db:
    image: postgres:14.1-alpine
    container_name: postgres_hackathonDataBeez
    env_file: [.env]
    ports: ["5433:5432"]
    volumes:
      - db_data:/var/lib/postgresql/data
      - ./init_airflow_db.sql:/docker-entrypoint-initdb.d/init_airflow_db.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  # Airflow avec SequentialExecutor (idéal hackathon/local)
  airflow:
    build: .
    container_name: airflow_etl
    restart: always
    env_file: [.env]
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=${AIRFLOW__CORE__SQL_ALCHEMY_CONN}
    depends_on:
      db: { condition: service_healthy }
    ports: ["8080:8080"]
    volumes:
      - ./dags:/opt/airflow/dags
      - ./etl_pipelines:/opt/airflow/etl_pipeline
      - ./data_corpinius:/opt/airflow/data_corpinius
      - ./data:/opt/airflow/data
    command: >
      bash -c "
        airflow db init &&
        airflow users create --username ${AIRFLOW_USER} 
                             --firstname ${AIRFLOW_FIRSTNAME} 
                             --lastname ${AIRFLOW_LASTNAME} 
                             --role ${AIRFLOW_ROLE} 
                             --email ${AIRFLOW_EMAIL} 
                             --password ${AIRFLOW_PASSWORD} &&
        airflow standalone
      "

volumes:
  db_data:
  logs:
```

**Ports** : Airflow UI → `localhost:8080`, Postgres → `localhost:5433`.[2][6]

## Commandes d'exécution

```bash
# 1. Build & démarrage (premier lancement)
docker compose up --build -d

# 2. Vérification
docker compose ps
docker compose logs db        # Logs Postgres
docker compose logs airflow   # Logs Airflow

# 3. Accès Postgres
docker exec -it postgres_hackathonDataBeez psql -U postgres -d airflow

-- Lister bases de données
\l

-- Se connecter à la DB Airflow (créée par init_airflow_db.sql)
\c airflow

-- Lister tables (DAG runs, task instances, etc.)
\dt

# 4. Airflow UI: http://localhost:8080
# 5. Arrêt propre
docker compose down
```

## Architecture du pipeline ETL

### A. Extraction (Extract)
- **Weather** : API météo par département Sénégal.[1]
- **FAO** : Données agricoles (rendement, culture).
- **Copernicus** : Indices sol (SWI) par chunks (optimisation mémoire).
- Sauvegarde `/tmp/data/*.csv` + libération mémoire immédiate.

### B. Transformation (Transform)
- Fusion `weather + FAO` sur `departement`/`date` (left join).
- Nettoyage : doublons, NaN → moyenne, normalisation dates.
- Résultat : `/tmp/data/final.csv`.

### C. Chargement (Load)
- Table PostgreSQL : `weather_agro_data`.
- Chargement par chunks (10k lignes) avec `if_exists='append'`.

## Monitoring & Debug

```bash
# Logs DAGs
docker compose logs -f airflow

# Stats Postgres
docker exec -it postgres_hackathonDataBeez psql -U postgres -d airflow \
  -c "SELECT dag_id, count(*) FROM dag_run GROUP BY dag_id;"

# Mémoire Airflow
docker exec airflow_etl psutil.Process().memory_info().rss / 1024 / 1024
```
## Vérifier le résultat final dans PostgreSQL :


   ```sql
   SELECT * FROM weather_agro_data;
   ```"