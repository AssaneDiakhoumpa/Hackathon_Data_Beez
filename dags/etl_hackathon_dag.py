from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from Extract.weather_extractor import get_weather_for_south_regions
from Extract.fao_extractor import get_fao_data
from Extract.copernicus_extractor import get_copernicus_data
from Transform.transform_data import transform
from Load.load_to_postgres import load_to_postgres
import pandas as pd
import os
import pathlib
import gc
import logging

# Configuration logging
logger = logging.getLogger(__name__)


def log_memory_usage(step_name):
    """Log l'utilisation mémoire actuelle"""
    import psutil
    process = psutil.Process()
    mem_info = process.memory_info()
    mem_mb = mem_info.rss / 1024 / 1024
    logger.info(f"[{step_name}] Mémoire utilisée: {mem_mb:.2f} MB")


def extract_data(**context):
    """Extraction optimisée avec gestion mémoire"""
    logger.info("Début extraction des données...")
    log_memory_usage("Début")
    
    # Configuration du répertoire de travail
    project_root = pathlib.Path(__file__).resolve().parents[1]
    data_candidate = project_root / "data_corpinius" / "swi_global_12.5km_10daily_v3_cog.csv"
    alt_candidate = pathlib.Path("/opt/airflow") / "data_corpinius" / "swi_global_12.5km_10daily_v3_cog.csv"
    
    if data_candidate.exists():
        os.chdir(project_root)
        logger.info(f"CWD changé vers {project_root}")
    elif alt_candidate.exists():
        os.chdir(alt_candidate.parents[1])
        logger.info(f"CWD changé vers {alt_candidate.parents[1]}")
    else:
        raise FileNotFoundError(
            f"Fichier de données introuvable.\n"
            f"Vérifié : {data_candidate}\n"
            f"Vérifié : {alt_candidate}"
        )
    
    # Créer le dossier de sortie
    os.makedirs("/tmp/data", exist_ok=True)
    
    # ⚡ EXTRACTION 1 : WEATHER (traiter et libérer immédiatement)
    try:
        logger.info("Extraction Weather...")
        weather = get_weather_for_south_regions()
        logger.info(f"Weather récupéré: {len(weather)} lignes")
        log_memory_usage("Après Weather")
        
        # Sauvegarder IMMÉDIATEMENT
        weather.to_csv("/tmp/data/weather.csv", index=False)
        
        # Libérer la mémoire
        del weather
        gc.collect()
        log_memory_usage("Après nettoyage Weather")
        
    except Exception as e:
        logger.error(f"Erreur Weather: {str(e)}")
        raise
    
    # ⚡ EXTRACTION 2 : FAO (traiter et libérer immédiatement)
    try:
        logger.info("Extraction FAO...")
        
        # Nouveau (simplifié)
        fao = get_fao_data()
        logger.info(f"FAO récupéré: {len(fao)} lignes")
        log_memory_usage("Après FAO")
        
        # Sauvegarder IMMÉDIATEMENT
        fao.to_csv("/tmp/data/fao.csv", index=False)
        
        # Libérer la mémoire
        del fao
        gc.collect()
        log_memory_usage("Après nettoyage FAO")
        
    except Exception as e:
        logger.error(f"Erreur FAO: {str(e)}")
        raise
    
    # ⚡ EXTRACTION 3 : COPERNICUS (le plus lourd - optimisé)
    try:
        logger.info("Extraction Copernicus...")
        
        # Désactiver temporairement le GC pour performance
        gc.disable()
        
        copernicus = get_copernicus_data()
        logger.info(f"Copernicus récupéré: {len(copernicus)} lignes")
        log_memory_usage("Après Copernicus")
        
        # Sauvegarder par morceaux si le DataFrame est très grand
        if len(copernicus) > 100000:
            logger.info("Grand dataset détecté, écriture par morceaux...")
            chunk_size = 50000
            for i in range(0, len(copernicus), chunk_size):
                chunk = copernicus.iloc[i:i+chunk_size]
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                chunk.to_csv("/tmp/data/copernicus.csv", mode=mode, header=header, index=False)
                logger.info(f"Chunk {i//chunk_size + 1} écrit")
        else:
            copernicus.to_csv("/tmp/data/copernicus.csv", index=False)
        
        # Libérer la mémoire
        del copernicus
        
        # Réactiver et forcer le GC
        gc.enable()
        gc.collect()
        log_memory_usage("Après nettoyage Copernicus")
        
    except Exception as e:
        gc.enable()  # S'assurer que le GC est réactivé même en cas d'erreur
        logger.error(f"Erreur Copernicus: {str(e)}")
        raise
    
    logger.info("Extraction terminée avec succès")
    log_memory_usage("Fin extraction")


def transform_data(**context):
    logger.info("Début transformation...")
    log_memory_usage("Début transform")
    
    try:
        logger.info("Chargement weather...")
        weather = pd.read_csv("/tmp/data/weather.csv", low_memory=False)
        log_memory_usage("Après chargement weather")
        
        logger.info("Chargement FAO...")
        fao = pd.read_csv("/tmp/data/fao.csv", low_memory=False)
        log_memory_usage("Après chargement FAO")
        
        # ⚡ Suppression du chargement Copernicus
        # logger.info("Chargement Copernicus...")
        # copernicus = pd.read_csv("/tmp/data/copernicus.csv", low_memory=False)
        # log_memory_usage("Après chargement Copernicus")
        
        logger.info("Application des transformations...")
        df_final = transform(weather, fao)  
        
        del weather, fao
        gc.collect()
        log_memory_usage("Après transformation")
        
        df_final.to_csv("/tmp/data/final.csv", index=False)
        logger.info(f"Transformation terminée: {len(df_final)} lignes finales")
        
        del df_final
        gc.collect()
        log_memory_usage("Fin transform")
        
    except Exception as e:
        logger.error(f"Erreur transformation: {str(e)}")
        raise


def load_data(**context):
    """Chargement optimisé vers PostgreSQL"""
    logger.info("Début chargement PostgreSQL...")
    log_memory_usage("Début load")
    
    try:
        # Charger par morceaux si nécessaire
        chunk_size = 10000
        first_chunk = True
        
        for chunk in pd.read_csv("/tmp/data/final.csv", chunksize=chunk_size):
            logger.info(f"Chargement chunk de {len(chunk)} lignes...")
            # CORRECTION : Utiliser 'chunk' au lieu de 'df'
            load_to_postgres(chunk, 'weather_agro_data', if_exists='append')
            first_chunk = False
            
            # Libérer le chunk
            del chunk
            gc.collect()
        
        logger.info("Données chargées dans PostgreSQL !")
        log_memory_usage("Fin load")
        
    except Exception as e:
        logger.error(f"Erreur chargement: {str(e)}")
        raise

# --- Définition du DAG ---
default_args = {
    'owner': 'hackathon_databeez',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=20),  # Timeout global
}

with DAG(
    'etl_hackathon_dag',
    default_args=default_args,
    description='Pipeline ETL météo/agro optimisé pour Hackathon DataBeez',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2025, 10, 15),
    catchup=False,
    max_active_runs=1,  # Éviter les exécutions simultanées
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        execution_timeout=timedelta(minutes=10),
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=8),
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        execution_timeout=timedelta(minutes=5),
    )
    
    # Chaînage du pipeline
    extract_task >> transform_task >> load_task