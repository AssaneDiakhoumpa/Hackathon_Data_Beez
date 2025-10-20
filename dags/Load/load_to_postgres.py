import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# Charger les variables d'environnement
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Connexion à la base PostgreSQL
def connect_postgres():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            print("Connexion à PostgreSQL réussie !")
        return engine
    except Exception as e:
        print("Erreur de connexion :", e)
        return None

# Créer la table si elle n'existe pas
def create_table_if_not_exists(engine):
    create_table_query = text("""
        CREATE TABLE IF NOT EXISTS weather_agro_data (
            id SERIAL PRIMARY KEY,
            region VARCHAR(100),
            date DATE,
            temperature_c FLOAT,
            precipitation_mm FLOAT,
            humidity_percent FLOAT,
            departement VARCHAR(100),
            culture VARCHAR(100),
            indicateur VARCHAR(100),
            valeur FLOAT
        );
    """)
    with engine.begin() as conn:
        conn.execute(create_table_query)
    print("Table 'weather_agro_data' vérifiée/créée avec succès !")

# Vérifier et créer les colonnes manquantes
def ensure_columns_exist(engine, table_name, df: pd.DataFrame):
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT column_name FROM information_schema.columns WHERE table_name = :table"),
            {"table": table_name}
        )
        existing_columns = {row[0] for row in result.fetchall()}
        
        for col, dtype in zip(df.columns, df.dtypes):
            if col not in existing_columns:
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "VARCHAR(255)"
                alter_sql = text(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {sql_type};')
                conn.execute(alter_sql)
                print(f"Colonne ajoutée : {col} ({sql_type})")

# Charger un DataFrame dans PostgreSQL
def load_to_postgres(df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
    engine = connect_postgres()
    if engine is None:
        print("Chargement annulé : pas de connexion à la base.")
        return
    
    # Créer la table si elle n'existe pas
    create_table_if_not_exists(engine)

    # Vérifier et ajouter les colonnes manquantes
    ensure_columns_exist(engine, table_name, df)

    # Charger les données
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"Données insérées avec succès dans la table '{table_name}' !")
    except Exception as e:
        print("Erreur lors du chargement des données :", e)
