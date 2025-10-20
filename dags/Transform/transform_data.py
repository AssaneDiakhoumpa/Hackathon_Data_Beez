import pandas as pd
import numpy as np
import logging

def transform(weather_df, fao_df):
    """
    Transformation et fusion des données météo et FAO uniquement.
    """

    logging.info("Démarrage de la fonction transform...")

    #Copie des DataFrames pour éviter les effets de bord
    weather = weather_df.copy()
    fao = fao_df.copy()

    #Normalisation des noms de colonnes
    # Harmonisation FAO
    if 'annee' in fao.columns:
        fao = fao.rename(columns={'annee': 'date'})
        fao['date'] = pd.to_datetime(fao['date'], format='%Y', errors='coerce')

    if 'REGION' in fao.columns:
        fao = fao.rename(columns={'REGION': 'region'})

    # Harmonisation WEATHER
    if 'REGION' in weather.columns:
        weather = weather.rename(columns={'REGION': 'region'})
    if 'date' in weather.columns:
        weather['date'] = pd.to_datetime(weather['date'], errors='coerce')

    logging.info("Colonnes harmonisées.")

    #Vérification des colonnes clés avant fusion
    for name, df in [('weather', weather), ('fao', fao)]:
        if 'region' not in df.columns:
            raise KeyError(f"Colonne 'region' manquante dans {name}")
        if 'date' not in df.columns:
            raise KeyError(f"Colonne 'date' manquante dans {name}")

    #Fusion progressive
    merge_keys = ['region']
    logging.info("Fusion weather + FAO...")
    merged = weather.merge(fao, on=merge_keys, how='left')

    #Nettoyage et enrichissement
    merged = merged.drop_duplicates()

    # Remplacement des valeurs manquantes numériques par la moyenne de la colonne
    for col in merged.select_dtypes(include=[np.number]).columns:
        merged[col] = merged[col].fillna(merged[col].mean())

    # Ajout éventuel d’indicateurs dérivés
    if 'temperature_2m_max' in merged.columns and 'temperature_2m_min' in merged.columns:
        merged['temp_moy'] = (merged['temperature_2m_max'] + merged['temperature_2m_min']) / 2

    logging.info(f"Fusion terminée. Shape finale : {merged.shape}")

    return merged
