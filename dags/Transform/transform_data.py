import pandas as pd
import numpy as np
import logging

def transform(weather_df, fao_df):
    """
    Transformation et fusion des données météo et FAO par département uniquement.
    """

    logging.info("Démarrage de la fonction transform...")

    #Copie pour éviter les effets de bord
    weather = weather_df.copy()
    fao = fao_df.copy()

    #Normalisation des noms de colonnes FAO
    fao = fao.rename(columns={
        'Annee': 'annee',
        'REGION': 'region',
        'Département': 'departement',
        'Culture': 'culture',
        'Indicateur': 'indicateur',
        'Valeur': 'valeur'
    })

    #Normalisation des noms de colonnes météo
    if 'departement' not in weather.columns:
        raise KeyError("Colonne 'departement' manquante dans le DataFrame météo")

    #Agrégation météo par département (moyenne sur toute la période)
    weather_agg = weather.groupby('departement', as_index=False).agg({
        'temp': 'mean',
        'feels_like': 'mean',
        'humidity': 'mean',
        'pressure': 'mean',
        'wind_speed': 'mean'
    })
    logging.info("Données météo agrégées par département.")

    #Agrégation FAO par département (moyenne des valeurs numériques)
    fao['valeur'] = pd.to_numeric(fao['valeur'], errors='coerce')
    fao_agg = fao.groupby('departement', as_index=False).agg({'valeur': 'mean'})
    logging.info("Données FAO agrégées par département.")

    #Fusion finale par département
    merged = pd.merge(weather_agg, fao_agg, on='departement', how='left')

    #Remplissage des valeurs manquantes
    for col in merged.select_dtypes(include=[np.number]).columns:
        merged[col] = merged[col].fillna(merged[col].mean())

    # #Calculs dérivés éventuels
    # merged['temp_moy'] = (merged['temp_min'] + merged['temp_max']) / 2
    # merged['temp_moy'] = merged['temp']

    #Nettoyage final
    merged = merged.drop_duplicates()

    logging.info(f"Fusion terminée. Shape finale : {merged.shape}")

    return merged

