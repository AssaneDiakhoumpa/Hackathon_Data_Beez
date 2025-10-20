import pandas as pd
import pathlib
import unicodedata

def clean_column_names(df):
    """Nettoie et normalise les noms de colonnes."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .map(lambda x: ''.join(
            c for c in unicodedata.normalize('NFD', x)
            if unicodedata.category(c) != 'Mn'
        ))
    )
    return df


def get_fao_data(file_path='data/EAA_2017_2022_T3_Superficie, Rendement et Production agricoles.xlsx'):
    """
    Lit un seul fichier FAO Excel depuis ./data et nettoie les colonnes.
    """
    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    try:
        # Lecture du fichier Excel
        df = pd.read_excel(path)
        df = clean_column_names(df)

        # Recherche et normalisation de la colonne "region"
        possible_cols = [c for c in df.columns if 'region' in c]
        if possible_cols:
            df.rename(columns={possible_cols[0]: 'region'}, inplace=True)
        else:
            print(f"Aucune colonne 'region' détectée. Colonnes: {df.columns.tolist()}")

        print(f"Fichier lu : {path.name} ({len(df)} lignes, {len(df.columns)} colonnes)")
        return df

    except Exception as e:
        raise RuntimeError(f"Erreur lecture fichier FAO {path.name} : {e}")