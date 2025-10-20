# python
import re
import pandas as pd
from shapely.geometry import box

# bbox approximatif du Sénégal (min_lon, min_lat, max_lon, max_lat)
SEN_BBOX = (-17.61, 12.54, -11.35, 16.68)
sen_box = box(*SEN_BBOX)

import pathlib

def get_copernicus_data(*args, data_dir=None, **kwargs):
    """
    Wrapper de compatibilité pour le DAG.
    - Si une fonction alternative existe dans ce module (get_copernicus, load_copernicus, ...),
      l'appelle en priorité.
    - Sinon lit le CSV monté dans /opt/airflow/data_corpinius (ou data_dir si fourni).
    """
    # tenter noms alternatifs définis dans ce module
    for alt in ('get_copernicus_data', 'get_copernicus', 'load_copernicus', 'read_copernicus', 'read_copernicus_data'):
        fn = globals().get(alt)
        if callable(fn) and fn is not get_copernicus_data:
            return fn(*args, **kwargs)

    # fallback : lire le CSV dans le conteneur Airflow
    base = pathlib.Path(data_dir) if data_dir else pathlib.Path("/opt/airflow/data_corpinius")
    path = base / "swi_global_12.5km_10daily_v3_cog.csv"
    if not path.exists():
        # essayer chemin relatif au projet si monté différemment
        path_rel = pathlib.Path(__file__).resolve().parents[1] / "data_corpinius" / "swi_global_12.5km_10daily_v3_cog.csv"
        if path_rel.exists():
            path = path_rel
        else:
            raise FileNotFoundError(
                f"Copernicus CSV introuvable: cherché {base} et {path_rel}.\n"
                "Vérifier le montage du dossier data_corpinius dans le conteneur Airflow."
            )

    return pd.read_csv(path, sep=';', **kwargs)
# ...existing code...

def parse_wkt_bbox(wkt):
    # extrait les coordonnées depuis "POLYGON((lon lat,lon lat,...))"
    nums = re.findall(r'-?\d+\.?\d*', wkt)
    pts = [(float(nums[i]), float(nums[i+1])) for i in range(0, len(nums), 2)]
    lons = [p[0] for p in pts]; lats = [p[1] for p in pts]
    return min(lons), min(lats), max(lons), max(lats)

df = pd.read_csv("data_corpinius/swi_global_12.5km_10daily_v3_cog.csv",
                 sep=';', parse_dates=[
                     "ingestion_date","content_date_start","content_date_end",
                     "nominal_date","modification_date"
                 ])

# ajouter colonnes min_lon,min_lat,max_lon,max_lat et flag d'intersection
coords = df['bbox'].apply(parse_wkt_bbox)
df[['min_lon','min_lat','max_lon','max_lat']] = pd.DataFrame(coords.tolist(), index=df.index)

def intersects_sen(row):
    tile = box(row.min_lon, row.min_lat, row.max_lon, row.max_lat)
    return tile.intersects(sen_box)

df_sen = df[df.apply(intersects_sen, axis=1)].reset_index(drop=True)

# colonnes utiles
print(df_sen[['name','s3_path','content_date_start','content_date_end','content_length']])