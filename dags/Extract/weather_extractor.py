import requests
import pandas as pd
import os

def get_weather_data(lat, lon):
    """Récupère les données météo pour une position (lat, lon)"""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,precipitation_sum,relative_humidity_2m_max"
        f"&timezone=Africa%2FDakar"
    )
    response = requests.get(url)
    response.raise_for_status()  # sécurise contre les erreurs réseau
    data = response.json()

    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temperature_c": data["daily"]["temperature_2m_max"],
        "precipitation_mm": data["daily"]["precipitation_sum"],
        "humidity_percent": data["daily"]["relative_humidity_2m_max"],
    })
    return df


def get_weather_for_south_regions():
    """Récupère les données météo par région, sauvegarde en CSV et fusionne"""
    regions = {
        "Ziguinchor": (12.5833, -16.2667),
        "Kolda": (12.8833, -14.9500),
        "Sédhiou": (12.7083, -15.5569)
    }

    out_dir = "/tmp/data/weather_regions"
    os.makedirs(out_dir, exist_ok=True)

    all_csvs = []
    for region, (lat, lon) in regions.items():
        print(f"📡 Récupération des données pour {region}...")
        try:
            df = get_weather_data(lat, lon)
            df["region"] = region

            # Sauvegarde CSV pour éviter de garder tout en mémoire
            file_path = os.path.join(out_dir, f"{region}.csv")
            df.to_csv(file_path, index=False)
            all_csvs.append(file_path)

            print(f"✅ {region} sauvegardé ({len(df)} lignes)")
        except Exception as e:
            print(f"⚠️ Erreur pour {region} : {e}")

    # Fusion incrémentale des CSV régionaux
    merged_path = "/tmp/data/weather.csv"
    first = True

    with open(merged_path, "w", encoding="utf-8") as outfile:
        for file_path in all_csvs:
            df = pd.read_csv(file_path)
            if first:
                df.to_csv(outfile, index=False)
                first = False
            else:
                df.to_csv(outfile, index=False, header=False)
    
    print(f"✅ Fusion finale enregistrée (mode append): {merged_path}")
    return pd.read_csv(merged_path)

if __name__ == "__main__":
    df_weather = get_weather_for_south_regions()
    print(df_weather.head())
