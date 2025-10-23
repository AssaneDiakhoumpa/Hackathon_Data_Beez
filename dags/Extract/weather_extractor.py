import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Charger la clé API
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not API_KEY:
    raise ValueError("Clé API OpenWeather manquante dans le fichier .env")

def get_current_weather(lat, lon):
    """
    Récupère la météo actuelle via OpenWeather API (gratuite)
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    record = {
        "date": pd.Timestamp.now(),
        "temp": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "wind_speed": data["wind"]["speed"],
        "weather": data["weather"][0]["description"],
    }

    return pd.DataFrame([record])

def get_weather_for_all_departments():
    """
    Récupère la météo actuelle pour tous les départements du Sénégal.
    """
    departments = {
        "Dakar": (14.7167, -17.4677), "Guédiawaye": (14.7843, -17.3741),
        "Pikine": (14.75, -17.4), "Rufisque": (14.7167, -17.2667),
        "Thiès": (14.791, -16.925), "Mbour": (14.419, -16.964),
        "Tivaouane": (15.15, -16.8667), "Diourbel": (14.65, -16.233),
        "Bambey": (14.7, -16.45), "Mbacké": (14.790, -15.908),
        "Saint-Louis": (16.0179, -16.4896), "Dagana": (16.517, -15.5),
        "Podor": (16.65, -14.9667), "Louga": (15.6167, -16.2167),
        "Kébémer": (15.483, -16.45), "Linguère": (15.383, -15.1167),
        "Matam": (15.655, -13.255), "Kanel": (15.489, -13.176),
        "Ranérou": (15.3, -13.966), "Kaolack": (14.15, -16.1),
        "Guinguinéo": (14.266, -15.95), "Nioro du Rip": (13.75, -15.8),
        "Kaffrine": (14.1, -15.55), "Birkilane": (14.5, -15.6),
        "Koungheul": (13.983, -14.783), "Malem Hodar": (14.15, -15.25),
        "Fatick": (14.333, -16.416), "Foundiougne": (14.133, -16.47),
        "Gossas": (14.48, -16.27), "Kolda": (12.8833, -14.95),
        "Vélingara": (13.15, -14.1167), "Médina Yoro Foulah": (13.38, -14.27),
        "Sédhiou": (12.7083, -15.5569), "Bounkiling": (12.78, -15.43),
        "Goudomp": (12.5667, -15.6833), "Ziguinchor": (12.5833, -16.2667),
        "Bignona": (12.8, -16.2333), "Oussouye": (12.485, -16.546),
        "Tambacounda": (13.7667, -13.6833), "Bakel": (14.9, -12.4667),
        "Goudiry": (14.18, -12.7), "Koumpentoum": (13.97, -13.35)
    }

    all_records = []
    for dept, (lat, lon) in departments.items():
        print(f"Récupération de la météo pour {dept}...")
        try:
            df = get_current_weather(lat, lon)
            df["departement"] = dept
            all_records.append(df)
        except Exception as e:
            print(f"Erreur pour {dept} : {e}")

    if not all_records:
        raise ValueError("Aucune donnée récupérée pour les départements.")

    merged_df = pd.concat(all_records, ignore_index=True)
    
    merged_df.to_csv("/tmp/data/weather_departments_current.csv", index=False)
    print("Données météo sauvegardées dans /tmp/data/weather_departments_current.csv")

    return merged_df


if __name__ == "__main__":
    df_weather = get_weather_for_all_departments()
    print(df_weather.head())
