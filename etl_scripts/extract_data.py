from dotenv import load_dotenv
import pandas as pd
import json
import os
import requests
from datetime import datetime

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
if not AIRFLOW_HOME:
    raise EnvironmentError("La variable d'environnement AIRFLOW_HOME n'est pas définie. "
                           "Veuillez l'exporter : export AIRFLOW_HOME=~/weather_dashboard_project")

RAW_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'processed')

# Assurez-vous que les dossiers existent
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)


# --- Fonctions d'Extraction ---

def extract_json_data(file_name: str = "all_capitals_weather.json") -> pd.DataFrame:
    """
    Extrait les données météorologiques actuelles des capitales à partir d'un fichier JSON local.
    :param file_name: Nom du fichier JSON dans le dossier data/raw.
    :return: DataFrame pandas des données JSON.
    """
    json_file_path = os.path.join(RAW_DATA_PATH, file_name)
    print(f"Tentative de lecture du fichier JSON : {json_file_path}")

    if not os.path.exists(json_file_path):
        print(f"Erreur : Le fichier {json_file_path} n'existe pas. "
              f"Veuillez vous assurer qu'il est bien placé dans '{RAW_DATA_PATH}'.")
        return pd.DataFrame()

    try:
        df = pd.read_json(json_file_path)
        print(f"Fichier JSON '{file_name}' lu avec succès. Nombre de lignes initiales : {len(df)}")
        return df
    except Exception as e:
        print(f"Erreur lors de la lecture du JSON '{file_name}' : {e}")
        print("Vérifiez le format du JSON ou la quantité de mémoire disponible si le fichier est très grand.")
        return pd.DataFrame()


def extract_openweather_data(city_coords: dict, api_key: str) -> pd.DataFrame:
    """
    Extrait les données météorologiques actuelles de l'API OpenWeatherMap pour une liste de villes.
    :param city_coords: Dictionnaire {nom_ville: {'lat': lat, 'lon': lon}} des villes à interroger.
    :param api_key: Clé API OpenWeatherMap.
    :return: DataFrame pandas des données météorologiques actuelles.
    """
    all_current_weather_data = []
    base_url = "https://api.openweathermap.org/data/2.5/weather"

    print(f"Extraction des données OpenWeather pour {len(city_coords)} villes...")

    for city_name, coords in city_coords.items():
        params = {
            'lat': coords['lat'],
            'lon': coords['lon'],
            'appid': api_key,
            'units': 'metric'  # Températures en Celsius
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Lève une HTTPError pour les codes d'état d'erreur (4xx ou 5xx)
            data = response.json()

            # Extraction et normalisation des champs pertinents
            extracted_data = {
                'location_name': city_name,
                'latitude': coords['lat'],
                'longitude': coords['lon'],
                'last_updated': pd.to_datetime(data['dt'], unit='s', utc=True).tz_convert(data.get('timezone')), # Convertir timestamp Unix en datetime, avec fuseau horaire
                'temperature_celsius': data['main']['temp'],
                'feels_like_celsius': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure_mb': data['main']['pressure'],
                'wind_kph': data['wind']['speed'] * 3.6,  # Convertir m/s en km/h
                'condition_text': data['weather'][0]['description'],
                'cloud': data['clouds']['all'],
                'visibility_km': data.get('visibility', 0) / 1000,  # Visibilité en km, gère l'absence
                'precip_mm': data.get('rain', {}).get('1h', 0)  # Précipitations sur la dernière heure, gère l'absence
            }
            all_current_weather_data.append(extracted_data)
            print(f"-> Données OpenWeather pour '{city_name}' extraites avec succès.")

        except requests.exceptions.RequestException as e:
            print(f"Erreur HTTP/Connexion lors de l'appel API OpenWeather pour '{city_name}': {e}")
        except KeyError as e:
            print(f"Erreur de clé dans la réponse OpenWeather pour '{city_name}': champ manquant '{e}'.")
            print(f"Réponse API (partielle pour debug) : {data}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue pour '{city_name}': {e}")

    return pd.DataFrame(all_current_weather_data)


def extract_historical_data(file_name: str) -> pd.DataFrame:
    """
    Extrait les données météorologiques historiques depuis un fichier CSV local.
    :param file_name: Nom du fichier CSV dans le dossier data/raw.
    :return: DataFrame pandas des données historiques.
    """
    historical_file_path = os.path.join(RAW_DATA_PATH, file_name)
    print(f"Extraction des données historiques depuis : {historical_file_path}")

    if not os.path.exists(historical_file_path):
        print(f"Erreur : Le fichier historique {historical_file_path} n'a pas été trouvé. "
              f"Veuillez vous assurer qu'il est bien placé dans '{RAW_DATA_PATH}'.")
        return pd.DataFrame()

    try:
        # Assurez-vous d'adapter ces paramètres à la structure réelle de votre CSV historique
        df_historical = pd.read_csv(historical_file_path)
        print(f"Données historiques de '{file_name}' lues avec succès. Nombre de lignes : {len(df_historical)}")
        return df_historical
    except Exception as e:
        print(f"Erreur lors de la lecture des données historiques '{file_name}': {e}")
        return pd.DataFrame()


def get_selected_city_coords(df_json: pd.DataFrame, selected_city_names: list) -> dict:
    """
    Extrait les latitudes et longitudes pour les villes sélectionnées à partir du DataFrame JSON.
    Gère les doublons potentiels et les noms de villes manquants.
    :param df_json: DataFrame contenant toutes les données JSON des capitales.
    :param selected_city_names: Liste des noms de villes à extraire (ex: "Paris", "London").
    :return: Dictionnaire {nom_ville: {'lat': lat, 'lon': lon}} pour les villes trouvées.
    """
    city_coords = {}
    print(f"\nRecherche des coordonnées pour les villes sélectionnées : {selected_city_names}")
    for city_name in selected_city_names:
        # Utilise .str.lower() pour une recherche insensible à la casse
        # Utilise .str.strip() pour supprimer les espaces blancs en trop
        # Utilise df_json['location_name'].astype(str) pour éviter les erreurs si la colonne contient des non-strings
        city_data = df_json[df_json['location_name'].astype(str).str.lower().str.strip() == city_name.lower().strip()]

        if not city_data.empty:
            # Si plusieurs entrées pour la même ville (rare mais possible avec des homonymes), prenez la première
            # Assurez-vous que latitude et longitude sont convertis en float pour l'API
            lat = float(city_data['latitude'].iloc[0])
            lon = float(city_data['longitude'].iloc[0])
            city_coords[city_name] = {'lat': lat, 'lon': lon}
            print(f"  - Coordonnées trouvées pour '{city_name}': Lat={lat}, Lon={lon}")
        else:
            print(f"  - Avertissement : Coordonnées non trouvées pour la ville '{city_name}' dans le dataset JSON.")
    return city_coords


# --- Bloc de test (pour exécution directe du script) ---
if __name__ == "__main__":
    print("--- Démarrage des tests d'extraction ---")

    # --- Étape 1: Extraction et traitement du fichier JSON principal ---
    print("\n[TEST] Extraction du fichier JSON 'all_capitals_weather.json'...")
    df_json_raw = extract_json_data("all_capitals_weather.json")

    if df_json_raw.empty:
        print("Impossible de continuer les tests sans les données JSON initiales.")
        exit() # Quitte le script si l'extraction JSON échoue

    print(f"Aperçu des données JSON brutes :\n{df_json_raw.head()}")
    print(f"Colonnes JSON brutes : {df_json_raw.columns.tolist()[:5]}...") # Affiche les 5 premières colonnes


    # --- Étape 2: Sélection des villes pour l'API OpenWeather et l'historique ---
    # Définissez ici la liste des villes que vous souhaitez analyser.
    # Essayez de choisir des villes avec des climats variés !
    target_cities = [
        "London", "New York", "Tokyo", "Paris", "Berlin", "Sydney",
        "Rio de Janeiro", "Cairo", "Moscow", "Dubai", "Beijing",
        "Rome", "Madrid", "Mexico City", "Buenos Aires", "Cape Town",
        "New Delhi", "Singapore", "Oslo", "Washington"
    ]
    city_coords_for_api = get_selected_city_coords(df_json_raw, target_cities)

    # Assurez-vous d'avoir des coordonnées pour au moins quelques villes avant de faire des appels API
    if not city_coords_for_api:
        print("Aucune coordonnée de ville trouvée pour les appels OpenWeather API. Vérifiez la liste 'target_cities'.")
    else:
        # --- Étape 3: Extraction des données en temps réel via OpenWeather API ---
        print("\n[TEST] Extraction des données en temps réel via OpenWeather API...")
        # REMPLACEZ 'VOTRE_CLE_API' PAR VOTRE VRAIE CLÉ OPENWEATHERMAP
        # Pour des raisons de sécurité, en production, cette clé devrait être une variable d'environnement ou gérée par un secret manager.
        openweather_api_key = os.getenv("OPENWEATHER_API_KEY")

        df_openweather = extract_openweather_data(city_coords_for_api, openweather_api_key)
        if not df_openweather.empty:
            print(f"\nAperçu des données OpenWeather extraites :\n{df_openweather.head()}")
            print(f"Colonnes OpenWeather : {df_openweather.columns.tolist()}")
        else:
            print("Aucune donnée OpenWeather n'a été extraite.")

    # --- Étape 4: Test de l'extraction des données historiques (avec un fichier CSV de test) ---
    print("\n[TEST] Extraction des données historiques (avec un fichier de test 'historical_test.csv')...")
    # Créons un petit fichier CSV factice pour le test si ce n'est pas déjà fait
    temp_historical_file_name = "historical_test.csv"
    temp_historical_file_path = os.path.join(RAW_DATA_PATH, temp_historical_file_name)

    if not os.path.exists(temp_historical_file_path):
        print(f"Création d'un fichier de test historique '{temp_historical_file_name}' pour le test...")
        with open(temp_historical_file_path, 'w') as f:
            f.write("Date,City,Temperature_Celsius,Precipitation_mm\n")
            f.write("2023-01-01,London,5.0,0.5\n")
            f.write("2023-01-01,New York,-2.0,1.2\n")
            f.write("2023-01-02,London,6.0,0.0\n")
            f.write("2023-01-02,New York,-1.0,0.0\n")
            f.write("2023-01-03,Tokyo,10.0,0.0\n")
            f.write("2023-01-03,Paris,7.5,2.1\n")
        print(f"Fichier de test historique '{temp_historical_file_name}' créé avec succès.")

    df_historical_test = extract_historical_data(temp_historical_file_name)
    if not df_historical_test.empty:
        print(f"\nAperçu des données historiques extraites :\n{df_historical_test.head()}")
        print(f"Colonnes historiques : {df_historical_test.columns.tolist()}")
    else:
        print("Aucune donnée historique n'a été extraite.")

    print("\n--- Fin des tests d'extraction ---")