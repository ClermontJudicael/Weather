import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- Configuration des chemins ---
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
if not AIRFLOW_HOME:
    raise EnvironmentError("La variable d'environnement AIRFLOW_HOME n'est pas définie.")

RAW_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'raw')
PROCESSED_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'processed')

os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)


def clean_and_transform_data(df_json: pd.DataFrame, df_openweather: pd.DataFrame, df_historical: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie, transforme et unifie les données météorologiques provenant de différentes sources.
    :param df_json: DataFrame des données extraites du JSON initial.
    :param df_openweather: DataFrame des données extraites de l'API OpenWeather.
    :param df_historical: DataFrame des données historiques extraites (CSV).
    :return: DataFrame unifié et nettoyé.
    """
    print("\n--- Début de la transformation des données ---")

    # --- 1. Traitement du DataFrame JSON initial ---
    print("Traitement du DataFrame JSON...")
    json_cols_mapping = {
        'location_name': 'city',
        'country': 'country',
        'last_updated': 'date',
        'temperature_celsius': 'temp_celsius',
        'feels_like_celsius': 'feels_like_celsius',
        'humidity': 'humidity_percent',
        'pressure_mb': 'pressure_mb',
        'wind_kph': 'wind_kph',
        'precip_mm': 'precipitation_mm',
        'cloud': 'cloud_percent',
        'visibility_km': 'visibility_km',
        'uv_index': 'uv_index',
        'condition_text': 'weather_condition',
        'latitude': 'latitude',
        'longitude': 'longitude',
    }

    df_json_transformed = pd.DataFrame() # Initialiser comme vide par sécurité

    if not df_json.empty:
        # S'assurer que toutes les colonnes à mapper existent avant de les sélectionner
        cols_to_select_json = [col for col in json_cols_mapping.keys() if col in df_json.columns]

        # Avertissement si des colonnes sont manquantes
        missing_json_cols = [col for col in json_cols_mapping.keys() if col not in df_json.columns]
        if missing_json_cols:
            print(f"AVERTISSEMENT : Colonnes JSON attendues mais manquantes dans le DataFrame source : {missing_json_cols}")
            print("Vérifiez le fichier 'all_capitals_weather.json' pour ces colonnes.")

        if cols_to_select_json: # Ne procéder que si nous avons des colonnes à sélectionner
            # Filtrer le mapping pour ne garder que les colonnes qui existent
            effective_json_mapping = {k: v for k, v in json_cols_mapping.items() if k in df_json.columns}
            
            df_json_transformed = df_json[list(effective_json_mapping.keys())].rename(columns=effective_json_mapping)
            df_json_transformed['source'] = 'json_initial'
            df_json_transformed['date'] = pd.to_datetime(df_json_transformed['date'], errors='coerce').dt.floor('D')
            df_json_transformed.dropna(subset=['date'], inplace=True)
            print(f"DataFrame JSON transformé. Colonnes: {df_json_transformed.columns.tolist()}")
        else:
            print("Aucune colonne pertinente trouvée dans le DataFrame JSON ou DataFrame JSON vide. Aucune transformation JSON effectuée.")
    else:
        print("DataFrame JSON d'entrée est vide. Aucune transformation effectuée pour JSON.")


    # --- 2. Traitement du DataFrame OpenWeather API (Données en temps réel) ---
    print("Traitement du DataFrame OpenWeather...")
    df_openweather_transformed = pd.DataFrame() # Initialiser comme vide par sécurité

    if not df_openweather.empty:
        openweather_cols_mapping = {
            'location_name': 'city',
            'last_updated': 'date',
            'temperature_celsius': 'temp_celsius',
            'feels_like_celsius': 'feels_like_celsius',
            'humidity': 'humidity_percent',
            'pressure_mb': 'pressure_mb',
            'wind_kph': 'wind_kph',
            'precip_mm': 'precipitation_mm',
            'cloud': 'cloud_percent',
            'visibility_km': 'visibility_km',
            'condition_text': 'weather_condition',
            'latitude': 'latitude',
            'longitude': 'longitude',
        }
        
        cols_to_select = [col for col in openweather_cols_mapping.keys() if col in df_openweather.columns]
        
        missing_ow_cols = [col for col in openweather_cols_mapping.keys() if col not in df_openweather.columns]
        if missing_ow_cols:
            print(f"AVERTISSEMENT : Colonnes OpenWeather attendues mais manquantes dans le DataFrame source : {missing_ow_cols}")

        if cols_to_select:
            df_openweather_transformed = df_openweather[cols_to_select].rename(columns=openweather_cols_mapping)
            df_openweather_transformed['source'] = 'openweather_api'

            df_openweather_transformed['date'] = pd.to_datetime(df_openweather_transformed['date'], errors='coerce')
            df_openweather_transformed['date'] = df_openweather_transformed['date'].dt.floor('D')
            
            df_openweather_transformed.dropna(subset=['date'], inplace=True)
            
            print(f"DataFrame OpenWeather transformé. Colonnes: {df_openweather_transformed.columns.tolist()}")
        else:
            print("Aucune colonne pertinente trouvée dans le DataFrame OpenWeather ou DataFrame OpenWeather vide. Aucune transformation OpenWeather effectuée.")
    else:
        print("DataFrame OpenWeather d'entrée est vide. Aucune transformation effectuée pour OpenWeather.")


    # --- 3. Traitement du DataFrame Historique (CSV) ---
    print("Traitement du DataFrame Historique...")
    historical_cols_mapping = {
        'Date': 'date',
        'City': 'city',
        'Temperature_Celsius': 'temp_celsius',
        'Precipitation_mm': 'precipitation_mm'
    }
    missing_cols_hist = [col for col in historical_cols_mapping.keys() if col not in df_historical.columns]
    if missing_cols_hist:
        print(f"AVERTISSEMENT : Colonnes historiques manquantes dans le DataFrame historique : {missing_cols_hist}")
        print("Cela signifie que le mapping 'historical_cols_mapping' doit être ajusté pour correspondre à vos vraies données historiques.")
        historical_cols_mapping = {k: v for k, v in historical_cols_mapping.items() if k in df_historical.columns}

    df_historical_transformed = pd.DataFrame() # Initialiser comme vide

    if not df_historical.empty and historical_cols_mapping:
        df_historical_transformed = df_historical[list(historical_cols_mapping.keys())].rename(columns=historical_cols_mapping)
        df_historical_transformed['source'] = 'historical_csv'
        df_historical_transformed['date'] = pd.to_datetime(df_historical_transformed['date'], errors='coerce').dt.floor('D')
        df_historical_transformed.dropna(subset=['date'], inplace=True)

        common_cols = [
            'city', 'date', 'temp_celsius', 'precipitation_mm',
            'feels_like_celsius', 'humidity_percent', 'pressure_mb', 'wind_kph',
            'cloud_percent', 'visibility_km', 'uv_index', 'weather_condition',
            'latitude', 'longitude', 'country'
        ]
        for col in common_cols:
            if col not in df_historical_transformed.columns:
                df_historical_transformed[col] = np.nan
        print(f"DataFrame Historique transformé. Colonnes: {df_historical_transformed.columns.tolist()}")
    else:
        print("DataFrame Historique d'entrée est vide ou pas de colonnes mappées. Aucune transformation effectuée pour l'historique.")


    # --- 4. Fusion des DataFrames ---
    print("Fusion des DataFrames...")

    all_dfs = [df for df in [df_json_transformed, df_openweather_transformed, df_historical_transformed] if not df.empty]
    
    if not all_dfs:
        print("Aucun DataFrame à fusionner après transformation. Le DataFrame unifié sera vide.")
        return pd.DataFrame()

    common_columns_set = set()
    for df in all_dfs:
        common_columns_set.update(df.columns)

    for i, df in enumerate(all_dfs):
        for col in common_columns_set:
            if col not in df.columns:
                all_dfs[i][col] = np.nan
        all_dfs[i] = all_dfs[i].reindex(columns=list(common_columns_set))


    final_columns_order = [
        'city', 'date', 'source', 'country', 'latitude', 'longitude',
        'temp_celsius', 'feels_like_celsius', 'humidity_percent',
        'pressure_mb', 'wind_kph', 'precipitation_mm', 'cloud_percent',
        'visibility_km', 'uv_index', 'weather_condition'
    ]
    final_columns_effective_order = [col for col in final_columns_order if col in common_columns_set]
    for col in common_columns_set:
        if col not in final_columns_effective_order:
            final_columns_effective_order.append(col)


    df_unified = pd.concat(all_dfs, ignore_index=True)
    df_unified = df_unified[final_columns_effective_order]

    # --- 5. Enrichissement des données : Ajouter le pays et les coordonnées manquantes ---
    print("Enrichissement : Ajout des infos de pays et coordonnées manquantes...")
    city_info_from_json = df_json_transformed[['city', 'country', 'latitude', 'longitude']].drop_duplicates(subset=['city'])
    df_unified = pd.merge(df_unified, city_info_from_json, on='city', how='left', suffixes=('', '_json'))

    df_unified['country'] = df_unified['country'].combine_first(df_unified['country_json'])
    df_unified['latitude'] = df_unified['latitude'].combine_first(df_unified['latitude_json'])
    df_unified['longitude'] = df_unified['longitude'].combine_first(df_unified['longitude_json'])

    df_unified = df_unified.drop(columns=['country_json', 'latitude_json', 'longitude_json'], errors='ignore')

    # --- 6. Nettoyage final et typage ---
    print("Nettoyage final et conversion des types...")
    df_unified.dropna(subset=['city', 'date'], inplace=True)

    df_unified['date'] = pd.to_datetime(df_unified['date'], errors='coerce')
    
    numeric_cols = [
        'temp_celsius', 'feels_like_celsius', 'humidity_percent',
        'pressure_mb', 'wind_kph', 'precipitation_mm', 'cloud_percent',
        'visibility_km', 'uv_index', 'latitude', 'longitude'
    ]
    for col in numeric_cols:
        df_unified[col] = pd.to_numeric(df_unified[col], errors='coerce')
        if col in ['precipitation_mm', 'uv_index']:
            df_unified[col] = df_unified[col].fillna(0)
        else:
            if not df_unified[col].isnull().all():
                df_unified[col] = df_unified[col].fillna(df_unified[col].median())
            else:
                df_unified[col] = df_unified[col].fillna(0)

    df_unified['weather_condition'] = df_unified['weather_condition'].fillna('unknown').astype(str)
    df_unified['city'] = df_unified['city'].astype(str)
    df_unified['country'] = df_unified['country'].fillna('unknown').astype(str)
    df_unified['source'] = df_unified['source'].astype(str)

    # --- 7. Calcul d'indicateurs supplémentaires ---
    print("Calcul d'indicateurs supplémentaires...")
    df_unified['is_rainy_day'] = (df_unified['precipitation_mm'] > 0.1).astype(int)

    print(f"Transformation terminée. Taille du DataFrame unifié : {df_unified.shape}")
    print(f"Aperçu du DataFrame unifié :\n{df_unified.head()}")
    return df_unified

# --- Nouvelle fonction de chargement ---
def load_data(df: pd.DataFrame, filename: str = "transformed_weather_data.parquet"):
    """
    Charge le DataFrame transformé dans un fichier Parquet dans le dossier processed.
    :param df: Le DataFrame à charger.
    :param filename: Le nom du fichier Parquet.
    """
    if df.empty:
        print("Le DataFrame est vide, aucun fichier ne sera chargé.")
        return

    output_path = os.path.join(PROCESSED_DATA_PATH, filename)
    try:
        df.to_parquet(output_path, index=False)
        print(f"\nDonnées transformées chargées avec succès dans : {output_path}")
        print(f"Nombre de lignes chargées : {len(df)}")
    except Exception as e:
        print(f"Erreur lors du chargement des données vers Parquet : {e}")


# --- Bloc de test (pour exécution directe du script) ---
if __name__ == "__main__":
    print("--- Démarrage des tests de transformation ---")

    from extract_data import extract_json_data, extract_openweather_data, extract_historical_data, get_selected_city_coords, RAW_DATA_PATH

    print("\n[TEST PREP] Chargement des données brutes pour la transformation...")
    df_json_raw_test = extract_json_data("all_capitals_weather.json")

    test_target_cities = [
        "London", "New York", "Tokyo", "Paris", "Berlin", "Sydney",
        "Rio de Janeiro", "Cairo", "Moscow", "Dubai", "Beijing",
        "Rome", "Madrid", "Mexico City", "Buenos Aires", "Cape Town",
        "New Delhi", "Singapore", "Oslo", "Washington"
    ]
    test_city_coords = get_selected_city_coords(df_json_raw_test, test_target_cities)

    test_openweather_api_key = os.getenv("OPENWEATHER_API_KEY", "a00629f7d3cd8fb3f58f8c3cd15b19a1")
    if test_openweather_api_key == "VOTRE_CLE_API":
        print("ATTENTION: Clé OpenWeather API non définie. Les appels API réels ne seront pas effectués.")
        df_openweather_test = pd.DataFrame()
    else:
        df_openweather_test = extract_openweather_data(test_city_coords, test_openweather_api_key)

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


    df_transformed = clean_and_transform_data(
        df_json=df_json_raw_test,
        df_openweather=df_openweather_test,
        df_historical=df_historical_test
    )

    if not df_transformed.empty:
        print("\n[TEST RESULTATS] DataFrame final après transformation :")
        print(f"Forme (lignes, colonnes): {df_transformed.shape}")
        print("Types de données des colonnes principales :")
        print(df_transformed[['city', 'date', 'temp_celsius', 'precipitation_mm', 'is_rainy_day', 'source']].info())
        print("\nQuelques lignes du DataFrame transformé :")
        print(df_transformed.head())
        print("\nStatistiques descriptives pour les colonnes numériques :")
        print(df_transformed.describe())
        print(f"\nNombre de valeurs uniques par ville : {df_transformed['city'].nunique()}")
        print(f"Sources de données présentes : {df_transformed['source'].unique()}")

        if 'city' not in df_transformed.columns or \
           'date' not in df_transformed.columns or \
           'temp_celsius' not in df_transformed.columns:
            print("\nAVERTISSEMENT : Les colonnes critiques (city, date, temp_celsius) sont manquantes après transformation.")
            print("Vérifiez la logique de mapping et de fusion.")

        # --- NOUVELLE ÉTAPE : Chargement des données transformées ---
        load_data(df_transformed) # Appel de la fonction de chargement
    else:
        print("\n[TEST RESULTATS] Le DataFrame transformé est vide. Vérifiez les étapes précédentes.")

    print("\n--- Fin des tests de transformation ---")