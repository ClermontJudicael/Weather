import pandas as pd
import os

# --- Configuration des chemins ---
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
if not AIRFLOW_HOME:
    raise EnvironmentError("La variable d'environnement AIRFLOW_HOME n'est pas définie.")

PROCESSED_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'processed')

def load_transformed_data(filename: str = "transformed_weather_data.parquet") -> pd.DataFrame:
    """
    Charge le DataFrame des données météorologiques transformées.
    :param filename: Le nom du fichier Parquet des données transformées.
    :return: DataFrame des données transformées.
    """
    filepath = os.path.join(PROCESSED_DATA_PATH, filename)
    if not os.path.exists(filepath):
        print(f"Erreur : Le fichier {filepath} n'existe pas. Assurez-vous d'avoir exécuté 'transform_data.py' en premier.")
        return pd.DataFrame() # Retourne un DataFrame vide en cas d'erreur

    print(f"\nChargement des données transformées depuis : {filepath}")
    try:
        df = pd.read_parquet(filepath)
        print(f"Données transformées chargées. Taille : {df.shape}")
        return df
    except Exception as e:
        print(f"Erreur lors du chargement du fichier Parquet : {e}")
        return pd.DataFrame()

def create_monthly_weather_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée un résumé mensuel des données météorologiques par ville.
    :param df: DataFrame des données météorologiques unifiées.
    :return: DataFrame agrégé par mois et par ville.
    """
    if df.empty:
        print("Le DataFrame d'entrée est vide, impossible de créer le résumé mensuel.")
        return pd.DataFrame()

    print("\nCréation du résumé météorologique mensuel...")

    # Assurez-vous que 'date' est bien un datetime et qu'il y a les colonnes nécessaires
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)

    # Extraire l'année et le mois
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    # Agrégation mensuelle
    monthly_summary = df.groupby(['city', 'country', 'latitude', 'longitude', 'year', 'month']).agg(
        avg_temp_celsius=('temp_celsius', 'mean'),
        total_precipitation_mm=('precipitation_mm', 'sum'),
        num_rainy_days=('is_rainy_day', 'sum'), # is_rainy_day est 1 pour pluie, 0 sinon
        max_wind_kph=('wind_kph', 'max'),
        avg_humidity_percent=('humidity_percent', 'mean')
    ).reset_index()

    # Convertir 'month' en nom de mois pour une meilleure lisibilité
    monthly_summary['month_name'] = monthly_summary['month'].apply(lambda x: pd.to_datetime(f'2000-{x}-01').strftime('%B'))
    
    print(f"Résumé mensuel créé. Taille : {monthly_summary.shape}")
    print("Aperçu du résumé mensuel :\n", monthly_summary.head())
    return monthly_summary

def save_modeled_data(df: pd.DataFrame, filename: str = "modeled_weather_data.parquet"):
    """
    Sauvegarde le DataFrame de données modélisées dans un fichier Parquet.
    :param df: Le DataFrame modélisé à sauvegarder.
    :param filename: Le nom du fichier Parquet de sortie.
    """
    if df.empty:
        print("Le DataFrame modélisé est vide, aucun fichier ne sera sauvegardé.")
        return

    output_path = os.path.join(PROCESSED_DATA_PATH, filename)
    try:
        df.to_parquet(output_path, index=False)
        print(f"\nDonnées modélisées sauvegardées avec succès dans : {output_path}")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du fichier Parquet modélisé : {e}")

if __name__ == "__main__":
    print("--- Démarrage de l'étape de modélisation des données ---")

    # 1. Charger les données transformées
    df_transformed = load_transformed_data()

    if not df_transformed.empty:
        # 2. Créer le résumé mensuel
        df_monthly_summary = create_monthly_weather_summary(df_transformed)

        # 3. Sauvegarder les données modélisées
        if not df_monthly_summary.empty:
            save_modeled_data(df_monthly_summary)
        else:
            print("Le DataFrame de résumé mensuel est vide, aucune donnée modélisée sauvegardée.")
    else:
        print("Le DataFrame transformé est vide, l'étape de modélisation est ignorée.")

    print("\n--- Fin de l'étape de modélisation des données ---")