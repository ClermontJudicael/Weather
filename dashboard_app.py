import streamlit as st
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go

# --- Configuration des chemins ---
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')
if not AIRFLOW_HOME:
    st.error("La variable d'environnement AIRFLOW_HOME n'est pas définie. Veuillez la définir et relancer l'application.")
    st.stop() # Arrête l'exécution de l'application Streamlit

PROCESSED_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'processed')
MODELED_DATA_FILENAME = "modeled_weather_data.parquet"
MODELED_DATA_FILEPATH = os.path.join(PROCESSED_DATA_PATH, MODELED_DATA_FILENAME)

# --- Fonction de chargement des données (avec cache pour performance) ---
@st.cache_data
def load_modeled_data():
    """
    Charge les données météorologiques modélisées depuis le fichier Parquet.
    Utilise st.cache_data pour éviter de recharger les données à chaque interaction.
    """
    if not os.path.exists(MODELED_DATA_FILEPATH):
        st.error(f"Erreur : Le fichier de données modélisées n'a pas été trouvé à : {MODELED_DATA_FILEPATH}")
        st.warning("Veuillez vous assurer d'avoir exécuté les scripts ETL ('transform_data.py' et 'data_modeling.py') avant de lancer le tableau de bord.")
        return pd.DataFrame() # Retourne un DataFrame vide

    try:
        df = pd.read_parquet(MODELED_DATA_FILEPATH)
        df['month_year'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement ou du traitement du fichier de données modélisées : {e}")
        return pd.DataFrame()

# --- Titre du tableau de bord ---
st.set_page_config(layout="wide", page_title="Tableau de Bord Météo")
st.title("☀️ Tableau de Bord Météo des Capitales Mondiales")
st.markdown("Explorez les tendances météorologiques mensuelles agrégées pour différentes villes.")

# --- Chargement des données ---
df_modeled = load_modeled_data()

if df_modeled.empty:
    st.info("Aucune donnée disponible à afficher. Vérifiez les messages d'erreur ci-dessus.")
    st.stop() # Arrête l'application si les données ne sont pas chargées


# --- Barres latérales pour les filtres ---
st.sidebar.header("Filtres d'Analyse")

# Sélecteur de ville
all_cities = sorted(df_modeled['city'].unique().tolist())
selected_cities = st.sidebar.multiselect(
    "Sélectionnez les villes :",
    options=all_cities,
    default=all_cities[:5] # Sélectionne les 5 premières villes par défaut
)

# Sélecteur d'année
all_years = sorted(df_modeled['year'].unique().tolist(), reverse=True)
selected_years = st.sidebar.multiselect(
    "Sélectionnez les années :",
    options=all_years,
    default=all_years[0] if all_years else [] # Sélectionne la dernière année disponible par défaut
)

# Filtrer le DataFrame en fonction des sélections
df_filtered = df_modeled[
    (df_modeled['city'].isin(selected_cities)) &
    (df_modeled['year'].isin(selected_years))
]

if df_filtered.empty:
    st.warning("Aucune donnée disponible pour la sélection actuelle. Veuillez ajuster vos filtres.")
    st.stop()


# --- Onglets pour différentes vues ---
tab1, tab2, tab3 = st.tabs(["📊 Vue d'ensemble Graphiques", "📈 Tendances Mensuelles", "📋 Données Brutes Modélisées"])

with tab1:
    st.header("Vue d'ensemble des Données Météo")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Température Moyenne Mensuelle (°C)")
        fig_temp = px.line(
            df_filtered.sort_values(by=['month_year']),
            x='month_year',
            y='avg_temp_celsius',
            color='city',
            title='Température Moyenne par Mois et Ville',
            labels={'avg_temp_celsius': 'Température Moyenne (°C)', 'month_year': 'Mois'}
        )
        fig_temp.update_xaxes(dtick="M1", tickformat="%b\n%Y") # Format de l'axe X pour le mois et l'année
        st.plotly_chart(fig_temp, use_container_width=True)

    with col2:
        st.subheader("Précipitations Totales Mensuelles (mm)")
        fig_precip = px.bar(
            df_filtered.sort_values(by=['month_year']),
            x='month_year',
            y='total_precipitation_mm',
            color='city',
            title='Précipitations Totales par Mois et Ville',
            labels={'total_precipitation_mm': 'Précipitations (mm)', 'month_year': 'Mois'}
        )
        fig_precip.update_xaxes(dtick="M1", tickformat="%b\n%Y")
        st.plotly_chart(fig_precip, use_container_width=True)

    st.subheader("Nombre de Jours Pluvieux par Mois")
    fig_rainy = px.bar(
        df_filtered.sort_values(by=['month_year']),
        x='month_year',
        y='num_rainy_days',
        color='city',
        title='Nombre de Jours Pluvieux par Mois et Ville',
        labels={'num_rainy_days': 'Jours Pluvieux', 'month_year': 'Mois'}
    )
    fig_rainy.update_xaxes(dtick="M1", tickformat="%b\n%Y")
    st.plotly_chart(fig_rainy, use_container_width=True)


with tab2:
    st.header("Tendances Mensuelles Détaillées")
    st.write("Visualisez les tendances pour différentes métriques au fil des mois.")

    metric_options = {
        "Température Moyenne (°C)": "avg_temp_celsius",
        "Précipitations Totales (mm)": "total_precipitation_mm",
        "Nombre de Jours Pluvieux": "num_rainy_days",
        "Humidité Moyenne (%)": "avg_humidity_percent",
        "Vitesse Maximale du Vent (kph)": "max_wind_kph"
    }
    selected_metric_name = st.selectbox(
        "Sélectionnez une métrique à visualiser :",
        options=list(metric_options.keys())
    )
    selected_metric_column = metric_options[selected_metric_name]

    fig_trend = px.line(
        df_filtered.sort_values(by=['month_year']),
        x='month_year',
        y=selected_metric_column,
        color='city',
        title=f'Tendance Mensuelle pour {selected_metric_name}',
        labels={selected_metric_column: selected_metric_name, 'month_year': 'Mois'}
    )
    fig_trend.update_xaxes(dtick="M1", tickformat="%b\n%Y")
    st.plotly_chart(fig_trend, use_container_width=True)


with tab3:
    st.header("Données Modélisées Brutes")
    st.write("Voici un aperçu des données modélisées utilisées pour les visualisations.")
    st.dataframe(df_filtered) # Affiche le DataFrame filtré
    st.download_button(
        label="Télécharger les données filtrées",
        data=df_filtered.to_csv(index=False).encode('utf-8'),
        file_name='modeled_weather_data_filtered.csv',
        mime='text/csv',
    )
    st.subheader("Informations sur le DataFrame")
    buffer = pd.io.common.StringIO()
    df_filtered.info(buf=buffer)
    st.text(buffer.getvalue())