from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajoutez le répertoire racine du projet au PYTHONPATH pour que les imports fonctionnent
# Assurez-vous que AIRFLOW_HOME pointe vers le répertoire racine de votre projet
project_root = os.environ.get('AIRFLOW_HOME')
if project_root not in sys.path:
    sys.path.append(project_root)

# Importez vos fonctions ETL
# Assurez-vous que ces imports fonctionnent depuis la perspective d'Airflow
# C'est-à-dire que etl_scripts/ doit être accessible dans le PYTHONPATH
# ou que les chemins des fonctions soient directs.
# Pour simplifier, nous allons appeler les scripts via os.system ou subprocess.
# Dans un environnement de production, il serait préférable d'importer les fonctions directement.

# Fonctions wrapper pour les opérateurs Python
def _run_extract_data():
    """Exécute le script d'extraction des données."""
    # Nous utilisons os.system ici pour la simplicité.
    # Dans un DAG plus robuste, on importerait et appellerait les fonctions directement.
    # N'oubliez pas que les variables d'environnement (AIRFLOW_HOME, OPENWEATHER_API_KEY)
    # doivent être définies dans l'environnement du worker Airflow.
    cmd = f"python {project_root}/etl_scripts/extract_data.py"
    print(f"Exécution de : {cmd}")
    result = os.system(cmd)
    if result != 0:
        raise Exception(f"Le script extract_data.py a échoué avec le code de sortie {result}")

def _run_transform_data():
    """Exécute le script de transformation des données."""
    cmd = f"python {project_root}/etl_scripts/transform_data.py"
    print(f"Exécution de : {cmd}")
    result = os.system(cmd)
    if result != 0:
        raise Exception(f"Le script transform_data.py a échoué avec le code de sortie {result}")

def _run_data_modeling():
    """Exécute le script de modélisation des données."""
    cmd = f"python {project_root}/etl_scripts/data_modeling.py"
    print(f"Exécution de : {cmd}")
    result = os.system(cmd)
    if result != 0:
        raise Exception(f"Le script data_modeling.py a échoué avec le code de sortie {result}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour les données météorologiques',
    schedule_interval=timedelta(days=1), # Exécute une fois par jour
    start_date=datetime(2023, 1, 1),
    catchup=False, # Ne pas exécuter les DAGs pour les dates passées
    tags=['weather', 'etl', 'dashboard'],
) as dag:
    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=_run_extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=_run_transform_data,
    )

    model_task = PythonOperator(
        task_id='model_weather_data',
        python_callable=_run_data_modeling,
    )

    # Définition de l'ordre des tâches
    extract_task >> transform_task >> model_task