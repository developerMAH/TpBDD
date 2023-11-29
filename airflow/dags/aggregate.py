import pendulum
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)

def insert_into_bigquery(total_posts):
    from google.cloud import bigquery
    
    # Charger les informations d'authentification depuis le fichier service_account.json
    client = bigquery.Client.from_service_account_json("../data/service_account.json")

    print("**********************client authentifié")

    # Définir les détails de l'insertion dans BigQuery
    project_id = "tpnosql-404409"
    dataset_id = "mongodb_aggregation"
    table_id = "posts_aggregated"

    # Construire l'objet BigQuery
    bigquery_client = bigquery.Client(project=project_id)

    # Construire la référence de table
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery_client.get_table(table_ref)

    print("*************************acceder à la table")

    # Insérer le résultat dans la table BigQuery
    rows_to_insert = [total_posts]
    print("**************************", rows_to_insert)
    errors = bigquery_client.insert_rows(table, rows_to_insert, fields=["posts_number", "aggregation_date"])

    print("*************************insertion dans la table")

    if errors:
        print(f"Errors while inserting into BigQuery: {errors}")
    else:
        print("Successfully inserted into BigQuery")

def aggregate_data():
    from pymongo import MongoClient
    import json
    from datetime import datetime, timedelta
    from google.cloud import bigquery

    # Se connecter à MongoDB
    mongo_client = MongoClient("mongodb://admin:admin@mongodb:27017/")
    db = mongo_client["tpNoSQL"]
    collection = db["posts"]

    # Pré-agréger les données (nombre total de posts)
    total_posts = collection.count_documents({})

    # Imprimer le nombre total de posts
    print(f"Total number of posts: {total_posts}")

    
    # Charger les informations d'authentification depuis le fichier service_account.json
    client = bigquery.Client.from_service_account_json("./data/service_account.json")

    # Définir les détails de l'insertion dans BigQuery
    dataset_id = "mongodb_aggregation"
    table_id = "posts_aggregated"

    # Construire la référence de table
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Insérer le résultat dans la table BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("posts_number", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_json(
        json_rows=[{"posts_number": total_posts}],
        destination=table,
        job_config=job_config,
    )

    job.result()  # Attendre la fin du job

    if job.errors:
        print(f"Errors while inserting into BigQuery: {job.errors}")
    else:
        print("Successfully inserted into BigQuery")

with DAG(
    dag_id="DAG_aggregate_mongodb_data",
    schedule_interval=timedelta(minutes=1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    aggregate_mongodb_data = PythonVirtualenvOperator(
        task_id="aggregate_mongodb_data",
        requirements=["pymongo", "google-cloud-bigquery"],
        python_callable=aggregate_data,
    )
