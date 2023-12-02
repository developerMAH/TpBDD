import pendulum
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)


def aggregate_data():
    from pymongo import MongoClient
    import json
    from datetime import datetime, timedelta
    from google.cloud import bigquery

    '''
        ***************** Authentification à mongoDB *****************
    '''

    # Se connecter à MongoDB
    mongo_client = MongoClient("mongodb://admin:admin@mongodb:27017/")
    db = mongo_client["tpNoSQL"]
    collection = db["posts"]

    '''
        ***************** Authentification à BQ *****************
    '''

    # Charger les informations d'authentification depuis le fichier service_account.json
    client = bigquery.Client.from_service_account_json("./data/service_account.json")

    # Définir les détails de l'insertion dans BigQuery
    dataset_id = "mongodb_aggregation"


    '''
        ***************** Agrégation du nombre total des publications *****************
    '''

    # Pré-agréger les données (nombre total de posts)
    total_posts = collection.count_documents({})

    # Imprimer le nombre total de posts
    print(f"Total number of posts: {total_posts}")

    table_total_pubs = "total_number_of_posts"

    # Construire la référence de table
    table_ref = client.dataset(dataset_id).table(table_total_pubs)
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


    '''
        ***************** Agrégation du nombre total des publications par type *****************
    '''
    # Effectuer l'agrégation du nombre total de publications par type
    pipeline = [
        {
            "$group": {
                "_id": "$@PostTypeId",
                "total_number": {"$sum": 1}
            }
        }
    ]

    result = list(collection.aggregate(pipeline))

    # Imprimer les résultats de l'agrégation par type
    for entry in result:
        post_type_id = entry["_id"]
        total_number = entry["total_number"]
        print(f"Type {post_type_id}: {total_number} posts")

    # Insérer le résultat agrégé dans la table BigQuery
    table_total_per_post = "total_number_per_post"

    # Construire la référence de table
    table_ref_per_post = client.dataset(dataset_id).table(table_total_per_post)
    table_per_post = client.get_table(table_ref_per_post)

    # Insérer les résultats agrégés dans la table BigQuery
    job_config_per_post = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("type", "INTEGER"),
            bigquery.SchemaField("total_number", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Convertir les résultats de l'agrégation en une liste de dictionnaires
    agg_results_list = [{"type": entry["_id"], "total_number": entry["total_number"]} for entry in result]

    job_per_post = client.load_table_from_json(
        json_rows=agg_results_list,
        destination=table_per_post,
        job_config=job_config_per_post,
    )

    job_per_post.result()  # Attendre la fin du job

    if job_per_post.errors:
        print(f"Errors while inserting into BigQuery: {job_per_post.errors}")
    else:
        print("Successfully inserted into BigQuery")

    
    '''
        ***************** Score moyen des publications *****************
    '''

    # Effectuer l'agrégation du score moyen des publications
    pipeline_avg_score = [
        {
            "$group": {
                "_id": None,
                "avg_score": {"$avg": {"$toDouble": "$@Score"}}  # Convert Score to double before calculating average
            }
        }
    ]

    result_avg_score = list(collection.aggregate(pipeline_avg_score))

    # Imprimer le résultat de l'agrégation du score moyen
    avg_score_posts = result_avg_score[0]["avg_score"] if result_avg_score else None
    print(f"Average score of posts: {avg_score_posts}")

    # Insérer le résultat agrégé dans la table BigQuery
    table_avg_score = "avg_score_per_posts"

    # Construire la référence de table
    table_ref_avg_score = client.dataset(dataset_id).table(table_avg_score)
    table_avg_score = client.get_table(table_ref_avg_score)

    # Insérer le résultat agrégé dans la table BigQuery
    job_config_avg_score = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("avg_score_posts", "FLOAT"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    job_avg_score = client.load_table_from_json(
        json_rows=[{"avg_score_posts": avg_score_posts}],
        destination=table_avg_score,
        job_config=job_config_avg_score,
    )

    job_avg_score.result()  # Attendre la fin du job

    if job_avg_score.errors:
        print(f"Errors while inserting into BigQuery (avg_score_per_posts): {job_avg_score.errors}")
    else:
        print("Successfully inserted into BigQuery (avg_score_per_posts)")

    '''
        ***************** Nombre total de vues par année de publication  *****************
    '''
    # Agrégation du nombre total de publications par année
    pipeline_total_posts_per_year = [
        {
            "$group": {
                "_id": {"$year": {"$toDate": "$@CreationDate"}},
                "total_posts": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "year": "$_id",
                "total_posts": "$total_posts"
            }
        }
    ]

    result_total_posts_per_year = list(collection.aggregate(pipeline_total_posts_per_year))

    # Imprimer les résultats de l'agrégation par année
    for entry in result_total_posts_per_year:
        year = entry["year"]
        total_posts_per_year = entry["total_posts"]
        print(f"Year {year}: {total_posts_per_year} posts")

    # Insérer le résultat agrégé dans la table BigQuery
    table_total_posts_per_year = "total_number_posts_per_year"

    # Construire la référence de table
    table_ref_total_posts_per_year = client.dataset(dataset_id).table(table_total_posts_per_year)
    table_total_posts_per_year = client.get_table(table_ref_total_posts_per_year)

    # Insérer les résultats agrégés dans la table BigQuery
    job_config_total_posts_per_year = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("total_posts", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Convertir les résultats de l'agrégation en une liste de dictionnaires
    agg_results_total_posts_per_year = [{"year": entry["year"], "total_posts": entry["total_posts"]} for entry in result_total_posts_per_year]

    job_total_posts_per_year = client.load_table_from_json(
        json_rows=agg_results_total_posts_per_year,
        destination=table_total_posts_per_year,
        job_config=job_config_total_posts_per_year,
    )

    job_total_posts_per_year.result()  # Attendre la fin du job

    if job_total_posts_per_year.errors:
        print(f"Errors while inserting into BigQuery (total_number_posts_per_year): {job_total_posts_per_year.errors}")
    else:
        print("Successfully inserted into BigQuery (total_number_posts_per_year)")

    '''
        ***************** les 50 Mots clés les plus fréquents dans le corps du texte  *****************
    '''
    pipeline_frequent_words = [
        {
            "$unwind": {
                "path": "$@Body",
            }
        },
        {
            "$project": {
                "words": {
                    "$regexFindAll": {
                        "input": {"$toLower": "$@Body"},
                        "regex": "\\w+"
                    }
                }
            }
        },
        {
            "$unwind": "$words"
        },
        {
            "$group": {
                "_id": "$words.match",
                "word_count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "_id": {"$regex": "\\w{3,}"}  # Filtre pour les mots de longueur >3
            }
        },
        {
            "$sort": {"word_count": -1}
        },
        {
            "$limit": 50
        }
    ]



    result_frequent_words = list(collection.aggregate(pipeline_frequent_words))

    # Imprimer les résultats des 50 mots clés les plus fréquents
    for entry in result_frequent_words:
        word = entry["_id"]
        word_count = entry["word_count"]
        print(f"Word: {word}, Count: {word_count}")

    # Insérer le résultat agrégé dans la table BigQuery
    table_frequent = "frequent_words_count"

    # Construire la référence de table
    table_ref_frequent = client.dataset(dataset_id).table(table_frequent)
    table_frequent = client.get_table(table_ref_frequent)

    # Insérer les résultats agrégés dans la table BigQuery
    job_config_frequent = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("word", "STRING"),
            bigquery.SchemaField("word_count", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Convertir les résultats de l'agrégation en une liste de dictionnaires
    agg_results_frequent = [{"word": entry["_id"], "word_count": entry["word_count"]} for entry in result_frequent_words]

    job_frequent = client.load_table_from_json(
        json_rows=agg_results_frequent,
        destination=table_frequent,
        job_config=job_config_frequent,
    )

    job_frequent.result()  # Attendre la fin du job

    if job_frequent.errors:
        print(f"Errors while inserting into BigQuery (frequent): {job_frequent.errors}")
    else:
        print("Successfully inserted into BigQuery (frequent)")


    '''
        ***************** les auteurs qui ont le plus de scores sur leurs publications et commentaires  *****************
    '''

    pipeline_top_score_authors = [
        {
            "$match": {
                "@OwnerUserId": {"$exists": True, "$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$@OwnerUserId",
                "total_score": {"$sum": {"$toInt": "$@Score"}}
            }
        },
        {
            "$sort": {"total_score": -1}
        },
        {
            "$limit": 50
        }
    ]

    result_top_score_authors = list(collection.aggregate(pipeline_top_score_authors))

    # Imprimer les résultats des 50 auteurs avec le plus de scores
    for entry in result_top_score_authors:
        author_id = entry["_id"]
        total_score = entry["total_score"]
        print(f"Author ID: {author_id}, Total Score: {total_score}")

    # Insérer le résultat agrégé dans la table BigQuery
    table_top_score_authors = "top_score_authors"

    # Construire la référence de table
    table_ref_top_score_authors = client.dataset(dataset_id).table(table_top_score_authors)
    table_top_score_authors = client.get_table(table_ref_top_score_authors)

    # Insérer les résultats agrégés dans la table BigQuery
    job_config_top_score_authors = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("author_id", "STRING"),
            bigquery.SchemaField("total_score", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Convertir les résultats de l'agrégation en une liste de dictionnaires
    agg_results_top_score_authors = [{"author_id": str(entry["_id"]), "total_score": entry["total_score"]} for entry in result_top_score_authors]

    job_top_score_authors = client.load_table_from_json(
        json_rows=agg_results_top_score_authors,
        destination=table_top_score_authors,
        job_config=job_config_top_score_authors,
    )

    job_top_score_authors.result()  # Attendre la fin du job

    if job_top_score_authors.errors:
        print(f"Errors while inserting into BigQuery (top_score_authors): {job_top_score_authors.errors}")
    else:
        print("Successfully inserted into BigQuery (top_score_authors)")



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
