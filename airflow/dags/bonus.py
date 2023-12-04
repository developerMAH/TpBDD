import pendulum
import logging
import json
import random
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

log = logging.getLogger(__name__)

def read_badges_and_push_to_bq():
    # Read data from Badges.json
    data_filepath = "./data/Badges.json"
    with open(data_filepath, "r") as f:
        content = f.read()
        badges_data = json.loads(content)

    # Limit to the first 50 rows
    random_badges_data = random.sample(badges_data, min(50, len(badges_data)))

    # Authenticate with BigQuery
    client = bigquery.Client.from_service_account_json("./data/service_account.json")
    dataset_id = "mongodb_aggregation"
    table_id = "badges_data"

    # Configure the job to write to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Id", "STRING"),
            bigquery.SchemaField("UserId", "STRING"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Date", "TIMESTAMP"),
            bigquery.SchemaField("Class", "STRING"),
            bigquery.SchemaField("TagBased", "BOOLEAN"),
        ],
        write_disposition="WRITE_APPEND",
    )

    # Convert the data into a list of dictionaries
    rows_to_insert = [
        {
            "Id": entry["@Id"],
            "UserId": entry["@UserId"],
            "Name": entry["@Name"],
            "Date": entry["@Date"],
            "Class": entry["@Class"],
            "TagBased": entry["@TagBased"] == "True",
        }
        for entry in random_badges_data
    ]

    table_ref_per_post = client.dataset(dataset_id).table(table_id)
    table_per_post = client.get_table(table_ref_per_post)
    job_per_post = client.load_table_from_json(
        json_rows=rows_to_insert,
        destination=table_per_post,
        job_config=job_config,
    )

    job_per_post.result()  # Wait for the job to finish
    log.info("Successfully inserted data into BigQuery.")

    # SQL query to count occurrences of each Name
    query = f"""
        SELECT Name, COUNT(*) as Count
        FROM `{client.project}.{dataset_id}.{table_id}`
        GROUP BY Name
        ORDER BY Count DESC
    """

    # Run the query and fetch the results
    query_job = client.query(query)
    results = query_job.result()

    # Convert the results into a list of dictionaries
    aggregation_rows = [
        {"Name": row["Name"], "total_badges": row["Count"]} for row in results
    ]

    # Configure the job to write to the aggregation table
    aggregation_table_id = "badges_aggregations"
    aggregation_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("total_badges", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    # Load data into the aggregation table
    aggregation_table_ref = client.dataset(dataset_id).table(aggregation_table_id)
    aggregation_table = client.get_table(aggregation_table_ref)
    aggregation_job = client.load_table_from_json(
        json_rows=aggregation_rows,
        destination=aggregation_table,
        job_config=aggregation_job_config,
    )

    aggregation_job.result()  # Wait for the job to finish
    log.info("Successfully inserted aggregated data into BigQuery.")

# Define the DAG
with DAG(
    dag_id="DAG_badges_data",
    schedule_interval=timedelta(minutes=1),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    # Define the PythonOperator to execute the task
    read_badges_task = PythonOperator(
        task_id="read_badges_and_push_to_bq",
        python_callable=read_badges_and_push_to_bq,
    )

    # Set task dependencies if needed
    read_badges_task
