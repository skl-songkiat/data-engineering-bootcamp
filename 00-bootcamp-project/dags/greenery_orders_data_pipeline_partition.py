import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
PROJECT_ID = "learn-gcs-can-deleted"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "orders"


def _extract_data(ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
               "order_id","created_at","order_cost","shipping_cost","order_total","tracking_id","shipping_service","estimated_delivery_at","delivered_at","status","user","promo","address"
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["order_id"],
                    each["created_at"],
                    each["order_cost"],
                    each["shipping_cost"],
                    each["tracking_id"],
                    each["shipping_service"],
                    each["estimated_delivery_at"],
                    each["delivered_at"],
                    each["status"],
                    each["user"],
                    each["promo"],
                    each["address"],
                ]
                writer.writerow(data)


def _load_data_to_gcs(ds):
    keyfile_gcs = f"{DAGS_FOLDER}/learn-gcs-can-deleted-eeb4b2ceecaa.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb2-bootcamp-200012"
    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)



def _load_data_from_gcs_to_bigquery():
    keyfile_bigquery = f"{DAGS_FOLDER}/learn-gcs-can-deleted-eeb4b2ceecaa.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{PROJECT_ID}.deb2.{DATA}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = "deb2-bootcamp-200012"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_orders_data_pipeline_partition",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = EmptyOperator(
        task_id="extract_data",
    )

    # Load data to GCS
    load_data_to_gcs = EmptyOperator(
        task_id="load_data_to_gcs",
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = EmptyOperator(
        task_id="load_data_from_gcs_to_bigquery",
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery