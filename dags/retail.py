from datetime import timedelta, datetime
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

BUCKET_NAME = 'ted_retail_data_bucket'
PROJECT_ID = 'de-gcp-book'
DATASET_NAME = "retail_data"

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='retail-pipeline',
    default_args=default_args,
    schedule_interval='@once',
    description='Datapipeline using dbt, airflow and bigQuery',
    tags=['dbt', 'bigquery'],
) as dag:

    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo first task',
    )

    CreateNewBucket = GCSCreateBucketOperator(
        task_id="CreateNewBucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        labels={"env": "dev", "team": "airflow"},
        storage_class="MULTI_REGIONAL",
        location="US",
        gcp_conn_id="airflow-dbt",
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id=DATASET_NAME,
        gcp_conn_id='airflow-dbt',
    )

    second_task = BashOperator(
        task_id='second_task',
        bash_command='echo second task',
    )

    # Upload sales table to GCS
    upload_sales_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_sales_to_gcs',
        src='/usr/local/airflow/include/dataset/sales.csv',
        dst='retail_data/',
        bucket=BUCKET_NAME,
        gcp_conn_id='airflow-dbt',
        mime_type='text/csv',
        gzip=True
    )

    # Product hierarchy
    upload_products_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_products_to_gcs',
        src='/usr/local/airflow/include/dataset/product_hierarchy.csv',
        dst='retail_data/',
        bucket=BUCKET_NAME,
        gcp_conn_id='airflow-dbt',
        mime_type='text/csv',
        gzip=True
    )

    # Store cities
    upload_stores_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_stores_to_gcs',
        src='/usr/local/airflow/include/dataset/store_cities.csv',
        dst='retail_data/',
        bucket=BUCKET_NAME,
        gcp_conn_id='airflow-dbt',
        mime_type='text/csv',
        gzip=True
    )

    gcs_to_sales_bq = GCSToBigQueryOperator(
        task_id='gcs_to_sales_bq',
        bucket=BUCKET_NAME,
        source_objects=['retail_data/sales.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_NAME}.sales",
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='airflow-dbt'
    )

    gcs_to_products_bq = GCSToBigQueryOperator(
        task_id='gcs_to_products_bq',
        bucket=BUCKET_NAME,
        source_objects=['retail_data/product_hierarchy.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_NAME}.products",
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='airflow-dbt'
    )

    gcs_to_stores_bq = GCSToBigQueryOperator(
        task_id='gcs_to_stores_bq',
        bucket=BUCKET_NAME,
        source_objects=['retail_data/store_cities.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_NAME}.stores",
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='airflow-dbt'
    )

    # Define task dependencies
    first_task >> [CreateNewBucket, create_retail_dataset]
    [CreateNewBucket, create_retail_dataset] >> second_task
    second_task >> [upload_sales_to_gcs, upload_products_to_gcs, upload_stores_to_gcs]

    # Define dependencies for uploading to BigQuery
    upload_sales_to_gcs >> gcs_to_sales_bq
    upload_products_to_gcs >> gcs_to_products_bq
    upload_stores_to_gcs >> gcs_to_stores_bq

