from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago


BUCKET_NAME = 'bucket-0112233'
PROJECT_ID = 'de-gcp-book'
DATASET_NAME = "retail"
TABLE_NAME = "raw_invoices"

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
    description='Datapipeline using dbt , airflow and bigQuery',
    tags=['dbt', 'bigquery'],
) as dag:

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/Online_Retail.csv',
        dst='raw/online_retail.csv',
        bucket='bucket-0112233',
        gcp_conn_id='airflow-dbt',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='airflow-dbt',
    )

    gcs_to_raw = GCSToBigQueryOperator(
        task_id='gcs_to_raw',
        bucket=BUCKET_NAME,
        source_objects=['raw/online_retail.csv'],
        destination_project_dataset_table=f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}",
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='airflow-dbt',
        schema_fields=[
            {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'UnitPrice', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        encoding='ISO-8859-1'
    )


upload_csv_to_gcs >> create_retail_dataset >> gcs_to_raw


