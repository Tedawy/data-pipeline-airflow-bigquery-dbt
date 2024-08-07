from pathlib import Path
import os
from datetime import  datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Define the ProjectConfig and ProfileConfig
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dags/dbt/dbtproject"  # Replace with your actual dbt project path
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="airflow-dbt",  # Replace with your Airflow GCP connection ID
        profile_args={
            "project": "de-gcp-book",  # Replace with your GCP project ID
            "dataset": "retail_data"  # Replace with your BigQuery dataset
        },
    ),
)


# Create the DbtDag
my_cosmos_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"/usr/local/airflow/dbt_venv/bin/dbt",),
    # normal dag parameters
    operator_args={
        "install_deps": True,
    },
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 2},
    tags=['dbt', 'bigquery']
)

my_cosmos_dag