# STEP 1: Import libraries
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import trigger_rule

from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# STEP 2: Define starting date and Spark references
# In this case: March 7th, 2022
start_date = datetime(2022, 3, 7)

# Spark References
SPARK_CODE = ("gs://us-central1-final-project-e-622f20ce-bucket/SPARK_FILES/extract_to_parquet.py")
dataproc_job_name = "spark_job_dataproc"

# STEP 3: Set default arguments
default_dag_args = {
    "start_date": start_date,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# STEP 4: Define DAG
# Set DAG name, description, schedule interval, etc.
with models.DAG(
    "data_pipeline_final_project",
    description="DAG of final project: data pipeline",
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args
) as dag:

    # STEP 5: Set Operators
    # 5.1 - DummyOperator: start_pipeline
    start_pipeline = DummyOperator(
        task_id="start_pipeline",
    )

    # 5.2 - Dataproc Operator: creating small dataproc cluster
    create_dataproc = dataproc_operator.DataprocClusterCreateOperator(
        task_id="create_dataproc",
        project_id=models.Variable.get("project_id"),
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        num_workers=0,
        zone=models.Variable.get("dataproc_zone"),
        region=models.Variable.get("dataproc_region"),
        master_machine_type="n1-standard-4",
        worker_machine_type="n1-standard-4"
    )

    # 5.3 - Dataproc Running PySpark Job
    extract_to_parquet = dataproc_operator.DataProcPySparkOperator(
        task_id="extract_to_parquet",
        main=SPARK_CODE,
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        region=models.Variable.get("dataproc_region"),
        job_name=dataproc_job_name
    )

    # 5.4 - DummyOperator: load_to_staging_pipeline
    load_to_staging_pipeline = DummyOperator(
        task_id="load_to_staging_pipeline",
    )

    # 5.5a - Load PARQUET files in Google Cloud Storage to BigQuery
    load_temp_by_city_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_temp_by_city_to_bigquery",
        bucket="final_project_ekoteguh",
        source_objects=["RAW/globaltempbycity.parquet/part*"],
        destination_project_dataset_table="de4-final-project-ekoteguh:FINAL_PROJECT_STAGING.temp_by_city",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5b - Load PARQUET files in Google Cloud Storage to BigQuery
    load_airport_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_airport_to_bigquery",
        bucket="final_project_ekoteguh",
        source_objects=["RAW/airport.parquet/part*"],
        destination_project_dataset_table="de4-final-project-ekoteguh:FINAL_PROJECT_STAGING.airport",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    # 5.5c - Load PARQUET files in Google Cloud Storage to BigQuery
    load_imigration_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id="load_imigration_to_bigquery",
        bucket="final_project_ekoteguh",
        source_objects=["RAW/immigration.parquet/part*"],
        destination_project_dataset_table="de4-final-project-ekoteguh:FINAL_PROJECT_STAGING.immigration",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    transform_staging_to_dimension = DummyOperator(
            task_id = 'transform_staging_to_dimension'
    )

    create_immigration_data = BigQueryOperator(
        task_id = "create_immigration_data",
        use_legacy_sql = False,
        params = {
            "project_id": "de4-final-project-ekoteguh",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/F_IMMIGRATION_DATA.sql"
    )

    transform_airport_to_dim = BigQueryOperator(
        task_id = "transform_airport_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "de4-final-project-ekoteguh",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_AIRPORT.sql"
    )

    transform_city_to_dim = BigQueryOperator(
        task_id = "transform_city_to_dim",
        use_legacy_sql = False,
        params = {
            "project_id": "de4-final-project-ekoteguh",
            "staging_dataset": "FINAL_PROJECT_STAGING",
            "dwh_dataset": "FINAL_PROJECT_DWH"
        },
        sql = "./SQL/D_CITY_DEMO.sql"
    )

    # 5.5 - Dataproc Cluster deletion
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id="delete_dataproc",
        project_id=models.Variable.get("project_id"),
        region=models.Variable.get("dataproc_region"),
        cluster_name="dataproc-cluster-final-project-{{ ds_nodash }}",
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    finish_pipeline = DummyOperator(
        task_id="finish_pipeline",
    )

    # STEP 6: Set DAGs dependencies
    start_pipeline >> create_dataproc >> extract_to_parquet >>  load_to_staging_pipeline >> [load_temp_by_city_to_bigquery, load_airport_to_bigquery, load_imigration_to_bigquery]

    [load_temp_by_city_to_bigquery, load_airport_to_bigquery, load_imigration_to_bigquery] >> create_immigration_data >> transform_staging_to_dimension

    transform_staging_to_dimension >> [transform_airport_to_dim, transform_city_to_dim] >> delete_dataproc >> finish_pipeline