"""
DAG to run the NWS alerts ETL process
"""

import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from scripts.extract_nws_to_gcs import nws_to_gcs
from scripts.write_to_historical import main_historical
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
PROJECT_ID = "instant-mind-445916-f0"
CLUSTER_NAME = "nws-etl-cluster"
REGION = "us-central1"

default_args = {
    "owner": "Composer Example",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

with DAG(
    dag_id="nws_alerts_etl",
    catchup=False,
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    # extract nws data via API, write to GCS
    extract_nws_to_gcs = PythonOperator(
        task_id='nws_to_gcs',
        python_callable=nws_to_gcs,
        dag=dag
    )


    with TaskGroup('transform_job', tooltip='Tasks for dataproc job') as transform_job:
        # transform nws data via PySpark job on Dataproc
        create_dataproc_cluster = DataprocCreateClusterOperator(
            task_id='create_dataproc_cluster',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_NAME,
            region=REGION,
            cluster_config={
                'master_config': {
                    'num_instances': 1,
                    'machine_type_uri': 'n2-standard-4',
                },
                'worker_config': {
                    'num_instances': 2,
                    'machine_type_uri': 'n2-standard-4',
                },
            },
            dag=dag
        )

        pyspark_job = {
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': 'gs://dataproc-staging-us-central1-302225578058-8lsbms4x/notebooks/jupyter/nws_gcs_to_bq.py'
            },
        }

        submit_pyspark_job = DataprocSubmitJobOperator(
            task_id='submit_pyspark_job',
            job=pyspark_job,
            region=REGION,
            project_id=PROJECT_ID,
            dag=dag
        )

        delete_dataproc_cluster = DataprocDeleteClusterOperator(
            task_id='delete_dataproc_cluster',
            project_id=PROJECT_ID,
            cluster_name=CLUSTER_NAME,
            region=REGION,
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag
        )

        create_dataproc_cluster >> submit_pyspark_job >> delete_dataproc_cluster

    # write to historical table in BigQuery
    historical_load = PythonOperator(
        task_id='write_to_historical',
        python_callable=main_historical,
        trigger_rule='all_success',
        dag=dag
    )

extract_nws_to_gcs >> transform_job >> historical_load
