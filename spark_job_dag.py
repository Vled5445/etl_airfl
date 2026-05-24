from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,  
    DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago

BUCKET_NAME = 'pyspark-bucket-123456'
OUTPUT_BUCKET_NAME = 'output-bucket-123456'
LOG_BUCKET_NAME = 'log-bucket-123456'
SA_ID = 'aje5q48vcamiucmba1o9'
SUBNET_ID = 'enp22sh2s6lg71gper7a'
ZONE = 'ru-central1-a'  

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='spark_data_processing',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        zone=ZONE,
        s3_bucket=LOG_BUCKET_NAME,
        service_account_id=SA_ID,
        subnet_id=SUBNET_ID,
        computenode_count=2,
        datanode_count=0,
        services=('SPARK', 'YARN'),
    )

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id='run_pyspark_job',
       
        cluster_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='cluster_id') }}",
        main_python_file_uri=f's3a://{BUCKET_NAME}/process_data.py',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',

        cluster_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='cluster_id') }}",
    )

    create_cluster >> run_pyspark_job >> delete_cluster
