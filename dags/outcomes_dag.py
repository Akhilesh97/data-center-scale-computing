"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

from datetime import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from etl_scripts.pipeline import transform_data
import os
from pathlib import Path
from etl_scripts.pipeline import extract_data, load_data_from_cloud, load_fact_data_from_cloud
from google.oauth2 import service_account

import json

SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date=20231119&accessType=DOWNLOAD'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds  }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'

credentials_dict = {
  "type": "service_account",
  "project_id": "XXXXX",
  "private_key_id": "XXXXX",
  "private_key": "-----BEGIN PRIVATE KEY-----\nXXXX\n-----END PRIVATE KEY-----\n",
  "client_email": "akhilesh@dazzling-tensor-405719.iam.gserviceaccount.com",
  "client_id": "XXX",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "XXXX",
  "client_x509_cert_url": "XXXX",
  "universe_domain": "googleapis.com"
}
credentials = service_account.Credentials.from_service_account_info(credentials_dict)

with DAG(
        dag_id = "outcome_dag",
        start_date = datetime(2023,11,19),
        schedule_interval = '@daily'
        ) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable = extract_data,
        op_kwargs = {
            'url': SOURCE_URL,
            'gcs_bucket_name': 'raw-csv-outcomes',
            'credentials': credentials
        }
    )
    
    transform = PythonOperator(
        task_id="transform",
        python_callable = transform_data,
        op_kwargs = {
            'source_gcs_bucket_name':'raw-csv-outcomes',
            'gcs_bucket_name': 'raw-parquet-files',
            'remote_blob_name': 'outcomes.parquet',
            'credentials': credentials
        }
    )
     
    
    load_outcome_dim = PythonOperator(
        task_id="load_outcome_dim",
        python_callable = load_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'dim_outcome_table.parquet',
            'credentials': credentials,
            'key': 'outcome_id'
        }
    )
    
    load_dim_animal_name = PythonOperator(
        task_id="load_dim_animal_name",
        python_callable = load_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'dim_animal_name.parquet',
            'credentials': credentials,
            'key': 'animal_key'
        }        
    )
    
    load_dim_animal_char = PythonOperator(
        task_id="load_dim_animal_char",
        python_callable = load_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'dim_animal_char.parquet',
            'credentials': credentials,
            'key': 'animal_char_key'
        }
    )
    
    load_dim_repro_table = PythonOperator(
        task_id="load_dim_repro_table",        
        python_callable = load_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'dim_repro_table.parquet',
            'credentials': credentials,
            'key': 'repro_key'
        }        
    )
    
    load_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable = load_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'dim_date.parquet',
            'credentials': credentials,
            'key': 'datetime_key'
        }         
    )
    
    load_fact_table = PythonOperator(
        task_id="load_fact_table",
        python_callable = load_fact_data_from_cloud,
        op_kwargs = {
            'gcs_bucket_name': 'raw-parquet-files'  ,          
            'remote_blob_name': 'fact_table.parquet',
            'credentials': credentials,            
        }  

    )
    
    extract >> transform >> [load_outcome_dim, load_dim_animal_name, load_dim_animal_char, load_dim_repro_table, load_dim_date] >> load_fact_table
    
    
'''with DAG(
    dag_id="example_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )
    # [END howto_operator_bash]

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    # [START howto_operator_bash_template]
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    # [END howto_operator_bash_template]
    also_run_this >> run_this_last

# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()'''