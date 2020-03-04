from datetime import datetime, timedelta
import os
import logging
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 6, 30),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udacity_dend_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='staging_events',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data/2018/11/{ds}-events.json',
    path = 's3://udacity-dend/log_json_path.json',
   delimiter=',',
   headers='1',
   quote_char='"',
   file_type='json',
   aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    },
    region = 'us-west-2',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='staging_songs',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='song_data/',
    delimiter=',',
    headers='1',
    quote_char='"',
    file_type='json',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    },
    region = 'us-west-2'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> stage_events_to_redshift >> end_operator
start_operator >> stage_songs_to_redshift >> end_operator

