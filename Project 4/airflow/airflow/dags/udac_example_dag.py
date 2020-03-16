from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from helpers import SqlQueries


aws_hook = AwsHook("aws_credentials") 
credentials = aws_hook.get_credentials()

AWS_KEY = credentials.access_key 
AWS_SECRET = credentials.secret_key

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'catchup': False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='staging_events',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data/2018/11/{ds}-events.json',
    path='s3://udacity-dend/log_json_path.json',
    delimiter=',',
    headers='1',
    quote_char='"',
    file_type='json',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    },
    region='us-west-2',
    provide_context=True)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='staging_songs',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key="song_data/A/A/A",
    delimiter=',',
    headers='1',
    quote_char='"',
    file_type='json',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    },
    region='us-west-2')

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql="songplay_table_insert",
    append_only=False)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql="user_table_insert",
    append_only=False)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql="song_table_insert",
    append_only=False)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql="artist_table_insert",
    append_only=False)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append_only=False)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    dg_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
   ]     
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator



