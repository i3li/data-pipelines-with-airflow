from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transfor cm data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

s3_bucket = Variable.get('s3_bucket')
s3_song_key = Variable.get('s3_song_key')
s3_log_key = Variable.get('s3_log_key')
s3_log_json_path_key = Variable.get('s3_log_json_path_key')

staging_events_table = 'staging_events'
staging_songs_table = 'staging_songs'

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table=staging_events_table,
    s3_bucket=s3_bucket,
    s3_key=s3_log_key,
    data_format="JSON 's3://{}/{}'".format(s3_bucket, s3_log_json_path_key),
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table=staging_songs_table,
    s3_bucket=s3_bucket,
    s3_key=s3_song_key,
    data_format="JSON 'auto'",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    select_sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    select_sql_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    select_sql_stmt=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    select_sql_stmt=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    select_sql_stmt=SqlQueries.time_table_insert,
    dag=dag
)


sql_count = 'SELECT COUNT(*) FROM {}'
has_rows_checker = lambda records: len(records) == 1 and len(records[0]) == 1 and records[0][0] > 0
has_no_rows_checker = lambda records: len(records) == 1 and len(records[0]) == 1 and records[0][0] == 0
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    postgres_conn_id='redshift',
    sql_stmts=(
        sql_count.format('songplays'), sql_count.format('users'),
        sql_count.format('songs'), sql_count.format('artists'),
        sql_count.format('time'), 'SELECT COUNT(*) FROM users WHERE first_name IS NULL'
    ),
    result_checkers=(
        has_rows_checker, has_rows_checker,
        has_rows_checker, has_rows_checker,
        has_rows_checker, has_no_rows_checker
    ),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
