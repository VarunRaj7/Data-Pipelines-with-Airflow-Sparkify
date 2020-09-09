from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Udacity and Varun',
    'start_date': datetime.now(), #datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('Sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/log_data",
    s3_key="",
    optional_parameters=["FORMAT AS JSON", "s3://udacity-dend/log_json_path.json", "TIMEFORMAT AS 'epochmillisecs';"]
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/song_data",
    s3_key="",
    optional_parameters=["FORMAT AS JSON 'AUTO';"]
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    table=songplays,
    append_to_table=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_stmt=SqlQueries.user_table_insert,
    table=users
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_stmt=SqlQueries.song_table_insert,
    table="public.songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_stmt=SqlQueries.artist_table_insert,
    table="public.artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",
    sql_stmt=SqlQueries.time_table_insert,
    table="public.time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    stmt_res=[["SELECT COUNT(*) FROM public.songplays WHERE songplay_id IS NULL",0]]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
