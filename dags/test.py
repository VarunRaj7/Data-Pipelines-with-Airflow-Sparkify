from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator

from helpers import SqlQueries

default_args = {
    'owner': 'Udacity_and_Varun',
    'start_date': datetime.now(), #datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('Sparkify_dag_test',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift",
    stmt_res=[["SELECT COUNT(*) FROM public.songplays WHERE songplay_id IS NULL",0]]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> run_quality_checks
run_quality_checks >> end_operator