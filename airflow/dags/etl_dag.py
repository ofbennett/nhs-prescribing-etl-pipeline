from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from helpers import sql_queries

default_args = {
    'owner': 'ofbennett',
    'start_date': datetime(2020, 1, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data into Postgres database with Airflow',
          schedule_interval='@monthly',
          catchup=False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> end_operator