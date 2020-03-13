from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
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

drop_all_tables = PostgresOperator(
    task_id="drop_all_tables",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.drop_all_tables
)

create_all_tables_if_not_exist = PostgresOperator(
    task_id="create_all_tables_if_not_exist",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.create_all_tables
)

populate_all_staging_tables = PostgresOperator(
    task_id="populate_all_staging_tables",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.populate_all_staging_tables
)

insert_all_warehouse_tables = PostgresOperator(
    task_id="insert_all_warehouse_tables",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.insert_all_warehouse_tables
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_all_tables >> create_all_tables_if_not_exist >> populate_all_staging_tables >> insert_all_warehouse_tables >> end_operator