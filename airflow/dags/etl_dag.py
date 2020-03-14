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

dag = DAG('etl_dag_split',
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

pres_staging_table_populate = PostgresOperator(
    task_id="pres_staging_table_populate",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.pres_staging_table_populate
)

gp_prac_staging_table_populate = PostgresOperator(
    task_id="gp_prac_staging_table_populate",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.gp_prac_staging_table_populate
)

bnf_info_staging_table_populate = PostgresOperator(
    task_id="bnf_info_staging_table_populate",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.bnf_info_staging_table_populate
)

pres_fact_table_insert = PostgresOperator(
    task_id="pres_fact_table_insert",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.pres_fact_table_insert
)

gp_pracs_dim_table_insert = PostgresOperator(
    task_id="gp_pracs_dim_table_insert",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.gp_pracs_dim_table_insert
)

bnf_info_dim_table_insert = PostgresOperator(
    task_id="bnf_info_dim_table_insert",
    dag=dag,
    postgres_conn_id="my_postgres_conn",
    sql=sql_queries.bnf_info_dim_table_insert
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_all_tables >> create_all_tables_if_not_exist
create_all_tables_if_not_exist >> [pres_staging_table_populate, gp_prac_staging_table_populate, bnf_info_staging_table_populate]
pres_staging_table_populate >> pres_fact_table_insert
gp_prac_staging_table_populate >> gp_pracs_dim_table_insert
bnf_info_staging_table_populate >> bnf_info_dim_table_insert
[pres_fact_table_insert, gp_pracs_dim_table_insert, bnf_info_dim_table_insert] >> end_operator