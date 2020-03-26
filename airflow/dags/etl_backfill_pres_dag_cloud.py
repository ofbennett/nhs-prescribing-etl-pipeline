from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.data_quality import DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from datetime import datetime, timedelta
from helpers import sql_queries_cloud

default_args = {
    'owner': 'ofbennett',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('etl_backfill_pres_dag_cloud',
          default_args=default_args,
          description='Load and transform backfill prescription data only into AWS redshift database with Airflow',
          schedule_interval='@monthly',
          catchup=True,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_staging_tables = PostgresOperator(
    task_id="drop_staging_tables",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.drop_staging_tables
)

pres_staging_table_create = PostgresOperator(
    task_id="pres_staging_table_create",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.pres_staging_table_create
)

pres_staging_table_populate = StageToRedshiftOperator(
    task_id="pres_staging_table_populate",
    dag=dag,
    provide_context=True,
    redshift_conn_id="my_redshift_conn",
    aws_credentials_id="my_aws_conn",
    table="pres_staging_table",
    s3_bucket="prescribing-data",
    s3_key="{{ execution_date.year }}_{{ ds[5:7] }}/T{{ execution_date.year }}{{ ds[5:7] }}PDPI_BNFT",
    header=True
)

pres_fact_table_insert = PostgresOperator(
    task_id="pres_fact_table_insert",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.pres_fact_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="my_redshift_conn",
    quality_checks=sql_queries_cloud.quality_tests
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_staging_tables >> pres_staging_table_create
pres_staging_table_create >> pres_staging_table_populate >> pres_fact_table_insert
pres_fact_table_insert >> run_quality_checks >> end_operator