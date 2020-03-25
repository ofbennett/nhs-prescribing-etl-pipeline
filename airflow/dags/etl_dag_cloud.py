from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.data_quality import DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from datetime import datetime, timedelta
from helpers import sql_queries_cloud

default_args = {
    'owner': 'ofbennett',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('etl_dag_cloud',
          default_args=default_args,
          description='Load and transform data into AWS redshift database with Airflow',
          schedule_interval='@monthly',
          catchup=False,
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_all_tables = PostgresOperator(
    task_id="drop_all_tables",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.drop_all_tables
)

create_all_tables_if_not_exist = PostgresOperator(
    task_id="create_all_tables_if_not_exist",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.create_all_tables
)

pres_staging_table_populate = StageToRedshiftOperator(
    task_id="pres_staging_table_populate",
    dag=dag,
    redshift_conn_id="my_redshift_conn",
    aws_credentials_id="my_aws_conn",
    table="pres_staging_table",
    s3_bucket="prescribing-data",
    s3_key="2019_12/T201912PDPI_BNFT",
    header=True
)

gp_prac_staging_table_populate = StageToRedshiftOperator(
    task_id="gp_prac_staging_table_populate",
    dag=dag,
    redshift_conn_id="my_redshift_conn",
    aws_credentials_id="my_aws_conn",
    table="gp_pracs_staging_table",
    s3_bucket="prescribing-data",
    s3_key="2019_11/T201911ADDR_BNFT",
    header=False
)

bnf_info_staging_table_populate = StageToRedshiftOperator(
    task_id="bnf_info_staging_table_populate",
    dag=dag,
    redshift_conn_id="my_redshift_conn",
    aws_credentials_id="my_aws_conn",
    table="bnf_info_staging_table",
    s3_bucket="prescribing-data",
    s3_key="BNF_Code_Information.csv",
    header=True
)

postcode_info_staging_table_populate = StageToRedshiftOperator(
    task_id="postcode_info_staging_table_populate",
    dag=dag,
    redshift_conn_id="my_redshift_conn",
    aws_credentials_id="my_aws_conn",
    table="postcode_info_staging_table",
    s3_bucket="prescribing-data",
    s3_key="postcode_info.csv",
    header=True
)

pres_fact_table_insert = PostgresOperator(
    task_id="pres_fact_table_insert",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.pres_fact_table_insert
)

gp_pracs_dim_table_insert = PostgresOperator(
    task_id="gp_pracs_dim_table_insert",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.gp_pracs_dim_table_insert
)

bnf_info_dim_table_insert = PostgresOperator(
    task_id="bnf_info_dim_table_insert",
    dag=dag,
    postgres_conn_id="my_redshift_conn",
    sql=sql_queries_cloud.bnf_info_dim_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="my_redshift_conn",
    quality_checks=sql_queries_cloud.quality_tests
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_all_tables >> create_all_tables_if_not_exist
create_all_tables_if_not_exist >> [pres_staging_table_populate, 
                                    gp_prac_staging_table_populate, 
                                    bnf_info_staging_table_populate, 
                                    postcode_info_staging_table_populate]
pres_staging_table_populate >> pres_fact_table_insert
[gp_prac_staging_table_populate, postcode_info_staging_table_populate] >> gp_pracs_dim_table_insert
bnf_info_staging_table_populate >> bnf_info_dim_table_insert
[pres_fact_table_insert, gp_pracs_dim_table_insert, bnf_info_dim_table_insert] >> run_quality_checks
run_quality_checks >> end_operator