import pendulum
import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "ilya",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'cdm_migration',
        default_args=args,
        description='DWH migration data to cdm layer',
        catchup=True,
        schedule_interval='20,40 * * * *',
        start_date=pendulum.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day, dt.datetime.now().hour, tz="UTC"),
        tags=['de-project-4', 'dwh', 'cdm', 'test'],
        is_paused_upon_creation=True,
) as dag:
    start_task = DummyOperator(task_id='cdm_migr_start')
    
    migr_dm_courier_ledger_task = PostgresOperator(
        task_id='migr_dm_courier_ledger',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/cdm_migr_dm_courier_ledger.sql")
    
    test_case_orders_count_task = PostgresOperator(
        task_id='test_case_orders_count',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/test_case_orders_count.sql")
    
    test_case_month_count_task = PostgresOperator(
        task_id='test_case_month_count',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/test_case_month_count.sql")
        
    test_case_couriers_count_task = PostgresOperator(
        task_id='test_case_couriers_count',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/test_case_couriers_count.sql")
    
    finish_task = DummyOperator(task_id='cdm_migr_finish')
    
    (
    start_task
    >> migr_dm_courier_ledger_task
    >> [test_case_orders_count_task, test_case_month_count_task, test_case_couriers_count_task]
    >> finish_task
    )
