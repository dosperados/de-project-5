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
        'dds_migration',
        default_args=args,
        description='DWH migration data to dds layer',
        catchup=True,
        schedule_interval='15,35,55 * * * *',
        start_date=pendulum.datetime(dt.datetime.now().year, dt.datetime.now().month, dt.datetime.now().day, dt.datetime.now().hour, tz="UTC"),
        tags=['de-project-4', 'dwh', 'dds'],
        is_paused_upon_creation=True,
) as dag:
    start_task = DummyOperator(task_id='dds_migr_start')
    
    migr_couriers_task = PostgresOperator(
        task_id='migr_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/dds_migr_couriers.sql")
        
    migr_deliveries_task = PostgresOperator(
        task_id='migr_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="sql_query/dds_migr_deliveries.sql")
    
    finish_task = DummyOperator(task_id='dds_migr_finish')
    
    (
    start_task
    >> migr_couriers_task
    >> migr_deliveries_task
    >> finish_task
    )
