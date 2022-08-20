import pendulum
import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

postgres_conn_id = 'postgresql_de'

query_migr_couriers = """
insert into dds.couriers (
	id
	, "name"
	, source_updated
)
select 
	c.object_id as id
	, c.object_value::json ->> 'name' as "name"
	, c.updated_at as source_updated
from stg.couriers c 
where c.updated_at > (select coalesce(max(source_updated)::timestamptz, '1900-01-01'::timestamptz) from dds.couriers)
on conflict (id) do update set
	"name" = excluded."name"
	, updated_at = excluded.updated_at
;
"""

query_migr_deliveries = """
insert into dds.deliveries (
	order_id
	, order_ts
	, delivery_id
	, courier_id
	, address
	, delivery_ts
	, rate
	, "sum"
	, tip_sum
	, source_updated
)
select 
	d.object_id as order_id
	, (d.object_value::json ->> 'order_ts')::timestamp as order_ts
	, d.object_value::json ->> 'delivery_id' as delivery_id
	, d.object_value::json ->> 'courier_id' as courier_id
	, d.object_value::json ->> 'address' as address
	, (d.object_value::json ->> 'delivery_ts')::timestamp as delivery_ts
	, (d.object_value::json ->> 'rate')::int4 as rate
	, (d.object_value::json ->> 'sum')::numeric as "sum"
	, (d.object_value::json ->> 'tip_sum')::numeric as tip_sum
	, d.updated_at as source_updated
from stg.deliveries d 
where d.updated_at > (select coalesce(max(source_updated)::timestamptz, '1900-01-01'::timestamptz) from dds.deliveries)
on conflict (order_id) do update set
	order_ts = excluded.order_ts
	, delivery_id = excluded.delivery_id
	, courier_id = excluded.courier_id
	, address = excluded.address
	, delivery_ts = excluded.delivery_ts
	, rate = excluded.rate
	, "sum" = excluded."sum"
	, tip_sum = excluded.tip_sum
	, source_updated = excluded.source_updated
;
"""


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
        sql=query_migr_couriers)
        
    migr_deliveries_task = PostgresOperator(
        task_id='migr_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql=query_migr_deliveries)
    
    finish_task = DummyOperator(task_id='dds_migr_finish')
    
    (
    start_task
    >> migr_couriers_task
    >> migr_deliveries_task
    >> finish_task
    )
