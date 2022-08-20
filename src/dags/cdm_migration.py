import pendulum
import datetime as dt

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime

postgres_conn_id = 'postgresql_de'

query_migr_dm_courier_ledger = """
with month_rates as (
	select
		dvr.courier_id::varchar as courier_id
		, extract('year' from dvr.order_ts)::int4 as settlement_year
		, extract('month' from dvr.order_ts)::int4 as settlement_month
		, avg(dvr.rate)::numeric as rate
	from dds.deliveries dvr
	group by dvr.courier_id::varchar, extract('year' from dvr.order_ts)::int4, extract('month' from dvr.order_ts)::int4
)
, detailed_deliveries as (
	select
		dvr.courier_id::varchar as courier_id
		, dvr.order_id as order_id
		, dvr.delivery_id as delivery_id
		, extract('year' from dvr.order_ts)::int4 as settlement_year
		, extract('month' from dvr.order_ts)::int4 as settlement_month
		, dvr.sum::numeric as orders_sum
		, dvr.rate::numeric as rate
		, mr.rate as avg_month_rate
		, case
			when mr.rate < 4.0 then greatest(dvr.sum * 0.05, 100)
			when mr.rate >= 4.0 and mr.rate < 4.5 then greatest(dvr.sum * 0.07, 150)
			when mr.rate >= 4.5 and mr.rate < 4.9 then greatest(dvr.sum * 0.08, 175)
			when mr.rate >= 4.9 then greatest(dvr.sum * 0.1, 200)
		end as courier_order_sum 
		, dvr.tip_sum::numeric as courier_tips
	from dds.deliveries dvr
		left join month_rates mr on mr.courier_id = dvr.courier_id::varchar
			and mr.settlement_year = extract('year' from dvr.order_ts)::int4
			and mr.settlement_month = extract('month' from dvr.order_ts)::int4
)
, agg_deliveries as (
	select 
		dd.courier_id
		, dd.settlement_year
		, dd.settlement_month
		, count(dd.order_id)::int4 as orders_count
		, sum(dd.orders_sum)::numeric as orders_total_sum
		, max(dd.avg_month_rate)::numeric as rate_avg
		, (sum(dd.orders_sum) * 0.25)::numeric as order_processing_fee
		, sum(courier_order_sum)::numeric as courier_order_sum 
		, sum(dd.courier_tips)::numeric as courier_tips_sum 
		, (sum(courier_order_sum) + sum(dd.courier_tips)) * 0.95 as courier_reward_sum
	from detailed_deliveries dd
	group by dd.courier_id, dd.settlement_year, dd.settlement_month
)
insert into cdm.dm_courier_ledger (
	courier_id
	, courier_name
	, settlement_year
	, settlement_month
	, orders_count
	, orders_total_sum
	, rate_avg
	, order_processing_fee
	, courier_order_sum
	, courier_tips_sum
	, courier_reward_sum
)
select 
	agg.courier_id
	, c.name as courier_name
	, agg.settlement_year
	, agg.settlement_month
	, agg.orders_count
	, agg.orders_total_sum
	, agg.rate_avg
	, agg.order_processing_fee
	, agg.courier_order_sum
	, agg.courier_tips_sum
	, agg.courier_reward_sum
from agg_deliveries agg
	left join dds.couriers c on c.id = agg.courier_id
order by agg.settlement_year, agg.settlement_month, agg.courier_id
on conflict (courier_id, settlement_year, settlement_month) do update set 
	courier_name = excluded.courier_name
	, orders_count = excluded.orders_count
	, orders_total_sum = excluded.orders_total_sum
	, rate_avg = excluded.rate_avg
	, order_processing_fee = excluded.order_processing_fee
	, courier_order_sum = excluded.courier_order_sum
	, courier_tips_sum = excluded.courier_tips_sum
	, courier_reward_sum = excluded.courier_reward_sum
;
"""

query_test_case_orders_count = """
with dm_orders_count as (
	select 
		sum(orders_count) as orders_count
	from cdm.dm_courier_ledger
)
, dds_orders_count as (
	select 
		count(order_id) as orders_count
	from dds.deliveries
)
insert into public_test.testing_result (test_date_time, test_name, test_result)
select 
	current_timestamp::timestamp as test_date_time
	, 'pos_test_case_orders_count' as test_name
	, (select orders_count from dm_orders_count) = (select orders_count from dds_orders_count) as test_result
;
"""

query_test_case_month_count = """
with dm_month_count as (
	select 
		count(distinct settlement_year::text || settlement_month::text) as month_count
	from cdm.dm_courier_ledger
)
, dds_month_count as (
	select 
		count(distinct extract('year' from order_ts)::text || extract('month' from order_ts)::text) as month_count
	from dds.deliveries
)
insert into public_test.testing_result (test_date_time, test_name, test_result)
select 
	current_timestamp::timestamp as test_date_time
	, 'pos_test_case_month_count' as test_name
	, (select month_count from dm_month_count) = (select month_count from dds_month_count) as test_result
;
"""

query_test_case_couriers_count = """
with dm_couriers_count as (
	select 
		count(distinct courier_id) as couriers_count
	from cdm.dm_courier_ledger
)
, dds_couriers_count as (
	select 
		count(distinct courier_id) as couriers_count
	from dds.deliveries
)
insert into public_test.testing_result (test_date_time, test_name, test_result)
select 
	current_timestamp::timestamp as test_date_time
	, 'pos_test_case_couriers_count' as test_name
	, (select couriers_count from dm_couriers_count) = (select couriers_count from dds_couriers_count) as test_result
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
        sql=query_migr_dm_courier_ledger)
    
    test_case_orders_count_task = PostgresOperator(
        task_id='test_case_orders_count',
        postgres_conn_id=postgres_conn_id,
        sql=query_test_case_orders_count)
    
    test_case_month_count_task = PostgresOperator(
        task_id='test_case_month_count',
        postgres_conn_id=postgres_conn_id,
        sql=query_test_case_month_count)
        
    test_case_couriers_count_task = PostgresOperator(
        task_id='test_case_couriers_count',
        postgres_conn_id=postgres_conn_id,
        sql=query_test_case_couriers_count)
    
    finish_task = DummyOperator(task_id='cdm_migr_finish')
    
    (
    start_task
    >> migr_dm_courier_ledger_task
    >> [test_case_orders_count_task, test_case_month_count_task, test_case_couriers_count_task]
    >> finish_task
    )
