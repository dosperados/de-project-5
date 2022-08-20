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