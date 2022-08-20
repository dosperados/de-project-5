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