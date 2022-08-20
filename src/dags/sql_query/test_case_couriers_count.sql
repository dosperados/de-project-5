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