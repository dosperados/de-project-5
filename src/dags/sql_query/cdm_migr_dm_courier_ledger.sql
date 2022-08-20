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