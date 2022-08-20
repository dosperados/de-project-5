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