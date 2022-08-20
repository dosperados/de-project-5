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