-- schema
create schema if not exists stg;
GRANT ALL ON SCHEMA stg TO jovyan;

create or replace function renew_updated_at() 
returns trigger as $$
begin
	new.updated_at = current_timestamp;
	return new;
end;
$$ language plpgsql;



-- restaurants
create table if not exists stg.restaurants (
	object_id varchar not null,
	object_value text null, 
	updated_at timestamptz default current_timestamp,
	constraint restaurants_pkey primary key (object_id)
)
;

create trigger renew_updated_at
before update on stg.restaurants
for each row
execute function renew_updated_at();

ALTER TABLE stg.restaurants OWNER TO jovyan;
GRANT ALL ON TABLE stg.restaurants TO jovyan;



-- couriers
create table if not exists stg.couriers (
	object_id varchar not null,
	object_value text null, 
	updated_at timestamptz default current_timestamp,
	constraint couriers_pkey primary key (object_id)
)
;

create trigger renew_updated_at
before update on stg.couriers
for each row
execute function renew_updated_at();

ALTER TABLE stg.couriers OWNER TO jovyan;
GRANT ALL ON TABLE stg.couriers TO jovyan;



-- deliveries
create table if not exists stg.deliveries (
	object_id varchar not null,
	object_value text null, 
	updated_at timestamptz default current_timestamp,
	constraint deliveries_pkey primary key (object_id)
)
;

create trigger renew_updated_at
before update on stg.deliveries
for each row
execute function renew_updated_at();

ALTER TABLE stg.deliveries OWNER TO jovyan;
GRANT ALL ON TABLE stg.deliveries TO jovyan;