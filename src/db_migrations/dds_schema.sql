-- schema
create schema if not exists dds;

create or replace function renew_updated_at() 
returns trigger as $$
begin
	new.updated_at = current_timestamp;
	return new;
end;
$$ language plpgsql;



-- couriers
create table if not exists dds.couriers (
	id varchar not null,
	name text null, 
	source_updated timestamptz null,
	updated_at timestamptz null default current_timestamp,
	constraint couriers_pkey primary key (id)
)
;

create trigger renew_updated_at
before update on dds.couriers
for each row
execute function renew_updated_at();

ALTER TABLE dds.couriers OWNER TO jovyan;
GRANT ALL ON TABLE dds.couriers TO jovyan;



-- deliveries
create table if not exists dds.deliveries (
	order_id varchar not null,
	order_ts timestamp null,
	delivery_id varchar null,
	courier_id varchar null,
	address text null, 
	delivery_ts timestamp null,
	rate int4 null,
	sum numeric null,
	tip_sum numeric null,
	source_updated timestamptz null,
	updated_at timestamptz null default current_timestamp,
	constraint deliveries_pkey primary key (order_id),
	constraint deliveries_courier_id_fkey foreign key (courier_id) references dds.couriers on delete cascade
)
;

create trigger renew_updated_at
before update on dds.deliveries
for each row
execute function renew_updated_at();

ALTER TABLE dds.deliveries OWNER TO jovyan;
GRANT ALL ON TABLE dds.deliveries TO jovyan;