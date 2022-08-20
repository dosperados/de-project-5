-- schema
create schema if not exists cdm;
GRANT ALL ON SCHEMA cdm TO jovyan;



-- dm_courier_ledger
create table if not exists cdm.dm_courier_ledger (
	id serial not null,
	courier_id integer not null, 
	courier_name varchar not null,
	settlement_year integer not null,
	settlement_month integer not null,
	orders_count integer not null,
	orders_total_sum numeric not null,
	rate_avg numeric not null,
	order_processing_fee numeric not null,
	courier_order_sum numeric not null,
	courier_tips_sum numeric not null,
	courier_reward_sum numeric not null,
	constraint dm_courier_ledger_pkey primary key (id),
	constraint dm_courier_ledger_month_check check (settlement_month >= 1 and settlement_month<=12)
)
;
create unique index if not exists dm_courier_ledger_unq on cdm.dm_courier_ledger (courier_id, settlement_year, settlement_month);

ALTER TABLE cdm.dm_courier_ledger OWNER TO jovyan;
GRANT ALL ON TABLE cdm.dm_courier_ledger TO jovyan;