create schema if not exists public_test;


-- testing_result
create table if not exists public_test.testing_result (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	test_date_time timestamp NOT NULL,
	test_name text NOT NULL,
	test_result bool NOT NULL,
	CONSTRAINT testing_result_pkey PRIMARY KEY (id)
)
;

ALTER TABLE public_test.testing_result OWNER TO jovyan;
GRANT ALL ON TABLE public_test.testing_result TO jovyan;
