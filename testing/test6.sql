drop table if exists tests.erno_test1;
create table tests.erno_test1 as 
select * from CO2_CalculateEmissionsLoop(
	'tests.erno_alue'::regclass,
    true,
	true,
	'kasvu'::varchar,
	'hjm'::varchar,
	'hankinta'::varchar,
	2021,
	2025,
	'tests.erno_kt'::regclass,
	'tests.erno_kv'::regclass,
	'tests.erno_jl'::regclass
);
