drop table if exists tests.newtest;
create table tests.newtest as 
select * from CO2_CalculateEmissions(
	'grid_globals.buildings'::regclass,
	'tests.rasu'::regclass,
    2021,
	'kasvu'::varchar,
	'em'::varchar,
	'hankinta'::varchar,
	2021
);
grant all on all tables in schema tests to ilmakalu;