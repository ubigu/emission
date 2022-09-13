drop table if exists tests.seutu3;
create table tests.seutu3 as 
select * from CO2_CalculateEmissions(
	'aluejaot.treks'::regclass
);

drop table if exists tests.seutu4;
create table tests.seutu4 as 
select * from CO2_CalculateEmissions(
	'aluejaot.treks'::regclass,
    2021,
	'kasvu'::varchar,
	'hjm'::varchar,
	'hankinta'::varchar,
	2021
);
grant all on all tables in schema tests to ilmakalu;