drop table if exists tests.seutu2;
create table tests.seutu2 as 
select * from CO2_CalculateEmissions(
	'aluejaot.treks'::regclass
);