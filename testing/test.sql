create table tests."newtest" as 
select * from CO2_CalculateEmissionsLoop(
	'tests.ykr_rak_rahu_uusi_toimipaikat_2020_vuores'::regclass,
	'tests.aluerajaus_vuores'::regclass,
	'kasvu'::varchar,
	'em'::varchar,
	'hankinta'::varchar,
	2020,
	2025,
	'tests.kt_bau_kaavaehdotus_vuores'::regclass,
	'tests.kv_ehdotus_ve3_vuores'::regclass,
	'tests.jl_ehdotus_ve4_vuores'::regclass
)