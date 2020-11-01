create table tests."testrun2" as 
select * from il_calculate_emissions_loop(
	'tests.ykr_vaesto_2019_vuores'::regclass,
	'tests.ykr_tyopaikat_2017_vuores'::regclass,
	'tests.ykr_rak_rahu_uusi_toimipaikat_2020_vuores'::regclass,
	'tests.aluerajaus_vuores'::regclass,
	'kasvu'::varchar,
	'em'::varchar,
	'hankinta'::varchar,
	'Tampere'::varchar,
	2019,
	2040,
	'tests.kt_bau_kaavaehdotus_vuores'::regclass,
	'tests.kv_ehdotus_ve3_vuores'::regclass,
	'tests.jl_ehdotus_ve4_vuores'::regclass
)