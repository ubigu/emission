DROP FUNCTION IF EXISTS il_calculate_emissions_loop_new;
CREATE OR REPLACE FUNCTION
public.il_calculate_emissions_loop_new(
    ykr_v regclass, -- YKR-väestödata | YKR population data
    ykr_tp regclass, -- YKR-työpaikkadata | YKR workplace data
    ykr_rakennukset regclass, -- ykr rakennusdatan taulunimi
    aoi regclass, -- Tutkimusalue | area of interest
    skenaario varchar, -- PITKO:n mukainen skenaario
    metodi varchar, -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    sahkolaji varchar, -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    alue varchar, -- Alue, jolle päästöjä ollaan laskemassa
    baseYear integer, -- Laskennan lähtövuosi
    targetYear integer, -- Laskennan tavoitevuosi
    kt_taulu regclass, -- Taulu, jossa käyttötarkoitusalueet tai vastaavat
    kv_taulu regclass default null, -- Taulu, jossa keskusverkkotiedot 
    jl_taulu regclass default null -- Taulu, jossa intensiivinen joukkoliikennejärjestelmä
)
RETURNS TABLE(
    xyind varchar(13),
    tilat_vesi_tco2 real,
    tilat_lammitys_tco2 real,
    tilat_jaahdytys_tco2 real,
    sahko_kiinteistot_tco2 real,
    sahko_kotitaloudet_tco2 real,
    sahko_palv_tco2 real,
    sahko_tv_tco2 real,
    liikenne_hlo_tco2 real,
    liikenne_tv_tco2 real,
    liikenne_palv_tco2 real,
    rak_korjaussaneeraus_tco2 real,
    rak_purku_tco2 real,
    rak_uudis_tco2 real,
    sum_yhteensa_tco2 real,
    sum_lammonsaato_tco2 real,
    sum_liikenne_tco2 real,
    sum_sahko_tco2 real,
    sum_rakentaminen_tco2 real,
    asukkaat int,
    kerrosala int,
    uz int,
    vuosi date,
    geom geometry(MultiPolygon, 3067))
AS $$
DECLARE
    laskentavuodet integer[];
    calculationYear integer;
BEGIN

    SELECT array(select generate_series(baseYear,targetYear)) INTO laskentavuodet;

    FOREACH calculationYear in ARRAY laskentavuodet
    LOOP

        IF calculationYear = baseYear THEN
            CREATE TEMP TABLE res AS SELECT * FROM il_calculate_emissions_new(ykr_v, ykr_tp, ykr_rakennukset, aoi, calculationYear, skenaario, metodi, sahkolaji, alue, baseYear, targetYear, kt_taulu, kv_taulu, jl_taulu);
        ELSE 
            INSERT INTO res SELECT * FROM il_calculate_emissions_new(ykr_v, ykr_tp, ykr_rakennukset, aoi, calculationYear, skenaario, metodi, sahkolaji, alue, baseYear, targetYear, kt_taulu, kv_taulu, jl_taulu);
        END IF;
        
    END LOOP;

/* Update to different jolis
    UPDATE res SET uz = CASE
        WHEN LEFT(res.uz::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901) THEN 9991
        WHEN LEFT(res.uz::varchar,5)::int IN (99912, 99922, 99932, 99942, 99952, 99962, 99902) THEN 9992
    ELSE res.uz END);
*/

    UPDATE res SET uz = CASE
        WHEN LEFT(res.uz::varchar,5)::int IN (99911, 99912) THEN 1
        WHEN LEFT(res.uz::varchar,5)::int IN (99921, 99922) THEN 2
        WHEN LEFT(res.uz::varchar,5)::int IN (99931, 99932) THEN 3
        WHEN LEFT(res.uz::varchar,5)::int IN (99941, 99942) THEN 3
        WHEN LEFT(res.uz::varchar,5)::int IN (99951, 99952) THEN 3
        WHEN LEFT(res.uz::varchar,5)::int IN (6, 99961, 99962) THEN 10
        WHEN LEFT(res.uz::varchar,5)::int IN (6, 99901, 99902) THEN 10
    ELSE res.uz END;

    RETURN QUERY SELECT * FROM res;
    DROP TABLE IF EXISTS res;
/*
    EXCEPTION WHEN OTHERS THEN
        DROP TABLE IF EXISTS res;
  */
END;
$$ LANGUAGE plpgsql;