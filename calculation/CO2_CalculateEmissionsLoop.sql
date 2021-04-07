DROP FUNCTION IF EXISTS CO2_CalculateEmissionsLoop;

CREATE OR REPLACE FUNCTION
public.CO2_CalculateEmissionsLoop(
    buildings regclass, -- ykr rakennusdatan taulunimi
    aoi regclass, -- Tutkimusalue | area of interest
    calculationScenario varchar, -- PITKO:n mukainen scenario
    method varchar, -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    electricityType varchar, -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    baseYear integer, -- Laskennan lähtövuosi
    targetYear integer, -- Laskennan tavoitevuosi
    plan_areas regclass default null, -- Taulu, jossa käyttötarkoitusalueet tai vastaavat
    plan_centers regclass default null, -- Taulu, jossa kkreskusverkkotiedot 
    plan_transit regclass default null -- Taulu, jossa intensiivinen joukkoliikennejärjestelmä 
)
RETURNS TABLE(
    geom geometry(MultiPolygon, 3067),
    xyind varchar(13),
    mun int,
    zone int,
    year date,
    floorspace int,
    pop smallint,
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
    sum_rakentaminen_tco2 real
)
AS $$
DECLARE
    calculationYears integer[];
    calculationYear integer;
BEGIN

    SELECT array(select generate_series(baseYear,targetYear))
    INTO calculationYears;

    FOREACH calculationYear in ARRAY calculationYears
    LOOP

        IF calculationYear = baseYear THEN
            CREATE TEMP TABLE res AS
            SELECT * FROM
                public.CO2_CalculateEmissions(
                    buildings, aoi, calculationYear, calculationScenario, method, electricityType, baseYear, targetYear, plan_areas, plan_centers, plan_transit
                );
        ELSE 
            INSERT INTO res
            SELECT * FROM
                public.CO2_CalculateEmissions(
                    buildings, aoi, calculationYear, calculationScenario, method, electricityType, baseYear, targetYear, plan_areas, plan_centers, plan_transit
                );
        END IF;
        
    END LOOP;

    UPDATE res SET zone = CASE
        WHEN LEFT(res.zone::varchar, 5)::int IN (99911, 99912) THEN 1
        WHEN LEFT(res.zone::varchar, 5)::int IN (99921, 99922) THEN 2
        WHEN LEFT(res.zone::varchar, 5)::int IN (99931, 99932) THEN 3
        WHEN LEFT(res.zone::varchar, 5)::int IN (99941, 99942) THEN 3
        WHEN LEFT(res.zone::varchar, 5)::int IN (99951, 99952) THEN 3
        WHEN LEFT(res.zone::varchar, 5)::int IN (6, 99961, 99962) THEN 10
        WHEN LEFT(res.zone::varchar, 5)::int IN (6, 99901, 99902) THEN 10
    ELSE res.zone END;

    RETURN QUERY SELECT * FROM res;
    DROP TABLE IF EXISTS res;

END;
$$ LANGUAGE plpgsql;