DROP FUNCTION IF EXISTS il_calculate_emissions_loop;
CREATE OR REPLACE FUNCTION
public.il_calculate_emissions_loop(
    ykr_v text, -- YKR-väestödata | YKR population data
    ykr_tp text, -- YKR-työpaikkadata | YKR workplace data
    ykr_rakennukset text, -- ykr rakennusdatan taulunimi
    aoi text, -- Tutkimusalue | area of interest
    skenaario varchar, -- PITKO:n mukainen skenaario
    metodi varchar, -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    sahkolaji varchar, -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    alue varchar, -- Alue, jolle päästöjä ollaan laskemassa
    baseYear integer, -- Laskennan lähtövuosi
    targetYear integer, -- Laskennan tavoitevuosi
    kt_taulu text, -- Taulu, jossa käyttötarkoitusalueet tai vastaavat
    kv_taulu text default null, -- Taulu, jossa keskusverkkotiedot 
    jl_taulu text default null -- Taulu, jossa intensiivinen joukkoliikennejärjestelmä
)
RETURNS TABLE(
    xyind varchar(13),
    yhteensa_tco2 real,
    vesi_tco2 real,
    lammitys_tco2 real,
    jaahdytys_tco2 real,
    kiinteistosahko_tco2 real,
    sahko_kotitaloudet_tco2 real,
    sahko_palv_tco2 real,
    sahko_tv_tco2 real,
    hloliikenne_tco2 real,
    tvliikenne_tco2 real,
    palvliikenne_tco2 real,
    korjaussaneeraus_tco2 real,
    purkaminen_tco2 real,
    uudisrakentaminen_tco2 real,
    vuosi smallint,
    geom geometry(MultiPolygon, 3067))
AS $$
DECLARE
    laskentavuodet integer[];
    vuosi integer;
BEGIN

    SELECT array(select generate_series(baseYear,targetYear)) INTO laskentavuodet;

    FOREACH vuosi in ARRAY laskentavuodet
    LOOP

        IF vuosi = baseYear THEN
            DROP TABLE IF EXISTS res;
            CREATE TEMP TABLE res AS SELECT * FROM il_calculate_emissions(ykr_v, ykr_tp, ykr_rakennukset, aoi, vuosi, skenaario, metodi, sahkolaji, alue, baseYear, targetYear, kt_taulu, kv_taulu, jl_taulu);
        ELSE 
            INSERT INTO res SELECT * FROM il_calculate_emissions(ykr_v, ykr_tp, ykr_rakennukset, aoi, vuosi, skenaario, metodi, sahkolaji, alue, baseYear, targetYear, kt_taulu, kv_taulu, jl_taulu);
        END IF;
        
    END LOOP;

    RETURN QUERY SELECT * FROM res;
    DROP TABLE IF EXISTS res;
/*
    EXCEPTION WHEN OTHERS THEN
        DROP TABLE IF EXISTS res;
  */
END;
$$ LANGUAGE plpgsql;