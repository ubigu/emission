-- Henkilöliikenne

-- LASKENTAKAAVOJEN TULOSTEN KOOSTAMINEN, PARAMETRIEN POIMINTA TAULUISTA JA LASKENTAKAAVOJEN KUTSUT

/* Asuinpaikan mukaan jyvitettävien co2-päästöjen laskenta */
/* Esimerkkikutsu: SELECT * FROM il_hloliikenne_co2(134, 521, 2018, 'bussi', 6.7, 3, 'Tampere', 'wem', 'em', 'hankinta'); */
DROP FUNCTION IF EXISTS il_traffic_personal_co2;
CREATE OR REPLACE FUNCTION
public.il_traffic_personal_co2(
    v_yht integer, -- Väestö yhteensä
    tp_yht integer, -- Väestö yhteensä 
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    kulkumuoto varchar, 
    centdist integer,
    vyoh15 integer,
    area varchar,
    scenario varchar, -- PITKO:n mukainen kehitysskenaario
    gco2kwh_matrix real[], /* Ominaispäästökertoimien pohja */ /* Emission values template */
    baseYear integer default null,
    renew boolean default null
) 
RETURNS real AS
$$
DECLARE
    hlt_taulu1 varchar;
    tposuus real;
    kmuoto_km_muutos real;
    kmuoto_gco2kwh real[];
    
    kmuoto_hkmvrk real;
    kmuoto_kvoima_jakauma real[];
    kvoima_kwhkm real[];
    kmuoto_kvoima_mult_res real[];

    apliikenne_kuormitus real;
    tpliikenne_kuormitus real;
    kmuoto_kwhkm real;
    apliikenne_hkm real;
    tpliikenne_hkm real;
    hloliikenne_kwh real;
    hloliikenne_co2 real[];
BEGIN
    IF (v_yht <= 0 OR v_yht IS NULL) AND (tp_yht <= 0 OR tp_yht IS NULL) THEN
        RETURN 0;
    ELSE

    -------------------------------------------------------------------------------------------
    /* Matkat ja kuormitukset */

    EXECUTE 'SELECT hlt FROM aluejaot.alueet WHERE kunta = $1 OR maakunta = $1' INTO hlt_taulu1 USING area;

    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.hlt_kmmuutos WHERE vyoh15 = $1' INTO kmuoto_km_muutos USING vyoh15;
    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.hlt_tposuus WHERE vyoh15 = $1' INTO tposuus USING vyoh15;
    
    IF renew THEN
        EXECUTE 'CREATE TEMP TABLE hlt AS SELECT * FROM liikenne.'||hlt_taulu1||'';
        UPDATE hlt SET
        raide = (CASE
            WHEN hlt.vyoh15 = 1 THEN
                raide + bussi * 0.4 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 2 THEN          
                raide + bussi * 0.3 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00033 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 3 THEN 
                raide + bussi * 0.20 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 4 OR hlt.vyoh15 = 10 THEN
                raide + bussi * 0.15 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 837101 OR hlt.vyoh15 = 9993 THEN 
                raide + bussi * 0.5 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            ELSE raide
        END),
        bussi = (CASE
            WHEN hlt.vyoh15 = 1 THEN
                bussi * 0.6
            WHEN hlt.vyoh15 = 2 THEN          
                bussi * 0.7
            WHEN hlt.vyoh15 = 3 THEN 
                bussi * 0.80
            WHEN hlt.vyoh15 = 4 OR hlt.vyoh15 = 10 THEN
                bussi * 0.95
            WHEN hlt.vyoh15 = 837101 OR hlt.vyoh15 = 9993 THEN 
                bussi * 0.5
            ELSE bussi
        END),
        hlauto = (CASE
            WHEN hlt.vyoh15 = 1 THEN
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 2 THEN          
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00033 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 3 THEN 
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 4 OR hlt.vyoh15 = 10 THEN
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh15 = 837101 OR hlt.vyoh15 = 9993 THEN 
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            ELSE hlauto
        END);
        EXECUTE 'SELECT ' || kulkumuoto || ' FROM hlt WHERE vyoh15 = $1' INTO kmuoto_hkmvrk USING vyoh15;
        DROP TABLE IF EXISTS hlt;
    ELSE 
        EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.'||hlt_taulu1||' WHERE vyoh15 = $1' INTO kmuoto_hkmvrk USING vyoh15;
    END IF;

    kmuoto_hkmvrk := (CASE
    WHEN centdist > 2 AND centdist < 10 AND vyoh15 != 4 
        THEN kmuoto_hkmvrk + COALESCE((centdist - 2) * kmuoto_km_muutos, 0)
    WHEN centdist > 2 AND vyoh15 = 4
        THEN(CASE WHEN kmuoto_hkmvrk - COALESCE((centdist - 2) * kmuoto_km_muutos, 0) > 0 THEN kmuoto_hkmvrk - COALESCE((centdist - 2) * kmuoto_km_muutos, 0) ELSE kmuoto_hkmvrk END)
    ELSE kmuoto_hkmvrk END);

    SELECT v_yht * (1-tposuus) * 365 * kmuoto_hkmvrk INTO apliikenne_hkm;
    SELECT tp_yht * tposuus * 365 * kmuoto_hkmvrk INTO tpliikenne_hkm; -- Tilastojen mukaan tehollisia työpäiviä keskimäärin noin 228-230 per vuosi

    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.apliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2'
        INTO apliikenne_kuormitus USING scenario, year;
    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.tpliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2'
        INTO tpliikenne_kuormitus USING scenario, year;

    -------------------------------------------------------------------------------------------
    /* Kulkumuotojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset */
    SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
        INTO kmuoto_kvoima_jakauma
        FROM liikenne.kvoima_kmuoto_jakauma
        WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;
    SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
        INTO kvoima_kwhkm
        FROM liikenne.kvoima_kwhkm
        WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;

    SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(kvoima_kwhkm)) INTO kmuoto_kvoima_mult_res;
    SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(gco2kwh_matrix)) INTO kmuoto_gco2kwh;

    kmuoto_kwhkm := SUM(a) FROM unnest(kmuoto_kvoima_mult_res) a;
   
    -------------------------------------------------------------------------------------------
    /* KWh-laskenta */
    /* KWh calculations */

    SELECT kmuoto_kwhkm * (COALESCE(apliikenne_hkm,0) / apliikenne_kuormitus + COALESCE(tpliikenne_hkm,0) / tpliikenne_kuormitus) INTO hloliikenne_kwh;

    -------------------------------------------------------------------------------------------

    SELECT array(SELECT hloliikenne_kwh * unnest(kmuoto_gco2kwh)) INTO hloliikenne_co2;
        RETURN SUM(a) FROM unnest(hloliikenne_co2) a;
    END IF;
END;
$$ LANGUAGE plpgsql;