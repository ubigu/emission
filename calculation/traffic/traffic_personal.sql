DROP FUNCTION IF EXISTS il_traffic_personal_co2_new;

CREATE
OR REPLACE FUNCTION public.il_traffic_personal_co2_new(
    v_yht integer,
    tp_yht integer,
    year integer,
    kulkumuoto varchar,
    centdist integer,
    vyoh integer,
    area varchar,
    scenario varchar,
    gco2kwh_matrix real []
) RETURNS real AS $$ DECLARE hlt_taulu1 varchar;

tposuus real;

km_muutos_bussi real;
km_muutos_hlauto real;
kmuoto_gco2kwh real [];
-- Kulkumuotojen kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] 
kmuoto_hkmvrk real;
kmuoto_kvoima_jakauma real [];
-- Kulkumuotojen käyttövoimajakaumaa. Arvot riippuvat taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
kvoima_kwhkm real [];
-- Käyttövoimien energian keskikulutus [kWh/km]. Arvot riippuvat taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
kmuoto_kvoima_mult_res real [];
apliikenne_kuormitus real;
-- Asukkaiden kulkumuotojen keskimääräiset kuormitukset laskentavuonna [hkm/km]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja kulkumuodosta
tpliikenne_kuormitus real;
-- Työssä käyvien kulkumuotojen keskimääräiset kuormitukset laskentavuonna [hkm/km]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja kulkumuodosta
kmuoto_kwhkm real;
-- Energian keskikulutus käyttövoimittain [kWh/km]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
apliikenne_hkm real;
-- Asukkaiden kulkumuodoilla jalkapyora, bussi, raide, hlauto ja muu vuorokauden aikana tekemien matkojen keskimääräiset pituudet henkilökilometreinä [hkm/as/vrk]. Arvo riippuu laskentavuodesta, kulkumuodosta, yhdyskuntarakenteen vyöhykkeestä ja tarkastelualueesta.
tpliikenne_hkm real;
--  Ruudussa työssä käyvien kulkumuodoilla jalkapyora, bussi, raide, hlauto ja muu vuorokauden aikana tekemien matkojen keskimääräiset pituudet henkilökilometreinä [hkm/as/vrk]. Arvo riippuu laskentavuodesta, kulkumuodosta, yhdyskuntarakenteen vyöhykkeestä ja  ja tarkastelualueesta.
hloliikenne_kwh real;
hloliikenne_co2 real [];
bussi real ;
hlauto real ;
raide real ;
muu real ;

BEGIN IF (
    v_yht <= 0
    OR v_yht IS NULL
)
AND (
    tp_yht <= 0
    OR tp_yht IS NULL
) THEN RETURN 0;

ELSE -------------------------------------------------------------------------------------------
/* Matkat ja kuormitukset */
EXECUTE format(
    'SELECT hlt FROM aluejaot.alueet WHERE kunta = %L OR maakunta = %L',
    area,
    area
) INTO hlt_taulu1;

IF vyoh::int IN (99931, 99941, 99951, 99932, 99942, 99952, 3, 4, 5) THEN
    EXECUTE 'SELECT bussi
        FROM liikenne.hlt_kmmuutos WHERE vyoh = CASE
            WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
            WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
            WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5 ELSE $1::int END'
        INTO km_muutos_bussi USING vyoh;

    EXECUTE 'SELECT hlauto
        FROM liikenne.hlt_kmmuutos WHERE vyoh = CASE
            WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
            WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
            WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5 ELSE $1::int END'
        INTO km_muutos_hlauto USING vyoh;
ELSE 
    km_muutos_hlauto := 0;
    km_muutos_bussi := 0;
END IF;

EXECUTE 'SELECT ' || kulkumuoto || '
FROM liikenne.hlt_tposuus WHERE vyoh = CASE 
        WHEN LEFT($1::varchar,5)::int IN (99911, 99912) THEN 1
        WHEN LEFT($1::varchar,5)::int IN (99921, 99922) THEN 2
        WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
        WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
        WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5
        WHEN LEFT($1::varchar,5)::int IN (99961, 99962) THEN 6
        WHEN LEFT($1::varchar,5)::int IN (99901, 99902) THEN 10
    ELSE $1::int END' INTO tposuus USING vyoh;

EXECUTE 'SELECT bussi
FROM liikenne.' || hlt_taulu1 || '
WHERE vyoh = CASE 
    WHEN LEFT($1::varchar,5)::int IN (99911, 99912) THEN 1
    WHEN LEFT($1::varchar,5)::int IN (99921, 99922) THEN 2
    WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
    WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
    WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5
    WHEN LEFT($1::varchar,5)::int IN (99961, 99962) THEN 6
    WHEN LEFT($1::varchar,5)::int IN (99901, 99902) THEN 10
ELSE $1::int END' INTO bussi USING vyoh;

EXECUTE 'SELECT raide
FROM liikenne.' || hlt_taulu1 || '
WHERE vyoh = CASE 
    WHEN LEFT($1::varchar,5)::int IN (99911, 99912) THEN 1
    WHEN LEFT($1::varchar,5)::int IN (99921, 99922) THEN 2
    WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
    WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
    WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5
    WHEN LEFT($1::varchar,5)::int IN (99961, 99962) THEN 6
    WHEN LEFT($1::varchar,5)::int IN (99901, 99902) THEN 10
ELSE $1::int END' INTO raide USING vyoh;

EXECUTE 'SELECT hlauto
FROM liikenne.' || hlt_taulu1 || '
WHERE vyoh = CASE 
    WHEN LEFT($1::varchar,5)::int IN (99911, 99912) THEN 1
    WHEN LEFT($1::varchar,5)::int IN (99921, 99922) THEN 2
    WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
    WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
    WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5
    WHEN LEFT($1::varchar,5)::int IN (99961, 99962) THEN 6
    WHEN LEFT($1::varchar,5)::int IN (99901, 99902) THEN 10
ELSE $1::int END' INTO hlauto USING vyoh;

EXECUTE 'SELECT muu
FROM liikenne.' || hlt_taulu1 || '
WHERE vyoh = CASE 
    WHEN LEFT($1::varchar,5)::int IN (99911, 99912) THEN 1
    WHEN LEFT($1::varchar,5)::int IN (99921, 99922) THEN 2
    WHEN LEFT($1::varchar,5)::int IN (99931, 99932) THEN 3
    WHEN LEFT($1::varchar,5)::int IN (99941, 99942) THEN 4
    WHEN LEFT($1::varchar,5)::int IN (99951, 99952) THEN 5
    WHEN LEFT($1::varchar,5)::int IN (99961, 99962) THEN 6
    WHEN LEFT($1::varchar,5)::int IN (99901, 99902) THEN 10
ELSE $1::int END' INTO muu USING vyoh;

bussi := (
    CASE
        WHEN centdist > 2 AND centdist < 10 AND vyoh IN (3, 5, 99931, 99932, 99951, 99952) THEN bussi + COALESCE((centdist - 2) * km_muutos_bussi, 0)
        WHEN centdist > 2 AND vyoh IN (4, 99941, 99942) THEN(
            CASE WHEN bussi - COALESCE((centdist - 2) * km_muutos_bussi, 0) > 0 THEN bussi - COALESCE((centdist - 2) * km_muutos_bussi, 0) ELSE bussi END
        ) ELSE bussi
    END
);

hlauto := (
    CASE
        WHEN centdist > 2 AND centdist < 10 AND vyoh IN (3, 5, 99931, 99932, 99951, 99952) THEN hlauto + COALESCE((centdist - 2) * km_muutos_hlauto, 0)
        WHEN centdist > 2 AND vyoh IN (4, 99941, 99942) THEN(
            CASE WHEN hlauto - COALESCE((centdist - 2) * km_muutos_hlauto, 0) > 0 THEN hlauto - COALESCE((centdist - 2) * km_muutos_hlauto, 0) ELSE hlauto END
        ) ELSE hlauto
    END
);

IF kulkumuoto = 'raide' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901) THEN
            $3 + $4 * 0.4 + ($5 * (1.01 ^ ($2 - RIGHT($1::varchar,4)::int + 1)) - $5)
        WHEN LEFT($1::varchar,5)::int IN (99912, 99922, 99932, 99942, 99952, 99962, 99902) THEN
            $3 + $4 * 0.5 + ($5 * (1.01 ^ ($2 - RIGHT($1::varchar,4)::int + 1)) - $5)
        ELSE $3 END' INTO kmuoto_hkmvrk USING vyoh, year, raide, bussi, hlauto;
ELSIF kulkumuoto = 'bussi' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901) THEN
            $2 * 0.6
        WHEN LEFT($1::varchar,5)::int IN (99912, 99922, 99932, 99942, 99952, 99962, 99902) THEN
            $2 * 0.5
        ELSE $2 END' INTO kmuoto_hkmvrk USING vyoh, bussi;
ELSIF kulkumuoto = 'hlauto' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902) THEN
        $3 * (0.99 ^ ($2 - RIGHT($1::varchar,4)::int + 1))
    ELSE $3 END' INTO kmuoto_hkmvrk USING vyoh, year, hlauto;
ELSIF kulkumuoto = 'muu' THEN
    kmuoto_hkmvrk := muu;
END IF;


/* muunto vuorokausisuoritteesta vuositasolle [vrk/a] (365) */
SELECT v_yht * (1 - tposuus) * 365 * kmuoto_hkmvrk INTO apliikenne_hkm;

SELECT tp_yht * tposuus * 365 * kmuoto_hkmvrk INTO tpliikenne_hkm;

-- Tilastojen mukaan tehollisia työpäiviä keskimäärin noin 228-230 per vuosi
EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.apliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2' INTO apliikenne_kuormitus USING scenario, year;

EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.tpliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2' INTO tpliikenne_kuormitus USING scenario, year;

-------------------------------------------------------------------------------------------
/* Kulkumuotojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset */
SELECT
    array [bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu] INTO kmuoto_kvoima_jakauma
FROM
    liikenne.kvoima_kmuoto_jakauma
WHERE
    vuosi = year
    AND skenaario = scenario
    AND kmuoto = kulkumuoto;

SELECT
    array [bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu] INTO kvoima_kwhkm
FROM
    liikenne.kvoima_kwhkm
WHERE
    vuosi = year
    AND skenaario = scenario
    AND kmuoto = kulkumuoto;

SELECT
    array(
        SELECT
            unnest(kmuoto_kvoima_jakauma) * unnest(kvoima_kwhkm)
    ) INTO kmuoto_kvoima_mult_res;

SELECT
    array(
        SELECT
            unnest(kmuoto_kvoima_jakauma) * unnest(gco2kwh_matrix)
    ) INTO kmuoto_gco2kwh;

kmuoto_kwhkm := SUM(a)
FROM
    unnest(kmuoto_kvoima_mult_res) a;

-------------------------------------------------------------------------------------------
/* KWh-laskenta */
/* KWh calculations */
SELECT
    kmuoto_kwhkm * (
        COALESCE(apliikenne_hkm, 0) / apliikenne_kuormitus + COALESCE(tpliikenne_hkm, 0) / tpliikenne_kuormitus
    ) INTO hloliikenne_kwh;

-------------------------------------------------------------------------------------------
SELECT
    array(
        SELECT
            hloliikenne_kwh * unnest(kmuoto_gco2kwh)
    ) INTO hloliikenne_co2;

RETURN SUM(a)
FROM
    unnest(hloliikenne_co2) a;

END IF;

END;

$$ LANGUAGE plpgsql;