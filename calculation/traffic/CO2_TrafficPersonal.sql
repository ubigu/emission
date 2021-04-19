DROP FUNCTION IF EXISTS CO2_TrafficPersonal;

CREATE OR REPLACE FUNCTION public.CO2_TrafficPersonal(
    municipality integer,
    pop integer, -- Population
    employ integer, -- Numer of workplaces
    calculationYear integer,
    mode varchar, -- Mode of transportation
    centdist integer,
    zone integer,
    calculationScenario varchar
) RETURNS real AS $$

DECLARE
trafficTable varchar;
km_muutos_bussi real default 0;
km_muutos_hlauto real default 0;
kmuoto_hkmvrk real;
bussi real ;
hlauto real ;
raide real ;
muu real ;
gco2_output real;

BEGIN 
IF (pop <= 0 OR pop IS NULL)
AND (employ <= 0 OR employ IS NULL)
THEN RETURN 0;

ELSE -------------------------------------------------------------------------------------------
/* Matkat ja kuormitukset */
EXECUTE format('SELECT hlt_table FROM traffic.hlt_lookup WHERE mun::int = %L', municipality)
    INTO trafficTable;

IF zone::int NOT IN (1, 2, 6, 10, 837101) THEN
    EXECUTE FORMAT('SELECT bussi
        FROM traffic.hlt_kmchange WHERE zone = CASE
            WHEN LEFT(%1$L::varchar, 5)::int IN (3, 99931, 99932) THEN 3
            WHEN LEFT(%1$L::varchar, 5)::int IN (4, 99941, 99942) THEN 4
            WHEN LEFT(%1$L::varchar, 5)::int IN (5, 99951, 99952) THEN 5
            ELSE 5 END', zone)
        INTO km_muutos_bussi;

    EXECUTE FORMAT('SELECT hlauto
        FROM traffic.hlt_kmchange WHERE zone = CASE
            WHEN LEFT(%1$L::varchar, 5)::int IN (3, 99931, 99932) THEN 3
            WHEN LEFT(%1$L::varchar, 5)::int IN (4, 99941, 99942) THEN 4
            WHEN LEFT(%1$L::varchar, 5)::int IN (5, 99951, 99952) THEN 5
            ELSE 5 END', zone)
        INTO km_muutos_hlauto;
END IF;

EXECUTE FORMAT('SELECT bussi
FROM traffic.%1$I
WHERE zone = CASE
    WHEN LEFT(%2$L::varchar, 4)::int = 9990 THEN 10
            WHEN LEFT(%2$L::varchar, 3)::int = 999
                THEN left(right(%2$L, 2), 1)::int
            WHEN %2$L::int = 837101 THEN 6
            ELSE %2$L::int END
', trafficTable, zone) INTO bussi;

EXECUTE FORMAT('SELECT raide
FROM traffic.%1$I
WHERE zone = CASE 
    WHEN LEFT(%2$L::varchar, 4)::int = 9990 THEN 10
            WHEN LEFT(%2$L::varchar, 3)::int = 999
                THEN left(right(%2$L, 2), 1)::int
            WHEN %2$L::int = 837101 THEN 6
            ELSE %2$L::int END
', trafficTable, zone) INTO raide;

EXECUTE FORMAT('SELECT hlauto
FROM traffic.%1$I
WHERE zone = CASE 
    WHEN LEFT(%2$L::varchar, 4)::int = 9990 THEN 10
            WHEN LEFT(%2$L::varchar, 3)::int = 999
                THEN left(right(%2$L, 2), 1)::int
            WHEN %2$L::int = 837101 THEN 6
            ELSE %2$L::int END
', trafficTable, zone) INTO hlauto;

EXECUTE FORMAT('SELECT muu
FROM traffic.%1$I
WHERE zone = CASE 
    WHEN LEFT(%2$L::varchar, 4)::int = 9990 THEN 10
            WHEN LEFT(%2$L::varchar, 3)::int = 999
                THEN left(right(%2$L, 2), 1)::int
            WHEN %2$L::int = 837101 THEN 6
            ELSE %2$L::int END
', trafficTable, zone) INTO muu;

bussi := (
    CASE
        WHEN centdist > 2 AND centdist < 10
            AND zone IN (3, 5, 99931, 99932, 99951, 99952, 81, 82, 83, 84, 85, 86, 87)
            THEN COALESCE(bussi + (centdist - 2) * km_muutos_bussi, 0)
        WHEN centdist > 2
            AND zone IN (4, 99941, 99942)
        THEN COALESCE(bussi - (centdist - 2) * km_muutos_bussi, 0)
    ELSE bussi END
);

hlauto := (
    CASE
        WHEN centdist > 2 AND centdist < 10
            AND zone IN (3, 5, 99931, 99932, 99951, 99952, 81, 82, 83, 84, 85, 86, 87)
            THEN COALESCE(hlauto + (centdist - 2) * km_muutos_hlauto, 0)
        WHEN centdist > 2 AND zone IN (4, 99941, 99942)
            THEN COALESCE(hlauto - (centdist - 2) * km_muutos_hlauto, 0)
    ELSE hlauto END
);

IF mode = 'raide' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901)
            THEN $3 + $4 * 0.4 + ($5 * (1.01 ^ ($2 - RIGHT($1::varchar,4)::int + 1)) - $5)
        WHEN LEFT($1::varchar,5)::int IN (99912, 99922, 99932, 99942, 99952, 99962, 99902)
            THEN $3 + $4 * 0.5 + ($5 * (1.01 ^ ($2 - RIGHT($1::varchar,4)::int + 1)) - $5)
        ELSE $3 END'
        INTO kmuoto_hkmvrk
        USING zone, calculationYear, raide, bussi, hlauto;
ELSIF mode = 'bussi' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901)
            THEN $2 * 0.6
        WHEN LEFT($1::varchar,5)::int IN (99912, 99922, 99932, 99942, 99952, 99962, 99902)
            THEN $2 * 0.5
        ELSE $2 END'
        INTO kmuoto_hkmvrk
        USING zone, bussi;
ELSIF mode = 'hlauto' THEN
    EXECUTE 'SELECT CASE WHEN LEFT($1::varchar,5)::int IN (99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902) THEN
        $3 * (0.99 ^ ($2 - RIGHT($1::varchar,4)::int + 1))
    ELSE $3 END'
    INTO kmuoto_hkmvrk USING zone,
    calculationYear, hlauto;
ELSIF mode = 'muu' THEN
    kmuoto_hkmvrk := muu;
END IF;

-- mode_power_distribution: Kulkumuotojen käyttövoimajakauma.
-- power_kwhkm: Energian keskikulutus käyttövoimittain [kWh/km].
 EXECUTE FORMAT(
    'WITH RECURSIVE
    power_distribution AS (
        SELECT
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut]
                as distribution
            FROM traffic.mode_power_distribution
                WHERE year = %1$L
                    AND scenario = %2$L::varchar
                    AND mun = %3$L::int
                    AND kmuoto = %4$L::varchar
    ), power_kwhkm as (
        SELECT 
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut] as kwhkm
            FROM traffic.power_kwhkm
                WHERE year = %1$L
                    AND scenario = %2$L
                    AND mun::int = %3$L
                    AND kmuoto = %4$L LIMIT 1
    ), 
    -- Kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] määritellään
    -- käyttövoimien ominaispäästökertoimien suoriteosuuksilla painotettuna keskiarvona huomioiden samalla niiden bio-osuudet.
    fossils as (
        -- Käyttövoimien fossiiliset osuudet [ei yksikköä].
        SELECT array[share, 1, share, 1, share, share, 1, 1, share] as share
            FROM traffic.power_fossil_share pfs
                WHERE pfs.year = %1$L
                AND pfs.scenario =  %2$L LIMIT 1
    ), electricity_gco2kwh AS (
        -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh]
        SELECT el.gco2kwh::int AS gco2
        FROM energy.electricity el
            WHERE el.year::int = %1$s::int
            AND el.scenario::varchar = %2$L
            AND el.metodi = ''em''
            AND el.paastolaji = ''tuotanto'' LIMIT 1
    ), gco2kwh_matrix as (
        SELECT 
        -- Käyttövoimien  kasvihuonekaasujen ominaispäästökerroin käytettyä energiayksikköä kohti [gCO2-ekv/kWh].
        array( SELECT 
            -- Dummy, jolla huomioidaan sähkön käyttö sähköautoissa, pistokehybrideissä ja polttokennoautojen vedyn tuotannossa [ei yksikköä].
            el.gco2 * unnest(array[0, 0, 0, 0, 0.5, 0.5, 1, 2.5, 0]) +
            -- phev_b ja phev_d saavat bensiinin ja dieselin ominaispäästöt = 241 & 237 gco2/kwh
            -- Etanolin päästöt vuonna 2017 = 49, tuotannon kehityksen myötä n. 0.8 parannus per vuosi
            unnest(array[241, (49 - (%1$s::int - 2017) * 0.8), 237, 80, 241, 237, 0, 0, 189]) *
            unnest(fossils.share) *
            unnest(array[1, 1, 1, 1, 0.5, 0.5, 0, 0, 1])
        ) as arr FROM electricity_gco2kwh el, fossils
    ), co2_km as (
        SELECT 
            unnest(distribution) * unnest(kwhkm) * unnest(gco2kwh_matrix.arr) as gco2km
        FROM power_distribution, power_kwhkm, gco2kwh_matrix
    ), load_personal as (
        SELECT %4$I as load_p
            FROM traffic.%9$I
                WHERE year::int = %1$L::int
                    AND scenario = %2$L
                    AND mun::int = %3$L::int
                LIMIT 1
    ), load_work as (
        SELECT %4$I as load_w
            FROM traffic.%10$I
                WHERE year::int = %1$L::int
                    AND scenario = %2$L
                    AND mun::int = %3$L::int
                LIMIT 1
    ), work_share as (
        SELECT %4$I as share_w
        FROM traffic.%8$I
        WHERE zone = CASE
            WHEN LEFT(%5$L::varchar, 4)::int = 9990 THEN 10
            WHEN LEFT(%5$L::varchar, 3)::int = 999
                THEN left(right(%5$L, 2), 1)::int
            WHEN %5$s::int = 837101 THEN 6
            WHEN %5$s::int IN (81,82,83,84,85,86,87) THEN 5
            ELSE %5$s::int END LIMIT 1
    ), distance as (
        SELECT CASE WHEN %7$s::int = 0 THEN
            (%6$s::int * %12$L::int * %11$L::real)::real
        ELSE 
            COALESCE(%6$s::int * (1 - COALESCE(share_w, 0.1))::real * %12$L::int * %11$L::real, 0) / load_p::real + 
            COALESCE(%7$s::int * COALESCE(share_w, 0.1)::real * %13$L::int * %11$L::real, 0) / load_w::real
        END as km
        FROM load_personal, load_work, work_share
    )
    SELECT SUM(gco2km * km)::real
    FROM co2_km, distance',
        calculationYear, -- 1
        calculationScenario, -- 2
        municipality, -- 3 
        mode, -- 4
        zone, -- 5
        COALESCE(pop,0), -- 6
        COALESCE(employ,0), -- 7
        'hlt_workshare', -- 8
        'citizen_traffic_stress', -- 9,  asukkaiden kulkumuotojen keskikuormitukset laskentavuonna [hkm/km].
        'workers_traffic_stress', -- 10, työssäkäyvien kulkumuotojen keskikuormitus laskentavuonna [hkm/km].
        kmuoto_hkmvrk, -- 11
        365, -- 12
        365 -- 13 - Tilastojen mukaan tehollisia työpäiviä keskimäärin noin 228-230 per vuosi ?
    ) INTO gco2_output;

RETURN gco2_output;

END IF;

END;

$$ LANGUAGE plpgsql;