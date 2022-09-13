 WITH RECURSIVE
    power_distribution AS (
        SELECT
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut] as distribution
            FROM traffic.mode_power_distribution
                WHERE year = 2025 AND scenario = 'wem' AND mun = '837' AND kmuoto = 'hlauto' limit 1
    ), power_kwhkm as (
        SELECT 
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut] as kwhkm
            FROM traffic.power_kwhkm
                WHERE year = 2025 AND scenario = 'wem'  AND mun = '837' AND kmuoto = 'hlauto' limit 1
    ), 
    -- Kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] määritellään
    -- käyttövoimien ominaispäästökertoimien suoriteosuuksilla painotettuna keskiarvona huomioiden samalla niiden bio-osuudet.
    fossils as (
        -- Käyttövoimien fossiiliset osuudet [ei yksikköä].
        SELECT array[share, 1, share, 1, share, share, 1, 1, share] as share
            FROM traffic.power_fossil_share pfs
                WHERE pfs.year = 2025
                AND pfs.scenario =  'wem' limit 1
    ), electricity_gco2kwh AS (
        -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh]
        SELECT el.gco2kwh::int AS gco2
        FROM energia.sahko el
            WHERE el.vuosi = 2025
            AND el.skenaario = 'wem'
            AND el.metodi = 'em'
            AND el.paastolaji = 'tuotanto' limit 1
    ),
    gco2kwh_matrix as (
        SELECT 
        -- Käyttövoimien  kasvihuonekaasujen ominaispäästökerroin käytettyä energiayksikköä kohti [gCO2-ekv/kWh].
        array( SELECT el.gco2 *
            -- Dummy, jolla huomioidaan sähkön käyttö sähköautoissa, pistokehybrideissä ja polttokennoautojen vedyn tuotannossa [ei yksikköä].
            unnest(array[0, 0, 0, 0, 0.5, 0.5, 1, 2.5, 0]) +
            -- phev_b ja phev_d saavat bensiinin ja dieselin ominaispäästöt = 241 & 237 gco2/kwh
            -- Etanolin päästöt vuonna 2017 = 49, tuotannon kehityksen myötä n. 0.8 parannus per vuosi
            unnest(array[241, (49 - (2025 - 2017) * 0.8), 237, 80, 241, 237, 0, 0, 189]) *
            unnest(fossils.share) *
            unnest(array[1, 1, 1, 1, 0.5, 0.5, 0, 0, 1])
        ) as arr FROM electricity_gco2kwh el, fossils
    ),
    co2_km as (
        SELECT 
            unnest(distribution) * unnest(kwhkm) * unnest(distribution) * unnest(gco2kwh_matrix.arr) as gco2km 
        FROM power_distribution, power_kwhkm, gco2kwh_matrix
    ), load_personal as (
        SELECT hlauto as load_p FROM liikenne.apliikenne_kuormitus_new WHERE year = 2025 AND scenario = 'wem' AND mun = '837' limit 1
    ), load_work as (
        SELECT hlauto as load_w FROM liikenne.tpliikenne_kuormitus_new WHERE year = 2025 AND scenario = 'wem' AND mun = '837' limit 1
    ), work_share as (
        SELECT hlauto as share_w FROM traffic.hlt_workshare
        WHERE zone = CASE 
            WHEN LEFT('99921'::varchar, 5)::int IN (99911, 99912) THEN 1
            WHEN LEFT('99921'::varchar, 5)::int IN (99921, 99922) THEN 2
            WHEN LEFT('99921'::varchar, 5)::int IN (99931, 99932) THEN 3
            WHEN LEFT('99921'::varchar, 5)::int IN (99941, 99942) THEN 4
            WHEN LEFT('99921'::varchar, 5)::int IN (99951, 99952) THEN 5
            WHEN LEFT('99921'::varchar, 5)::int IN (99961, 99962) THEN 6
            WHEN LEFT('99921'::varchar, 5)::int IN (99901, 99902) THEN 10
            ELSE '99921'::int END
    ), distance as (
        SELECT COALESCE(300 * (1 - share_w) * 365 * 50, 0) / load_p + 
            COALESCE(100 * share_w * 365 * 30, 0) / load_w as km
        FROM load_personal, load_work, work_share
    )
    SELECT SUM(gco2km * km) * 0.000001
    FROM co2_km kwh, distance