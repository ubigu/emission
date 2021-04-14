/* Tavaraliikenne palvelu- ja teollisuuden rakennuksiin
Goods traffic for service and industry buildings

Palvelurakennukset luokitellaan kasvihuonekaasupäästölaskelmissa myymälärakennuksiin (myymal_), majoitusliikerakennuksiin (majoit),
asuntolarakennuksiin (asla), ravintoloihin ja ruokaloihin (ravint), toimistorakennuksiin (tsto), liikenteen rakennuksiin (liiken),
hoitoalan rakennuksiin (hoito), kookoontumisrakennuksiin (kokoon), opetusrakennuksiin (opetus) ja muihin rakennuksiin (muut).
Teollisuus- ja varastorakennuksiin sisältyvät teollisuusrakennukset (teoll) ja varastorakennukset (varast).
*/

/*  Test : 
    SELECT co2_traffic_iwhs_co2('837', 2021::int, 'wem', 3000::int, 'myymal_hyper', array[23, 53, 25, 43, 66, 22, 11, 5, 4])
*/
DROP FUNCTION IF EXISTS CO2_TrafficIWHS;
CREATE OR REPLACE FUNCTION
public.CO2_TrafficIWHS(
    municipality integer, -- Municipality, for which the values are calculated
    calculationYear integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    calculationScenario varchar, -- PITKO:n mukainen kehitysskenaario
    floorSpace integer, -- Rakennusten kerrosala tai lukumäärä (vain teoll ja varast - tapauksissa)
    buildingType varchar -- buildingType | Building type. esim. | e.g. 'erpien', 'rivita'
)
RETURNS real AS
$$
DECLARE
    services varchar[] default ARRAY['myymal_hyper', 'myymal_super', 'myymal_pien', 'myymal_muu', 'myymal', 'majoit', 'asla', 'ravint', 'tsto', 'liiken', 'hoito', 'kokoon', 'opetus', 'muut'];
    gco2_output real;

BEGIN
    IF  floorSpace <= 0
        OR floorSpace IS NULL
        THEN RETURN 0;
    ELSE
    
    /*  Tavarakuljetusten vuosisuorite palv_km [km/a] on laskentavuonna
            palv_km = rakennus_ala * muunto_ala_tliikenne * palv_suorite * palv_kuljetus_km * arkipaivat
        Paketti- ja kuorma-autojen käyttövoimien suoriteosuuksilla painotettu keskikulutus kmuoto_kwhkm [kWh/km] lasketaan
            kmuoto_kwhkm = mode_power_distribution * kvoima_kwhkm
        Paketti- ja kuorma-autoilla tehdyn tavaraliikenteen vuosittainen energian käyttö [kWh/a] lasketaan kaavoilla
            ptv_liikenne_kwh = ptv_km * kmuoto_kwhkm
        Laskentavuoden palvelu- ja teollisuusrakennusten paketti- ja kuorma-autojen tavarakuljetussuoritteiden aiheuttamat kasvihuonekaasupäästöt [CO2-ekv/a] ovat
            ptv_liikenne_co2 = ptvliikenne_palv_kwh * kmuoto_gco2kwh
    */

    EXECUTE FORMAT(
        'WITH RECURSIVE
        mode_power_distribution AS
        (SELECT kmuoto,
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut] as distribution
            FROM traffic.mode_power_distribution
                WHERE year = %1$L
                    AND scenario = %2$L
                    AND mun = %3$L
                    AND kmuoto = ANY(%4L)
        ), power_kwhkm as (
        SELECT kmuoto,
            array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety, kvoima_muut] as kwhkm
            FROM traffic.power_kwhkm
                WHERE year = %1$L AND scenario = %2$L AND mun = %3$L AND kmuoto = ANY(%4$L)
        ),
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
                WHERE el.year = %1$L
                AND el.scenario = %2$L
                AND el.metodi = ''em''
                AND el.paastolaji = ''tuotanto'' LIMIT 1
        ),
        gco2kwh_matrix as (
            SELECT 
            -- Käyttövoimien  kasvihuonekaasujen ominaispäästökerroin käytettyä energiayksikköä kohti [gCO2-ekv/kWh].
            array( SELECT el.gco2 *
                -- Dummy, jolla huomioidaan sähkön käyttö sähköautoissa, pistokehybrideissä ja polttokennoautojen vedyn tuotannossa [ei yksikköä].
                unnest(array[0, 0, 0, 0, 0.5, 0.5, 1, 2.5, 0]) +
                -- phev_b ja phev_d saavat bensiinin ja dieselin ominaispäästöt = 241 & 237 gco2/kwh
                -- Etanolin päästöt vuonna 2017 = 49, tuotannon kehityksen myötä n. 0.8 parannus per vuosi
                unnest(array[241, (49 - (%1$L - 2017) * 0.8), 237, 80, 241, 237, 0, 0, 189]) *
                unnest(fossils.share) *
                unnest(array[1, 1, 1, 1, 0.5, 0.5, 0, 0, 1])
            ) as arr FROM electricity_gco2kwh el, fossils
        ),
        kwh_distribution as (
            SELECT a.kmuoto,
                unnest(distribution) * unnest(kwhkm) as kwh,
                unnest(gco2kwh_matrix.arr) as gco2
            FROM mode_power_distribution a
                NATURAL JOIN power_kwhkm b,
                gco2kwh_matrix
        ),
        distance as 
        -- Apply polynomial regression for estimating size to visits ratio of industry, scale suorite to industry-type weighted average (13.5)
        -- According to Turunen V. TAVARALIIKENTEEN MALLINTAMISESTA HELSINGIN SEUDULLA,
        -- These original traffic estimates create approximately twice the observed amount. Thus half everything.
        (SELECT f.kmuoto, f.%5$I::real *
            CASE WHEN %8$L = ''industr_performance'' THEN d.%5$I / (CASE WHEN %5$L != ''varast'' THEN 13.5 ELSE 46 END)
                * (0.000000000245131* %6$L^2 -0.000026867899351 * %6$L + 0.801629386363636) * 0.01
                ELSE d.%5$I * 0.01 END
            * %9$L as km
        FROM traffic.%7$I f
            LEFT JOIN traffic.%8$I d
            on d.kmuoto = f.kmuoto and d.year = f.year and d.scenario = f.scenario and d.mun::int = f.mun::int
            WHERE f.kmuoto = ANY(%4$L) AND f.year = %1$L AND f.scenario = %2$L AND f.mun::int = %3$L)
        SELECT sum(kwh * gco2 * km / 2) * %6$L
        FROM kwh_distribution kwh
            NATURAL JOIN distance',
    calculationYear, -- 1
    calculationScenario, -- 2
    municipality, -- 3 
    ARRAY['kauto', 'pauto'], -- 4
    buildingType, -- 5
    floorSpace::real,
     -- 6 - muuntaa kerrosneliömetrit sadoiksi kerrosneliömetreiksi (0.01)
    CASE WHEN buildingType = ANY(services) THEN 'services_transport_km' ELSE 'industr_transport_km' END, -- 7
    CASE WHEN buildingType = ANY(services) THEN 'service_performance' ELSE 'industr_performance' END, -- 8
    260) INTO gco2_output; -- 9 - Arkipäivien lukumäärä vuodessa (260) [vrk/a].
    
    RETURN gco2_output;

    END IF;
END;
$$ LANGUAGE plpgsql;