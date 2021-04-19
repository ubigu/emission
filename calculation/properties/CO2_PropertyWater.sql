/* Käyttöveden lämmitys | Heating of water

YKR-ruudun rakennusten vuoden aikana kuluttaman lämpimän käyttöveden lämmitystarve vesi_lammitystarve [kWh/a] on:
    vesi_lammitystarve = rakennus_ala * vesi_kwhm2

Lämpimän käyttöveden lämmittämiseen tarvittavan ostoenergian tarve vesi_kwh [kWh/a] on:
    vesi_kwh =  vesi_lammitystarve / (tilat_hyotysuhde - 0.05)

buildingTypeen käyttöveden lämmityksen tarvitseman ostoenergian kasvihuonekaasupäästöt vesi_co2 [CO2-ekv/a] ovat YKR-ruudussa:
    vesi_co2 = vesi_kwh * (lmuoto_apu1 * klampo_gco2kwh + lmuoto_apu2 * sahko_gco2kwh + lmuoto_apu3 * tilat_gco2kwh)
    
*/

DROP FUNCTION IF EXISTS CO2_PropertyWater;
CREATE OR REPLACE FUNCTION
public.CO2_PropertyWater(
	municipality int,
    calculationYear integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    calculationScenario varchar, -- PITKO:n mukainen kehitysskenaario
    floorSpace int, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta sekä rakennuksen tyypistä ja ikäluokasta [m2]
    buildingType varchar, -- buildingType, esim. 'erpien', 'rivita'
    buildingYear integer, -- buildingYearkymmen tai -vuosi (2017 alkaen)
    method varchar, 
    heatSource varchar default null -- Rakennuksen lämmityksessä käytettävä primäärinen energiamuoto 'energiam', mikäli tällainen on lisätty YKR/rakennusdataan
)
RETURNS real AS
$$
DECLARE -- Joillekin muuttujille on sekä yksittäiset että array-tyyppiset muuttujat, riippuen siitä, onko lähtödatana YKR-dataa (array) vai paikallisesti jalostettua rakennusdataa
    vesi_kwhm2 real; -- Rakennustyypin ikäluokan kerrosalaa kohti vesikuutiometrittäin lasketun lämpimän käyttöveden ominaiskulutuksen [m3/m2/a] ja yhden vesikuution lämmittämiseen tarvittavan energiamäärän 58,3 kWh/m3 tulo. Arvo riippuu laskentaskenaariosta sekä rakennuksen ikäluokasta ja tyypistä [kWh/m2,a].
    hyotysuhde real; -- Rakennustyypin ikäluokan lämmitysjärjestelmäkohtainen keskimääräinen vuosihyötysuhde tai lämpökerroin. Lukuarvo riippuu rakennuksen ikäluokasta, tyypistä ja lämmitysmuodosta [ei yksikköä].
    hyotysuhde_a real[]; -- Rakennustyypin ikäluokan lämmitysjärjestelmäkohtainen keskimääräinen vuosihyötysuhde tai lämpökerroin. Lukuarvo riippuu rakennuksen ikäluokasta, tyypistä ja lämmitysmuodosta [ei yksikköä].
    vesi_kwh real; -- Veden lämmityksen energiankulutus
    vesi_kwh_a real[]; -- Veden lämmityksen energiankulutus
    vesi_co2_a real[]; -- Veden lämmityksen kasvihuonekaasupäästöt [gCO2]
    lammitysosuus real[]; -- Lämmitysmuotojen keskimääräiset osuudet rakennustyypin kerrosalasta tietyssä ikäluokassa laskentavuonna. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, rakennuksen ikäluokasta ja tyypistä.
    gco2kwh_a real[];
    gco2kwh real; -- Käytettävä ominaispäästökerroin
BEGIN
    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF floorSpace <= 0 OR floorSpace IS NULL
        THEN RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE

        /* Käytetään kun on johdettu paikallisesta aineistosta lämmitys/energiamuototiedot ruututasolle */
        /* Used when local building register data has been used to derive grid level information incl. heating methods of building */
        IF heatSource IS NOT NULL THEN
            EXECUTE FORMAT('
                SELECT %1$I::real
                    FROM built.spaces_efficiency
                    WHERE rakennus_tyyppi = %2$L
                        AND rakv::int = %3$s::int
                        AND mun::int = %5$s::int LIMIT 1
                ', heatSource, buildingType, buildingYear, calculationScenario, municipality
            ) INTO hyotysuhde;
        ELSE

        /* Käytetään kun käytössä on pelkkää YKR-dataa */
        /* Used when basing the analysis on pure YKR data */
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO lammitysosuus
                    FROM built.distribution_heating_systems
                        WHERE scenario = calculationScenario
                        AND rakennus_tyyppi = buildingType
                        AND rakv = buildingYear
                        AND year = calculationYear;
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO hyotysuhde_a
                    FROM built.spaces_efficiency
                        WHERE rakennus_tyyppi = buildingType
                        AND rakv = buildingYear;
        END IF;

        /* Veden lämmityksen energiankulutus per kerrosneliö */
        /* Water heating power consumption per floor space, square meter */
        SELECT vesi_kwh_m2 INTO vesi_kwhm2
            FROM built.water_kwhm2 vesi_kwhm2
            WHERE vesi_kwhm2.scenario = calculationScenario
            AND vesi_kwhm2.rakennus_tyyppi = buildingType
            AND vesi_kwhm2.rakv = buildingYear;
    
        /* Kaukolämmön ominaispäästökertoimet  */
        /* Emission values for district heating (first finding out the name of the correct district heating table) */
        EXECUTE FORMAT(
            'WITH district_heating AS (
                SELECT %3$I as gco2kwh
                FROM energy.district_heating heat
                WHERE heat.year = %1$L
                AND heat.scenario = %2$L
                AND heat.mun::int = %4$L::int
            ), electricity AS (
                SELECT el.gco2kwh::int AS gco2kwh
                FROM energy.electricity el
                    WHERE el.year = %1$L
                    AND el.scenario = %2$L
                    AND el.metodi = ''em''
                    AND el.paastolaji = ''tuotanto''
            ), spaces AS (
                SELECT array[kevyt_oljy, raskas_oljy, kaasu, puu, turve, hiili, muu_lammitys] as gco2kwh
                FROM energy.spaces_gco2kwh t
                WHERE t.vuosi = %1$L LIMIT 1
            ) SELECT
                array[
                    district_heating.gco2kwh, -- kaukolampö
                    spaces.gco2kwh[1], -- kevyt_oljy
                    spaces.gco2kwh[2], -- raskas_oljy
                    spaces.gco2kwh[3], -- kaasu
                    electricity.gco2kwh, -- sahko
                    spaces.gco2kwh[4], -- puu
                    spaces.gco2kwh[5], -- turve
                    spaces.gco2kwh[6], -- hiili
                    electricity.gco2kwh, -- maalampo
                    spaces.gco2kwh[7] -- muu_lammitys
                ]
                FROM district_heating, spaces, electricity
            ',
            calculationYear, calculationScenario, method, municipality
        ) INTO gco2kwh_a;
    

        /* Lasketaan päästöt tilanteessa, jossa käytetään paikallista rakennusaineistoa */
        /* Calculating final emission when using local building data */
        IF heatSource IS NOT NULL THEN
            SELECT (floorSpace * vesi_kwhm2) / (hyotysuhde::real - 0.05) INTO vesi_kwh;

                SELECT CASE
                    WHEN heatSource = 'kaukolampo' THEN gco2kwh_a[1]
                    WHEN heatSource = 'kevyt_oljy' THEN gco2kwh_a[2]
                    WHEN heatSource = 'raskas_oljy' THEN gco2kwh_a[3]
                    WHEN heatSource = 'kaasu' THEN gco2kwh_a[4]
                    WHEN heatSource = 'sahko' THEN gco2kwh_a[5]
                    WHEN heatSource = 'puu' THEN gco2kwh_a[6]
                    WHEN heatSource = 'turve' THEN gco2kwh_a[7]
                    WHEN heatSource = 'hiili' THEN gco2kwh_a[8]
                    WHEN heatSource = 'maalampo' THEN gco2kwh_a[9]
                    WHEN heatSource = 'muu_lammitys' THEN gco2kwh_a[10]
                    END INTO gco2kwh;

            RETURN vesi_kwh * gco2kwh; -- vesi_co2
        ELSE
            /* Lasketaan päästöt tilanteessa, jossa käytetään YKR-rakennusaineistoa */
            /* Calculating final emissions when using YKR-based building data */
            SELECT array(SELECT(floorSpace * vesi_kwhm2) / (unnest(hyotysuhde_a) - 0.05)) INTO vesi_kwh_a;
            SELECT array(SELECT unnest(gco2kwh_a) * unnest(vesi_kwh_a) * unnest(lammitysosuus)) INTO vesi_co2_a;

            /* Palauta CO2-ekvivalentteja */
            /* Return CO2-equivalents */
            RETURN SUM(a) FROM unnest(vesi_co2_a) a;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;