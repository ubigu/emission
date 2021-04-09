/* Käyttöveden lämmitys | Heating of water

YKR-ruudun rakennusten vuoden aikana kuluttaman lämpimän käyttöveden lämmitystarve vesi_lammitystarve [kWh/a] on:
    vesi_lammitystarve = rakennus_ala * vesi_kwhm2

Lämpimän käyttöveden lämmittämiseen tarvittavan ostoenergian tarve vesi_kwh [kWh/a] on:
    vesi_kwh =  vesi_lammitystarve / (tilat_hyotysuhde - 0.05)

Rakennustyyppien käyttöveden lämmityksen tarvitseman ostoenergian kasvihuonekaasupäästöt vesi_co2 [CO2-ekv/a] ovat YKR-ruudussa:
    vesi_co2 = vesi_kwh * (lmuoto_apu1 * klampo_gco2kwh + lmuoto_apu2 * sahko_gco2kwh + lmuoto_apu3 * tilat_gco2kwh)
    
*/

DROP FUNCTION IF EXISTS il_prop_water_co2;
CREATE OR REPLACE FUNCTION
public.il_prop_water_co2(
	rakennus_ala real, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta sekä rakennuksen tyypistä ja ikäluokasta [m2]
    calculationYear integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi, esim. 'erpien', 'rivita'
    rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen)
    gco2kwh_a real[], -- Lopulliset omainaispäästökertoimet
    calculationScenario varchar, -- PITKO:n mukainen kehitysskenaario
    lammitysmuoto varchar default null -- Rakennuksen lämmityksessä käytettävä primäärinen energiamuoto 'energiam', mikäli tällainen on lisätty YKR/rakennusdataan
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
    gco2kwh real; -- Käytettävä ominaispäästökerroin
BEGIN
    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE

        /* Käytetään kun on johdettu paikallisesta aineistosta lämmitys/energiamuototiedot ruututasolle */
        /* Used when local building register data has been used to derive grid level information incl. heating methods of building */
        IF lammitysmuoto IS NOT NULL THEN
            EXECUTE 'SELECT ' || lammitysmuoto || ' FROM built.spaces_efficiency WHERE rakennus_tyyppi = $1 AND rakv = $2' INTO hyotysuhde USING rakennustyyppi, rakennusvuosi;
        ELSE 
        /* Käytetään kun käytössä on pelkkää YKR-dataa */
        /* Used when basing the analysis on pure YKR data */
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO lammitysosuus FROM built.distribution_heating_systems WHERE scenario = calculationScenario AND rakennus_tyyppi = rakennustyyppi AND rakv = rakennusvuosi AND year = calculationYear;
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO hyotysuhde_a FROM built.spaces_efficiency WHERE rakennus_tyyppi = rakennustyyppi AND rakv = rakennusvuosi;
        END IF;

        /* Veden lämmityksen energiankulutus per kerrosneliö */
        /* Water heating power consumption per floor space, square meter */
        SELECT vesi_kwh_m2 INTO vesi_kwhm2 FROM built.water_kwhm2 AS vesi_kwhm2 WHERE
            vesi_kwhm2.scenario = calculationScenario AND
            vesi_kwhm2.rakennus_tyyppi = rakennustyyppi AND 
            vesi_kwhm2.rakv = rakennusvuosi;
    
        /* Lasketaan päästöt tilanteessa, jossa käytetään paikallista rakennusaineistoa */
        /* Calculating final emission when using local building data */
        IF lammitysmuoto IS NOT NULL THEN
            SELECT (rakennus_ala * vesi_kwhm2) / (hyotysuhde - 0.05) INTO vesi_kwh;

                SELECT CASE
                    WHEN lammitysmuoto = 'kaukolampo' THEN gco2kwh_a[1]
                    WHEN lammitysmuoto = 'kevyt_oljy' THEN gco2kwh_a[2]
                    WHEN lammitysmuoto = 'raskas_oljy' THEN gco2kwh_a[3]
                    WHEN lammitysmuoto = 'kaasu' THEN gco2kwh_a[4]
                    WHEN lammitysmuoto = 'sahko' THEN gco2kwh_a[5]
                    WHEN lammitysmuoto = 'puu' THEN gco2kwh_a[6]
                    WHEN lammitysmuoto = 'turve' THEN gco2kwh_a[7]
                    WHEN lammitysmuoto = 'hiili' THEN gco2kwh_a[8]
                    WHEN lammitysmuoto = 'maalampo' THEN gco2kwh_a[9]
                    WHEN lammitysmuoto = 'muu_lammitys' THEN gco2kwh_a[10]
                    END INTO gco2kwh;

            RETURN vesi_kwh * gco2kwh; -- vesi_co2
        ELSE
            /* Lasketaan päästöt tilanteessa, jossa käytetään YKR-rakennusaineistoa */
            /* Calculating final emissions when using YKR-based building data */
            SELECT array(SELECT(rakennus_ala * vesi_kwhm2) / (unnest(hyotysuhde_a)-0.05)) INTO vesi_kwh_a;
            SELECT array(SELECT unnest(gco2kwh_a) * unnest(vesi_kwh_a) * unnest(lammitysosuus)) INTO vesi_co2_a;

            /* Palauta CO2-ekvivalentteja */
            /* Return CO2-equivalents */
            RETURN SUM(a) FROM unnest(vesi_co2_a) a;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;