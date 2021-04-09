/* Rakennusten jäähdytys | Cooling of buildings 

YKR-ruudun rakennusten jäähdytystarve jaahdytys_kwh [kWh/a] on:
    jaahdytys_kwh = rakennus_ala * jaahdytys_osuus * jaahdytys_kwhm2 * jaahdytys_muoto * jaahdytys_muutos

Rakennusten jäähdytykseen tarvitun ostoenergian kasvihuonekaasupäästöt [CO2/a] ovat:
    jaahdytys_co2 = jaahdytys_kwh * (jmuoto_apu1 * sahko_gco2kwh + jmuoto_apu2 * jaahdytys_gco2kwh)

*/

DROP FUNCTION IF EXISTS il_prop_cool_co2;
CREATE OR REPLACE FUNCTION
public.il_prop_cool_co2(
    rakennus_ala integer, -- Rakennustyypin tietyn ikäluokan kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta, rakennuksen tyypistä ja ikäluokasta [m2]
    calculationYear integer, -- Laskentavuosi | Calculation / reference year
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen) | Building decade or year (2017 onwards)
    calculationScenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    jaahdytys_gco2kwh real[] /* Jäähdytyksen aggregoidut ominaispäästökertoimet */ /* Aggegated cooling emission values */
)
RETURNS real AS
$$
DECLARE
    jaahdytys_muutos real; -- Kuvaa jäähdytyksen määrän kasvua rakennustyypeittäin. Arvo riippuu taustaskenaariosta, laskentavuodesta ja rakennuksen tyypistä [ei yksikköä]
    jaahdytys_kwhm2 real[]; -- Rakennustyypin ikäluokan jäähdytysenergian tarve yhtä kerrosneliötä kohti. Arvo riippuu taustaskenaariosta, rakennuksen tyypistä ja ikäluokasta [kWh/m2/a]
    jaahdytys_kwh real[]; -- Jäähdytyksen energiankulutus
    jaahdytys_co2 real[]; -- Jäähdytyksen kasvihuonekaasupäästöt [gCO2]
BEGIN

    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE
        
        /* Jäähdytystarpeet muutos */
        /* Cooling demand change */
        EXECUTE 'SELECT ' || rakennustyyppi || ' FROM built.cooling_change WHERE scenario = $1 AND year = $2' 
            INTO jaahdytys_muutos USING calculationScenario, calculationYear;
        
        /* Rakennusten jäähdytettävät osuudet & jäähdytyksen energiankulutus jäähdytysmuodoittain kerrosalaa kohden, rakennustyypeittäin ja -vuosittain */
        /* Proportion of different types of building cooled & energy consumption of cooling buildings per floor area, by building type and year and cooling method */
        EXECUTE 'SELECT array(SELECT unnest(array[kaukok,sahko,pumput,muu]) * (jaahdytys_osuus * jaahdytys_kwhm2) FROM built.cooling_proportions_kwhm2 WHERE scenario = $1 AND rakennus_tyyppi = $2 AND rakv = $3)' 
            INTO jaahdytys_kwhm2 USING calculationScenario, rakennustyyppi, rakennusvuosi;
        
        /* Laskenta */
        /* Calculation */
        SELECT array(SELECT(rakennus_ala * jaahdytys_muutos * unnest(jaahdytys_kwhm2))) INTO jaahdytys_kwh;
        SELECT array(SELECT unnest(jaahdytys_kwh) * unnest(jaahdytys_gco2kwh)) INTO jaahdytys_co2;

        /* Palauta CO2-ekvivalenttia */
        /* Return CO2-equivalents */
        RETURN SUM(a) FROM unnest(jaahdytys_co2) a;
    END IF;
END;
$$ LANGUAGE plpgsql;