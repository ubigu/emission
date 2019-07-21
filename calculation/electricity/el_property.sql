/* Kiinteistösähkön kulutus | Consupmtion of property tech electricity

YKR-ruudun rakennusten liittyvä kiinteistösähkön käyttö sahko_kiinteisto_kwh [kWh/a] lasketaan kaavalla:
    sahko_kiinteisto_kwh = rakennus_ala * sahko_kiinteisto_kwhm2 * sahko_kiinteisto_muutos

Kiinteistösähkön kasvihuonekaasupäästöt sahko_kiinteisto_co2 [CO2-ekv/a] ovat:
    sahko_kiinteisto_co2 = sahko_kiinteisto_kwh * sahko_gco2kwh

*/

DROP FUNCTION IF EXISTS il_el_property_co2;
CREATE OR REPLACE FUNCTION
public.il_el_property_co2(
    rakennus_ala real, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta sekä rakennuksen tyypistä ja ikäluokasta [m2]
    year integer, -- Laskentavuosi | Calculation / reference year
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen) | Building decade or year (2017 onwards)
    scenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    sahko_gco2kwh real -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh]. Riippuu laskentavuodesta, taustaskenaariosta, päästölajista ‘tuotanto’/’hankinta’ sekä laskentatavasta ‘em’/’hjm’.
)
RETURNS real AS
$$
DECLARE
    sahko_kiinteisto_muutos real; -- Rakennustyypin ikäluokkakohtainen keskimääräisen kiinteistösähkön kulutuksen muutos tarkasteluvuonna [Ei yksikköä]. Lukuarvo riippuu laskentavuodesta ja rakennuksen ikäluokasta
    sahko_kwhm2 real; -- Rakennustyypin ikäluokkakohtainen kiinteistösähkön ominaiskulutus yhtä kerrosneliötä kohti [kWh/m2/a]. Lukuarvo riippuu taustaskenaariosta sekä rakennuksen ikäluokasta ja tyypistä.
BEGIN
    
    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE

        /* Kiinteistöjen sähkönkulutus kerrosalaa kohti */
        /* Electricity consumption of integrated property technology */
        SELECT sahko_kiinteisto_kwhm2 INTO sahko_kwhm2 FROM rakymp.sahko_kiinteisto_kwhm2 AS kwhm2 WHERE
            kwhm2.skenaario = scenario AND
            kwhm2.rakennus_tyyppi = rakennustyyppi AND
            kwhm2.rakv = rakennusvuosi;

        /* Kiinteistöjen sähkönkulutuksen muutos rakennusten rakennusvuosikymmenittäin */
        /* Change of property electricity consupmtion according to year of building */
        SELECT muutos FROM rakymp.sahko_kiinteisto_muutos WHERE skenaario = scenario AND rakv = rakennusvuosi AND vuosi = year INTO sahko_kiinteisto_muutos;
        
        /* Lasketaan ja palautetaan päästöt CO2-ekvivalentteina */
        /* Calculate and return emissions as CO2-equivalents */
        RETURN rakennus_ala * sahko_kwhm2 * sahko_gco2kwh * sahko_kiinteisto_muutos; -- sahko_kiinteisto_co2

    END IF;
END;
$$ LANGUAGE plpgsql;