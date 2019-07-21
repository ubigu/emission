/* Sähkön käyttö, teollisuus, varastot ja palvelut | Electricity consumption, industry, warehouses and services

Palvelusektorin sekä teollisuuden ja varastojen sähkön käyttö muuhun kuin rakennusten lämmitykseen, jäähdytykseen ja kiinteistön laitteisiin sahko_palv_kwh [kWh/a] perustuu kaavaan

    sahko_ptv_kwh  = rakennus_ala * sahko_ptv_kwhm2

Palvelusektorin sekä teollisuuden ja varastojen muuhun lämmitykseen ja kiinteistöshköön käytetyn sähkön kasvihuonekaasupäästöt sahko_palv_co2 [CO2-ekv/a] ovat 

    sahko_ptv_co2 = sahko_ptv_kwh  * sahko_gco2kwh

Teollisuus- ja varastorakennusten sähkön käyttö sisältää myös niiden kiinteistösähkön kulutuksen.

*/

DROP FUNCTION IF EXISTS il_el_iwhs_co2;
CREATE OR REPLACE FUNCTION
public.il_el_iwhs_co2(
    rakennus_ala real, -- rakennustyypin kerrosala YKR-ruudussa laskentavuonna [m2]. Riippuu laskentavuodesta, rakennuksen tyypistä ja ikäluokasta.
    year integer, -- Laskentavuosi | Calculation / reference year
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    scenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    sahko_gco2kwh real, -- kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh]. Riippuu laskentavuodesta, taustaskenaariosta, päästölajista ‘tuotanto’/’hankinta’ sekä laskentatavasta ‘em’/’hjm’.
)
RETURNS real AS
$$
DECLARE
    sahko_ptv_kwhm2 real; -- rakennustyypissä tapahtuvan toiminnan sähköintensiteetti yhtä kerrosneliömetriä kohti [kWh/m2]. Riippuu tarkasteluskenaariosta, laskentavuodesta ja rakennuksen tyypistä (liike, tsto, liiken, hoito, kokoon, opetus, teoll, varast).
    sahko_ptv_kwh real; -- Palvelu- teollisuus- ja varastorakennusten sähkönkulutus [kWh/a]
BEGIN
    IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
        RETURN 0;
    ELSE
        EXECUTE 'SELECT ' || rakennustyyppi || ' FROM rakymp.sahko_ptv_kwhm2 WHERE skenaario = $1 AND vuosi = $2'
            INTO sahko_ptv_kwhm2 USING scenario, year;

        SELECT rakennus_ala * sahko_ptv_kwhm2 INTO sahko_ptv_kwh;
        RETURN sahko_ptv_kwh * sahko_gco2kwh;
    END IF;
END;
$$ LANGUAGE plpgsql;