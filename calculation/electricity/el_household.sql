/* Sähkön käyttö, kotitaloudet | Electricity consumption, households

Kotitalouksien sähkön käyttö muuhun kuin asuinrakennusten lämmitykseen, jäähdytykseen ja kiinteistön laitteisiin sahko_koti_kwh [kWh/a] lasketaan kaavalla

    sahko_koti_kwh = rakennus_ala * (sahko_koti_laite + sahko_koti_valo) + v_yht * sahko_koti_as

Kotitalouksien muuhun kuin lämmitykseen ja kiinteistösähköön käytetyn sähkön kasvihuonekaasupäästöt sahko_koti_co2 [CO2-ekv/a] ovat 

    sahko_koti_co2 = sahko_koti_kwh  * sahko_gco2kwh
*/

DROP FUNCTION IF EXISTS il_el_household_co2;
CREATE OR REPLACE FUNCTION
public.il_el_household_co2(
    ala_or_vaesto real, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna [m2] tai väestö laskentavuonna.
    calculationYear integer, -- Laskentavuosi | Calculation / reference year
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    calculationScenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    sahko_gco2kwh real, -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh]. Riippuu laskentavuodesta, taustaskenaariosta, päästölajista ‘tuotanto’/’hankinta’ sekä laskentatavasta ‘em’/’hjm’.
    sahko_as real default null
)
RETURNS real AS
$$
DECLARE
    sahko_koti_laite real; -- asuinrakennustyyppikohtainen laitesähkön keskimääräinen peruskulutus yhtä kerrosneliötä kohti [kWh/m2/a]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja rakennuksen tyypistä.
    sahko_koti_valo real; -- asuinrakennustyyppikohtainen sisävalaistuksen sähkön käyttö kerrosneliötä kohti vuodessa huomioiden tekniikan ja muuhun valaistukseen liittyvän sähkön käytön kehityksen [kWh/m2/a]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja rakennuksen tyypistä.
    sahko_koti_kwh real; -- Sähkönkulutus ruudussa [kWh/a]
    sahko_koti_as_kwh real; -- kuvaa asukkaiden lisävaikutusta asunnon sähkön kulutukseen. [kWh/as/a]. Arvo riippuu taustaskenaariosta ja laskentavuodesta.
BEGIN
    IF ala_or_vaesto <= 0 OR ala_or_vaesto IS NULL THEN
        RETURN 0;
    ELSE
        IF rakennustyyppi IS NOT NULL THEN 
            EXECUTE 'SELECT ' || rakennustyyppi || ' FROM built.electricity_home_device WHERE scenario = $1 AND year = $2'
                INTO sahko_koti_laite USING calculationScenario, calculationYear;
            EXECUTE 'SELECT ' || rakennustyyppi || ' FROM built.electricity_home_light WHERE scenario = $1 AND year = $2'
                INTO sahko_koti_valo USING calculationScenario, calculationYear;
            SELECT ala_or_vaesto * (sahko_koti_laite + sahko_koti_valo) INTO sahko_koti_kwh;
            RETURN sahko_koti_kwh * sahko_gco2kwh;
        ELSE 
            SELECT ala_or_vaesto * sahko_as INTO sahko_koti_as_kwh;
            RETURN sahko_koti_as_kwh * sahko_gco2kwh;
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;