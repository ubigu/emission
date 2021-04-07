/* Sähkön käyttö, kotitaloudet | Electricity consumption, households

Kotitalouksien sähkön käyttö muuhun kuin asuinrakennusten lämmitykseen, jäähdytykseen ja kiinteistön laitteisiin sahko_koti_kwh [kWh/a] lasketaan kaavalla

    sahko_koti_kwh = rakennus_ala * (sahko_koti_laite + sahko_koti_valo) + v_yht * sahko_koti_as

Kotitalouksien muuhun kuin lämmitykseen ja kiinteistösähköön käytetyn sähkön kasvihuonekaasupäästöt sahko_koti_co2 [CO2-ekv/a] ovat 

    sahko_koti_co2 = sahko_koti_kwh  * sahko_gco2kwh
*/

DROP FUNCTION IF EXISTS CO2_ElectricityHousehold;
CREATE OR REPLACE FUNCTION
public.CO2_ElectricityHousehold(
    calculationYear integer, -- Laskentavuosi | Calculation / reference year
    calculationScenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    area_or_pop real, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna [m2] tai väestö laskentavuonna.
    buildingType varchar -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
)
RETURNS real AS
$$
DECLARE
    result_gco2 real;
BEGIN
    IF area_or_pop <= 0 OR area_or_pop IS NULL THEN
        RETURN 0;
    ELSE

    IF buildingType IS NULL THEN
            EXECUTE FORMAT(
                'WITH electricity_home_percapita AS (
                    -- Asukkaiden lisävaikutus asunnon sähkön kulutukseen. [kWh/as/a].
                    SELECT sahko_koti_as AS kwh
                    FROM energia.sahko_koti_as sas
                    WHERE sas.vuosi = %1$L
                        AND sas.skenaario = %2$L
                ), electricity_gco2kwh AS (
                     -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh].
                    SELECT el.gco2kwh::int AS gco2
                    FROM energia.sahko el
                        WHERE el.vuosi = %1$L
                        AND el.skenaario = %2$L
                        AND el.metodi = ''em''
                        AND el.paastolaji = ''tuotanto''
                )
                SELECT %3$L * percapita.kwh * gco2kwh.gco2
                FROM electricity_home_percapita percapita, electricity_gco2kwh gco2kwh
                '
            , calculationYear,
            calculationScenario,
            area_or_pop
            ) INTO result_gco2;
    
    ELSE 
            EXECUTE FORMAT(
                'WITH electricity_home_devices AS (
                     -- Rakennustyyppikohtainen laitesähkön keskimääräinen peruskulutus [kWh/m2/a]
                    SELECT %3$I::int AS kwh
                    FROM rakymp.sahko_koti_laite WHERE skenaario = %2$L AND vuosi = %1$L
                ), electricity_home_lighting AS (
                     -- Rakennustyyppikohtainen sisävalaistuksen sähkön käyttö kerrosneliötä kohti huomioiden tekniikan ja muuhun valaistukseen liittyvän sähkön käytön kehityksen [kWh/m2/a]
                    SELECT %3$I::int AS kwh
                    FROM rakymp.sahko_koti_valo WHERE skenaario = %2$L AND vuosi = %1$L
                ), electricity_gco2kwh AS (
                    SELECT el.gco2kwh::int AS gco2 -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh].
                    FROM energia.sahko el
                        WHERE el.vuosi = %1$L
                        AND el.skenaario = %2$L
                        AND el.metodi = ''em''
                        AND el.paastolaji = ''tuotanto''
                )
                SELECT  %4$L * (devices.kwh + lights.kwh) * gco2kwh.gco2
                FROM electricity_home_devices devices,   
                    electricity_home_lighting lights,
                    electricity_gco2kwh gco2kwh
                '
            , calculationYear,
            calculationScenario,
            buildingType,
            area_or_pop
            ) INTO result_gco2;
    
    END IF;

    END IF;
    RETURN result_gco2;

END;
$$ LANGUAGE plpgsql;