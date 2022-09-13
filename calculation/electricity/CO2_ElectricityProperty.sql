/* Kiinteistösähkön kulutus | Consupmtion of property tech electricity

YKR-ruudun rakennusten liittyvä kiinteistösähkön käyttö sahko_kiinteisto_kwh [kWh/a] lasketaan kaavalla:
    sahko_kiinteisto_kwh = rakennus_ala * sahko_kiinteisto_kwhm2 * sahko_kiinteisto_muutos

Kiinteistösähkön kasvihuonekaasupäästöt sahko_kiinteisto_co2 [CO2-ekv/a] ovat:
    sahko_kiinteisto_co2 = sahko_kiinteisto_kwh * sahko_gco2kwh

*/

DROP FUNCTION IF EXISTS CO2_ElectricityProperty;
CREATE OR REPLACE FUNCTION
public.CO2_ElectricityProperty(
    calculationYears integer[], -- [year based on which emission values are calculated, min, max calculation years]
    calculationScenario varchar, -- PITKO-kehitysskenaario | PITKO development scenario
    floorSpace real, -- Rakennustyypin ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta sekä rakennuksen tyypistä ja ikäluokasta [m2]
    buildingType varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    buildingYear integer -- Rakennusvuosikymmen tai -vuosi (2017 alkaen) | Building decade or year (2017 onwards)
)
RETURNS real AS
$$
DECLARE
    calculationYear integer;
    result_gco2 real;
    sahko_kiinteisto_muutos real; -- Rakennustyypin ikäluokkakohtainen keskimääräisen kiinteistösähkön kulutuksen muutos tarkasteluvuonna [Ei yksikköä]. Lukuarvo riippuu laskentavuodesta ja rakennuksen ikäluokasta
BEGIN
    
    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF floorSpace <= 0 OR floorSpace IS NULL THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE

        calculationYear := CASE WHEN calculationYears[1] < calculationYears[2] THEN calculationYears[2]
        WHEN calculationYears[1] > calculationYears[3] THEN calculationYears[3]
        ELSE calculationYears[1]
        END;

        EXECUTE FORMAT(
        'WITH electricity_property_khwm2 AS (
            -- Kiinteistöjen sähkönkulutus kerrosalaa kohti
            -- Electricity consumption of integrated property technology
            SELECT sahko_kiinteisto_kwhm2 AS kwh
            FROM built.electricity_property_kwhm2
            WHERE scenario = %2$L
                AND rakennus_tyyppi = %3$L
                AND rakv = %4$L
                LIMIT 1
        ), electricity_property_change AS (
            -- Kiinteistöjen sähkönkulutuksen muutos rakennusten rakennusvuosikymmenittäin
            -- Change of property electricity consumption according to year of building
        
            SELECT CASE WHEN %4$L::int > 2010 THEN 1 ELSE %4$I END as change
                FROM built.electricity_property_change
                WHERE scenario = %2$L AND year = %1$L
                LIMIT 1
        ),
        electricity_gco2kwh AS (
            -- Kulutetun sähkön ominaispäästökerroin [gCO2-ekv/kWh].
            SELECT el.gco2kwh::int AS gco2
            FROM energy.electricity el
                WHERE el.year = %1$L
                AND el.scenario = %2$L
                AND el.metodi = ''em''
                AND el.paastolaji = ''tuotanto''
        )
        -- Lasketaan päästöt CO2-ekvivalentteina | Calculate emissions as gCO2-equivalents 
        SELECT %5$L * kwhm2.kwh * gco2kwh.gco2 * change.change
        FROM electricity_property_khwm2 kwhm2, electricity_gco2kwh gco2kwh, electricity_property_change change
        '
    , calculationYear,
    calculationScenario,
    buildingType,
    buildingYear,
    floorSpace
    ) INTO result_gco2;
    
    RETURN result_gco2;

    END IF;
END;
$$ LANGUAGE plpgsql;