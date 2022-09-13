/* Rakennusten purkaminen (energia) | Demolition of buildings (energy)

Rakennuksen elinkaaren katsotaan päättyvän, kun rakennus on purettu ja tontilta on
kuljetettu pois kaikki rakennusmateriaalit ja tontti on valmis seuraavaa käyttöä varten.
Päästölaskennassa huomioidaan rakennuksen purkutyön, puretun materiaalin jatkokäsittelykuljetusten
ja sen loppukäsittelyn ja -sijoituksen energiaperäiset kasvihuonekaasupäästöt rak_purku_energia_co2 [CO2-ekv/a] seuraavasti

    rak_purku_energia_co2 = rakennukset_poistuma * rak_purku_energia_gco2m2

*/
DROP FUNCTION IF EXISTS CO2_BuildDemolish;
CREATE OR REPLACE FUNCTION
public.CO2_BuildDemolish(
	buildingsRemoval real, -- rakennustyypin (erpien, rivita, askert, liike, tsto, liiken, hoito, kokoon, opetus, teoll, varast, muut) kerrosalan poistuma YKR-ruudussa laskentavuonna [m2].
    calculationYears integer[], -- [year based on which emission values are calculated, min, max calculation years]
    buildingType varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    calculationScenario varchar -- PITKO-kehitysskenaario | PITKO development scenario
)
RETURNS real AS
$$
DECLARE
    calculationYear integer;
    rak_purku_energia_gco2m2 real; -- [gCO2-ekv/m2] on rakennustyypin purkamisen, puretun materiaalin kuljetusten ja niiden käsittelyn kasvihuonekaasujen ominaispäästöt yhtä purettua kerroskerrosneliötä kohti. Lukuarvo riippuu taustaskenaariosta, tarkasteluvuodesta ja rakennustyypistä.
BEGIN

/* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
/* Returning zero, if grid cell has 0, -1 or NULL built floor area */
IF buildingsRemoval <= 0 OR buildingsRemoval IS NULL THEN
    RETURN 0;
/* Muussa tapauksessa jatka laskentaan */
/* In other cases continue with the calculation */
ELSE
    calculationYear := CASE WHEN calculationYears[1] < calculationYears[2] THEN calculationYears[2]
        WHEN calculationYears[1] > calculationYears[3] THEN calculationYears[3]
        ELSE calculationYears[1]
    END;
    /* Haetaan laskentavuoden ja kehitysskenaarion perusteella rakennustyyppikohtaiset uudisrakentamisen energiankulutuksen kasvihuonekaasupäästöt */
    /* Get the unit emissions for energy consumption of construction by year of building, scenario and building type */
    EXECUTE 'SELECT ' || buildingType || ' FROM built.build_demolish_energy_gco2m2 WHERE scenario = $1 AND year = $2'
        INTO rak_purku_energia_gco2m2  USING calculationScenario, calculationYear;
    
    /* Lasketaan ja palautetaan päästöt CO2-ekvivalentteina [gCO2-ekv/v] */
    /* Calculate and return emissions as CO2-equivalents [gCO2-ekv/a] */
    RETURN buildingsRemoval * rak_purku_energia_gco2m2;

END IF;

END;
$$ LANGUAGE plpgsql;