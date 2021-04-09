/* Uudisrakentaminen (energia) | Construction of new buildings (energy)

YKR-ruudun laskentavuoden uudisrakentamisen energian kasvihuonekaasupäästöt [CO2-ekv/a] lasketaan seuraavasti:

    rak_uusi_energia_co2 = rakennus_ala * rak_energia_gco2m2


YKR-ruudun laskentavuoden uudisrakentamisen rakennustuotteiden kasvihuonekaasupäästöt rak_uusi_materia_co2 [CO2-ekv/a] lasketaan seuraavasti:
 
    rak_uusi_materia_co2 = rakennus_ala * rak_materia_gco2m2


*/
DROP FUNCTION IF EXISTS public.il_build_new_co2;
CREATE OR REPLACE FUNCTION
public.il_build_new_co2(
	rakennus_ala real, -- Rakennustyypin tietyn ikäluokan kerrosala YKR-ruudussa laskentavuonna [m2]. Lukuarvo riippuu laskentavuodesta ja rakennuksen tyypistä.
  --  rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen). Tässä: Rakennuksen valmistumisvuosi = laskentavuosi.
    calculationYear integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi, esim. 'erpien', 'rivita'
    calculationScenario varchar -- PITKO:n mukainen kehitysskenaario
)
RETURNS real AS
$$
DECLARE
    rak_energia_gco2m2 real; -- Rakennustyypin rakentamisvaiheen työmaatoimintojen ja kuljetusten kasvihuonekaasujen ominaispäästöjä yhtä rakennettua kerrosneliötä kohti [gCO2-ekv/m2]. Arvo riippuu taustaskenaariosta, tarkasteluvuodesta ja rakennustyypistä.
    rak_materia_gco2m2 real; -- Rakennustyypin rakentamiseen tarvittujen rakennustuotteiden tuotantoprosessin välillisiä kasvihuonekaasujen ominaispäästöjä yhtä rakennettua kerrosneliötä kohti [gCO2-ekv/m2]. Arvo riippuu taustaskenaariosta, tarkasteluvuodesta ja rakennustyypistä.
BEGIN

/* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
/* Returning zero, if grid cell has 0, -1 or NULL built floor area */
IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
    RETURN 0;
/* Muussa tapauksessa jatka laskentaan */
/* In other cases continue with the calculation */
ELSE

    /* Jos rakennusvuosi ei ole tarkasteltavana oleva vuosi, palauta 0 */
    /* If the year of construction is not the current year of calculation, return 0 */
--    IF rakennusvuosi != year THEN
--        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases, continue */
--    ELSE
        /* Haetaan laskentavuoden ja kehitysskenaarion perusteella rakennustyyppikohtaiset uudisrakentamisen energiankulutuksen kasvihuonekaasupäästöt */
        /* Get the unit emissions for energy consumption of construction by year of building, scenario and building type */
        EXECUTE 'SELECT ' || rakennustyyppi || ' FROM built.constr_new_build_energy_gco2m2 WHERE year = $1 AND scenario = $2'
            INTO rak_energia_gco2m2 USING calculationYear, calculationScenario;
        
        /* Haetaan laskentavuoden ja kehitysskenaarion perusteella rakennustyyppikohtaiset uudisrakentamisen materiaalien valmistuksen kasvihuonekaasupäästöt */
        /* Get the unit emissions for production of materials for construction by year of building, scenario and building type */
        EXECUTE 'SELECT ' || CASE WHEN rakennustyyppi IN ('erpien', 'rivita', 'askert') THEN rakennustyyppi ELSE 'muut' END || ' FROM rakymp.rak_materia_gco2m2 WHERE vuosi = $1 AND skenaario = $2'
            INTO rak_materia_gco2m2 USING calculationYear, calculationScenario;

        /* Lasketaan ja palautetaan päästöt CO2-ekvivalenttia [gCO2-ekv/v] */
        /* Calculate and return emissions as CO2-equivalents [gCO2-ekv/a] */
        RETURN rakennus_ala * (rak_energia_gco2m2 + rak_materia_gco2m2); -- rak_uusi_energia_co2

--    END IF;
END IF;

END;
$$ LANGUAGE plpgsql;