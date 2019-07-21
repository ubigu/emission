/*
UUDISRAKENTAMINEN (energia)
CONSTRUCTION OF NEW BUILDINGS (energy)

YKR-ruudun laskentavuoden uudisrakentamisen kasvihuonekaasupäästöt [t CO2-ekv/a] lasketaan seuraavasti:
rak_uusi_energia_co2 = rakennus_ala * rak_energia_gco2m2 * muunto_massa

*/

CREATE OR REPLACE FUNCTION
public.il_build_new_co2(
	rakennus_ala real, -- [m2] Rakennustyypin tietyn ikäluokan kerrosala YKR-ruudussa laskentavuonna. Lukuarvo riippuu laskentavuodesta ja rakennuksen tyypistä.
    rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen). Tässä: Rakennuksen valmistumisvuosi = laskentavuosi.
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi, esim. 'erpien', 'rivita'
    scenario varchar -- PITKO:n mukainen kehitysskenaario
)
RETURNS real AS
$$
DECLARE
    rak_energia_gco2m2 real; -- [gCO2-ekv/m2] Rakennustyypin rakentamisvaiheen työmaatoimintojen ja kuljetusten kasvihuonekaasujen ominaispäästöjä yhtä rakennettua kerrosneliötä kohti. Lukuarvo riippuu taustaskenaariosta, tarkasteluvuodesta ja rakennustyypistä.
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
    IF rakennusvuosi != year THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases, continue */
    ELSE
        /* Haetaan laskentavuoden ja kehitysskenaarion perusteella rakennustyyppikohtaiset uudisrakentamisen energiankulutuksen kasvihuonekaasupäästöt */
        /* Get the unit emissions for energy consumption of construction by year of building, scenario and building type */
        EXECUTE 'SELECT ' || rakennustyyppi || ' FROM rakymp.rak_energia_gco2m2 WHERE vuosi = $1 AND skenaario = $2'
            INTO rak_energia_gco2m2  USING year, scenario;
        
        /* Lasketaan ja palautetaan päästöt CO2-ekvivalenttia [gCO2-ekv/v] */
        /* Calculate and return emissions as CO2-equivalents [gCO2-ekv/a] */
        RETURN rakennus_ala * rak_energia_gco2m2; -- rak_uusi_energia_co2

    END IF;
END IF;

END;
$$ LANGUAGE plpgsql;