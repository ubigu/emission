/* Korjausrakentamisen ja saneeraamisen päästöt | Emissions from renovations and large scale overhauls of buildings

Rakennusten käytön aikana tehtävien tavanomaisten korjausten ja rakennustuotteiden vaihtojen energian käytön kasvihuonekaasupäästöt rak_korj_energia_co2 [CO2-ekv/a] ovat

    rak_korj_energia_co2= rakennus_ala* rak_korj_energia_gco2m2

YKR-ruudun rakennusten laajamittaisen korjausten energian käytön laskentavuonna aiheuttamat kasvihuonekaasupäästöt rak_saneer_energia_co2 [t CO2-ekv/a] lasketaan kaavalla

    rak_saneer_energia_co2 = rakennus_ala * rak_saneer_osuus * rak_saneer_energia_gco2m2 * muunto_massa

Kummassakaan tarkastelussa ei oteta vielä tässä vaiheessa huomioon korjaamisessa tarvittavien materiaalien valmistuksen aiheuttamia välillisiä kasvihuonekaasupäästöjä.

*/

DROP FUNCTION IF EXISTS CO2_BuildRenovate;
CREATE OR REPLACE FUNCTION
public.CO2_BuildRenovate(
    floorSpace real, -- Rakennustyypin (erpien, rivita, askert, liike, tsto, liiken, hoito, kokoon, opetus, teoll, varast, muut) ikäluokkakohtainen kerrosala YKR-ruudussa laskentavuonna [m2]. Lukuarvo riippuu laskentavuodesta sekä rakennuksen tyypistä ja ikäluokasta.
    calculationYear integer, -- Laskentavuosi | Calculation / reference year
    buildingType varchar,  -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    buildingYear integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen) | Building decade or year (2017 onwards)
    calculationScenario varchar) -- PITKO-kehitysskenaario | PITKO development scenario
RETURNS real AS
$$
DECLARE
    rak_korj_energia_gco2m2 real; -- Tarkasteltavan rakennustyypin pienimuotoisten korjausten työmaatoimintojen ja kuljetusten kasvihuonekaasun ominaispäästöt yhtä kerrosneliötä kohti laskentavuonna [gCO2-ekv/m2]. Riippuu taustaskenaariosta, laskentavuodesta ja rakennustyypistä.
    rak_saneer_energia_gco2m2  real; -- Tarkasteltavan rakennustyypin laajamittaisen korjausrakentamisen työmaatoimintojen ja kuljetusten kasvihuonekaasun ominaispäästöt yhtä kerroskerrosneliötä kohti laskentavuonna [gCO2-ekv/m2]. Riippuu taustaskenaariosta, laskentavuodesta ja rakennustyypistä.
    rak_saneer_osuus real; -- Rakennustyypin ikäluokkakohtainen kerrosalaosuus, johon tehdään laskentavuoden aikana laajamittaisia korjausrakentamista [ei yksikköä]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta sekä rakennuksen ikäluokasta ja tyypistä.
BEGIN
    IF floorSpace <= 0 OR floorSpace IS NULL THEN
        RETURN 0;
    ELSE
        /* Korjausrakentamisen ominaispäästöt */
        EXECUTE 'SELECT ' || buildingType || ' FROM built.build_renovation_energy_gco2m2 WHERE scenario = $1 AND year = $2'
            INTO rak_korj_energia_gco2m2  USING calculationScenario, calculationYear;
        /* Saneerauksen ominaispäästöt ja vuosittainen kattavuus */
        EXECUTE 'SELECT ' || buildingType || ' FROM built.build_rebuilding_energy_gco2m2 WHERE scenario = $1 AND year = $2'
            INTO rak_saneer_energia_gco2m2  USING calculationScenario, calculationYear;
        EXECUTE 'SELECT ' || buildingType || ' FROM built.build_rebuilding_share WHERE scenario = $1 AND rakv = $2 AND year = $3'
            INTO rak_saneer_osuus USING calculationScenario, buildingYear, calculationYear;

        RETURN floorSpace * (rak_korj_energia_gco2m2 + rak_saneer_energia_gco2m2 * rak_saneer_osuus); -- rak_korjaussaneeraus_energia_co2
        
    END IF;
END;
$$ LANGUAGE plpgsql;