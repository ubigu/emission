/* Rakennusten lämmitys | Heating of buildings

Rakennusten lämmityksen kasvihuonekaasupäästöjen laskennalla tarkoitetaan tässä rakennusten nettomääräisen lämmitysenergian tarpeen arviointia.
Tämä tarkoittaa rakennusten tilojen lämmittämiseen tarvittavaa energiaa, josta on vähennetty henkilöistä, valaistuksesta ja sähkölaitteista syntyvien lämpökuormien energia,
poistoilmasta ja muista energiavirroista talteen otettu tilojen lämmityksessä hyödynnetty energia ja ikkunoiden kautta tuleva auringon säteilyenergia.

Lämmitysmuotoihin – tai oikeammin lämmönlähteisiin – sisältyvät kauko- ja aluelämpö, kevyt polttoöljy, raskas polttoöljy, maakaasu, sähkölämmitys, puupolttoaineet, turve,
kivihiili ja koksi, maalämpö ja muut vastaavat lämpöpumput sekä muut lämmitysmuodot. Jälkimmäiseen ryhmään sisältyvät myös tilastojen tuntemattomat lämmitysmuodot.

YKR-ruudun buildingTypeen eri ikäluokkien lämmitysmuotojen tilojen nettolämmitystarpeet tilat_lammitystarve [kWh/a] lasketaan seuraavalla kaavalla.
 
    tilat_lammitystarve =  floorSpace * tilat_kwhm2  * lammitystarve_vuosi / lammitystarve_vertailu / lammitystarve_korjaus_vkunta

Jos tarkasteltavan alueen rakennusten tyyppi-, ikäluokka- ja lämmitysmuotojakauma perustuu kunnan omaan paikkatietopohjaiseen aineistoon,
floorSpace -tieto on lämmitysmuotokohtainen. Tämä tarkempi laskentatapa ottaa huomioon keskimääräiseen jakaumaan perustuvaa laskentaa paremmin huomioon lämmitysmuotojakauman ruutukohtaiset erot.
Muutoin voidaan käyttää esilaskettua buildingType- ja ikäluokkakohtaista keskimääristä lämmitysmuotojakaumaa, joka on tarkasteltavasta YKR-ruudusta riippumaton.
 
YKR-ruudun buildingTypeen tilojen lämmitykseen vuoden aikana tarvittu ostoenergia tilat_kwh [kWh/a] on
    
    tilat_kwh = tilat_lammitystarve / tilat_hyotysuhde

buildingTypeen tilojen lämmityksen tarvitseman ostoenergian kasvihuonekaasupäästöt tilat_co2 [CO2-ekv/a] saadaan kaavalla
 
    tilat_co2 = tilat_kwh * (lmuoto_apu1 * klampo_gco2kwh + lmuoto_apu2 * sahko_gco2kwh + lmuoto_apu3 * tilat_gco2kwh)

*/
DROP FUNCTION IF EXISTS CO2_PropertyHeat;
CREATE OR REPLACE FUNCTION
public.CO2_PropertyHeat(
    municipality int,
    calculationYears integer[], -- [year based on which emission values are calculated, min, max calculation years]
    calculationScenario varchar, -- PITKO:n mukainen kehitysskenaario
    floorSpace int, -- Rakennustyypin tietyn ikäluokan kerrosala YKR-ruudussa laskentavuonna. Arvo riippuu laskentavuodesta, rakennuksen tyypistä ja ikäluokasta ja paikallista aineistoa käytettäessä lämmitysmuodosta [m2]
    buildingType varchar, -- buildingType, esim. 'erpien', 'rivita'S
    buildingYear int, -- buildingYear decade tai -vuosi (2017 alkaen)
    method varchar,
    heatSource varchar default null -- Rakennuksen lämmityksessä käytettävä primäärinen energiamuoto 'energiam', mikäli tällainen on lisätty YKR/rakennusdataan
)
RETURNS real AS
$$
DECLARE -- Joillekin muuttujille on sekä yksittäiset että array-tyyppiset muuttujat, riippuen siitä, onko lähtödatana YKR-dataa (array) vai paikallisesti jalostettua rakennusdataa
    calculationYear integer;
    heating_kwh real; -- Raw heating of spaces without efficiency ratio
    hyotysuhde real; -- Rakennustyypin ikäluokan lämmitysjärjestelmäkohtainen keskimääräinen vuosihyötysuhde tai lämpökerroin. Lukuarvo riippuu rakennuksen ikäluokasta, tyypistä ja lämmitysmuodosta [ei yksikköä].
    hyotysuhde_a real[]; -- Rakennustyypin ikäluokan keskimääräiset vuosihyötysuhteet eri lämmitysjärjestelmille. Lukuarvo riippuu rakennuksen ikäluokasta ja tyypistä [ei yksikköä].
    lammitys_kwh_a real[]; -- Lämmityksen energiankulutus (array)
    lammitys_co2_a real[]; -- Lämmityksen kasvihuonekaasupäästöt [gCO2]
    lammitysosuus real[]; -- Lämmitysmuotojen keskimääräiset osuudet rakennustyypin kerrosalasta tietyssä ikäluokassa laskentavuonna. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, rakennuksen ikäluokasta ja tyypistä.
    gco2kwh_a real[];
    gco2kwh real; -- Lopulliset ominaispäästökertoimet
    result_gco2 real;
BEGIN

    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF floorSpace <= 0 OR floorSpace IS NULL THEN
        RETURN 0;
    /* In other cases continue with the calculation */
    ELSE

        calculationYear := CASE WHEN calculationYears[1] < calculationYears[2] THEN calculationYears[2]
        WHEN calculationYears[1] > calculationYears[3] THEN calculationYears[3]
        ELSE calculationYears[1]
        END;

        /* Used when local building register data has been used to derive grid level information incl. heating methods of building */
        IF heatSource IS NOT NULL THEN
            EXECUTE FORMAT('
                SELECT %1$I::real
                    FROM built.spaces_efficiency
                    WHERE rakennus_tyyppi = %2$L
                        AND rakv::int = %3$s::int
                        AND mun::int = %5$s::int LIMIT 1
                ', heatSource, buildingType, buildingYear, calculationScenario, municipality
            ) INTO hyotysuhde;
        ELSE
            /* Used when basing the analysis on pure YKR data */
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO lammitysosuus
                    FROM built.distribution_heating_systems
                    WHERE scenario = calculationScenario
                    AND rakennus_tyyppi = buildingType
                    AND rakv = buildingYear
                    AND year = calculationYear;
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO hyotysuhde_a FROM built.spaces_efficiency WHERE rakennus_tyyppi = buildingType AND rakv = buildingYear;
        END IF;

            /* Energy demand for different building types, per square meter floor space */

        /* Ilmaston lämpenemisen myötä tilojen lämmitystarve pienenee, välillä 2015-2050 noin -17% eli n. 0.5% per vuosi. 
            Lineaariseksi määritetty kehitys perustuu tutkimukseen:
            P. Pirinen, H. Simola, S. Nevala, P. Karlsson ja S. Ruuhela, ”Ilmastonmuutos ja lämmitystarveluku paikkatietoarvioina Suomessa,” Ilmastieteen laitos, Helsinki, 2014.
            ja tässä valittu  aiemman ilmastonmuutosmallinnuksen IPCC:n SRES-kasvihuonekaasuinventaarion A1B-skenaario;
            Tämä vastaa uusien kasvihuonekaasujen pitoisuuksien kehityskulkuja mallintavien RCP-päästöskenaarioiden optimistisimman RCP4.5- ja pessimistisen RCP8.5-skenaarion välimaastoa.
        */
        EXECUTE FORMAT('
            WITH spaces AS (
                -- Rakennustyypin ikäluokkakohtainen kerrosneliömetrin lämmittämiseen vuodessa tarvittu nettolämmitysenergia.
                -- Kerroin huomioi olevan rakennuskannan energiatehokkuuden kehityksen [kWh/m2/a].
                SELECT %6$I as kwhm2
                FROM built.spaces_kwhm2
                    WHERE scenario = %1$L AND rakv = %2$L AND mun::int = %3$L AND year = %4$L LIMIT 1),
                heating as (
                    SELECT ((1 - 0.005 * (%4$L::int - 2015)) * multiplier)::real as scaler
                        FROM energy.heating_degree_days dd
                            WHERE dd.mun::int = %3$L
                ) SELECT spaces.kwhm2 * heating.scaler * %5$L
                    FROM spaces, heating
                ', calculationScenario, buildingYear, municipality, calculationYear, floorSpace, buildingType
        ) INTO heating_kwh;

         /* Kaukolämmön ominaispäästökertoimet  */
        /* Emission values for district heating (first finding out the name of the correct district heating table) */
        EXECUTE FORMAT(
            'WITH district_heating AS (
                SELECT %3$I as gco2kwh -- Laskentavuonna kulutetun kaukolämmön ominaispäästökerroin [gCO2-ekv/kWh]
                FROM energy.district_heating heat
                WHERE heat.year = %1$L
                AND heat.scenario = %2$L
                AND heat.mun::int = %4$L
                ORDER BY %3$I DESC LIMIT 1
            ), electricity AS (
                SELECT el.gco2kwh::int AS gco2kwh
                FROM energy.electricity el
                    WHERE el.year = %1$L
                    AND el.scenario = %2$L
                    AND el.metodi = ''em''
                    AND el.paastolaji = ''tuotanto''
            ), spaces AS (
                --  Lämmönlähteiden kasvihuonekaasupäästöjen ominaispäästökertoimet [gCO2-ekv/kWh]
                SELECT array[kevyt_oljy, raskas_oljy, kaasu, puu, turve, hiili, muu_lammitys] as gco2kwh 
                FROM energy.spaces_gco2kwh t
                WHERE t.vuosi = %1$L
            ) SELECT
                array[
                    district_heating.gco2kwh, -- kaukolämpö
                    spaces.gco2kwh[1], -- kevyt_oljy
                    spaces.gco2kwh[2], -- raskas_oljy
                    spaces.gco2kwh[3], -- kaasu
                    electricity.gco2kwh, -- sahko
                    spaces.gco2kwh[4], -- puu
                    spaces.gco2kwh[5], -- turve
                    spaces.gco2kwh[6], -- hiili
                    electricity.gco2kwh, -- maalampo
                    spaces.gco2kwh[7] -- muu_lammitys
                ] FROM district_heating, spaces, electricity
            ',
            calculationYear, calculationScenario, method, municipality
        ) INTO gco2kwh_a;

            /* Lasketaan päästöt tilanteessa, jossa käytetään paikallista rakennusaineistoa */
            /* Calculating final emission when using local building data */
            IF heatSource IS NOT NULL THEN
                -- Lämmityksen energiankulutus (kwh) * gco2 per kwh
                SELECT heating_kwh / COALESCE(hyotysuhde, 1)::real *
                    CASE WHEN heatSource = 'kaukolampo' THEN gco2kwh_a[1]
                    WHEN heatSource = 'kevyt_oljy' THEN gco2kwh_a[2]
                    WHEN heatSource = 'raskas_oljy' THEN gco2kwh_a[3]
                    WHEN heatSource = 'kaasu' THEN gco2kwh_a[4]
                    WHEN heatSource = 'sahko' THEN gco2kwh_a[5]
                    WHEN heatSource = 'puu' THEN gco2kwh_a[6]
                    WHEN heatSource = 'turve' THEN gco2kwh_a[7]
                    WHEN heatSource = 'hiili' THEN gco2kwh_a[8]
                    WHEN heatSource = 'maalampo' THEN gco2kwh_a[9]
                    WHEN heatSource = 'muu_lammitys' THEN gco2kwh_a[10] ELSE 1 END
                INTO result_gco2;
                RETURN result_gco2;
            ELSE
            
            /* Lasketaan päästöt tilanteessa, jossa käytetään YKR-rakennusaineistoa */
            /* Calculating final emissions when using YKR-based building data */
                SELECT array(SELECT(heating_kwh / unnest(hyotysuhde_a))) INTO lammitys_kwh_a;
                SELECT array(SELECT unnest(gco2kwh_a) * unnest(lammitys_kwh_a) * unnest(lammitysosuus)) INTO lammitys_co2_a;

                /* Palauta CO2-ekvivalenttia */
                /* Return CO2-equivalents */
                SELECT SUM(a) FROM unnest(lammitys_co2_a) a INTO result_gco2;
                RETURN result_gco2;
            END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;