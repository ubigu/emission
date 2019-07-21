/* Rakennusten lämmitys | Heating of buildings

Rakennusten lämmityksen kasvihuonekaasupäästöjen laskennalla tarkoitetaan tässä rakennusten nettomääräisen lämmitysenergian tarpeen arviointia.
Tämä tarkoittaa rakennusten tilojen lämmittämiseen tarvittavaa energiaa, josta on vähennetty henkilöistä, valaistuksesta ja sähkölaitteista syntyvien lämpökuormien energia,
poistoilmasta ja muista energiavirroista talteen otettu tilojen lämmityksessä hyödynnetty energia ja ikkunoiden kautta tuleva auringon säteilyenergia.

Lämmitysmuotoihin – tai oikeammin lämmönlähteisiin – sisältyvät kauko- ja aluelämpö, kevyt polttoöljy, raskas polttoöljy, maakaasu, sähkölämmitys, puupolttoaineet, turve,
kivihiili ja koksi, maalämpö ja muut vastaavat lämpöpumput sekä muut lämmitysmuodot. Jälkimmäiseen ryhmään sisältyvät myös tilastojen tuntemattomat lämmitysmuodot.

YKR-ruudun rakennustyyppien eri ikäluokkien lämmitysmuotojen tilojen nettolämmitystarpeet tilat_lammitystarve [kWh/a] lasketaan seuraavalla kaavalla.
 
    tilat_lammitystarve =  rakennus_ala * tilat_kwhm2  * lammitystarve_vuosi / lammitystarve_vertailu / lammitystarve_korjaus_vkunta

Jos tarkasteltavan alueen rakennusten tyyppi-, ikäluokka- ja lämmitysmuotojakauma perustuu kunnan omaan paikkatietopohjaiseen aineistoon,
rakennus_ala -tieto on lämmitysmuotokohtainen. Tämä tarkempi laskentatapa ottaa huomioon keskimääräiseen jakaumaan perustuvaa laskentaa paremmin huomioon lämmitysmuotojakauman ruutukohtaiset erot.
Muutoin voidaan käyttää esilaskettua rakennustyyppi- ja ikäluokkakohtaista keskimääristä lämmitysmuotojakaumaa, joka on tarkasteltavasta YKR-ruudusta riippumaton.
 
YKR-ruudun rakennustyyppien tilojen lämmitykseen vuoden aikana tarvittu ostoenergia tilat_kwh [kWh/a] on
    
    tilat_kwh = tilat_lammitystarve / tilat_hyotysuhde

Rakennustyyppien tilojen lämmityksen tarvitseman ostoenergian kasvihuonekaasupäästöt tilat_co2 [CO2-ekv/a] saadaan kaavalla
 
    tilat_co2 = tilat_kwh * (lmuoto_apu1 * klampo_gco2kwh + lmuoto_apu2 * sahko_gco2kwh + lmuoto_apu3 * tilat_gco2kwh)

*/
DROP FUNCTION IF EXISTS il_prop_heat_co2;
CREATE OR REPLACE FUNCTION
public.il_prop_heat_co2(
    rakennus_ala real, -- Rakennustyypin tietyn ikäluokan kerrosala YKR-ruudussa laskentavuonna. Arvo riippuu laskentavuodesta, rakennuksen tyypistä ja ikäluokasta ja paikallista aineistoa käytettäessä lämmitysmuodosta [m2]
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi, esim. 'erpien', 'rivita'
    rakennusvuosi integer, -- Rakennusvuosikymmen tai -vuosi (2017 alkaen)
    lammitystarve real,  /* Lämmitystarve vuositasolla */ /* Annual heating demand */
    gco2kwh_a real[], -- Lopulliset omainaispäästökertoimet
    scenario varchar, -- PITKO:n mukainen kehitysskenaario
    lammitysmuoto varchar default null -- Rakennuksen lämmityksessä käytettävä primäärinen energiamuoto 'energiam', mikäli tällainen on lisätty YKR/rakennusdataan
)
RETURNS real AS
$$
DECLARE -- Joillekin muuttujille on sekä yksittäiset että array-tyyppiset muuttujat, riippuen siitä, onko lähtödatana YKR-dataa (array) vai paikallisesti jalostettua rakennusdataa
    tilat_kwhm2 real; -- Rakennustyypin ikäluokkakohtaisesti määritelty yhden kerrosneliömetrin lämmittämiseen vuodessa tarvittavan nettolämmitysenergian määrä laskentavuonna. Kerroin huomioi olevan rakennuskannan energiatehokkuuden kehityksen. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta sekä rakennuksen ikäluokasta ja tyypistä [kWh/m2/a].
    hyotysuhde real; -- Rakennustyypin ikäluokan lämmitysjärjestelmäkohtainen keskimääräinen vuosihyötysuhde tai lämpökerroin. Lukuarvo riippuu rakennuksen ikäluokasta, tyypistä ja lämmitysmuodosta [ei yksikköä].
    hyotysuhde_a real[]; -- Rakennustyypin ikäluokan keskimääräiset vuosihyötysuhteet eri lämmitysjärjestelmille. Lukuarvo riippuu rakennuksen ikäluokasta ja tyypistä [ei yksikköä].
    lammitys_kwh real; -- Lämmityksen energiankulutus
    lammitys_kwh_a real[]; -- Lämmityksen energiankulutus (array)
    lammitys_co2_a real[]; -- Lämmityksen kasvihuonekaasupäästöt [gCO2]
    lammitysosuus real[]; -- Lämmitysmuotojen keskimääräiset osuudet rakennustyypin kerrosalasta tietyssä ikäluokassa laskentavuonna. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, rakennuksen ikäluokasta ja tyypistä.
    gco2kwh real; -- Lopulliset omainaispäästökertoimet
BEGIN

    /* Palautetaan nolla, mikäli ruudun kerrosala on 0, -1 tai NULL */
    /* Returning zero, if grid cell has 0, -1 or NULL built floor area */
    IF rakennus_ala <= 0 OR rakennus_ala IS NULL THEN
        RETURN 0;
    /* Muussa tapauksessa jatka laskentaan */
    /* In other cases continue with the calculation */
    ELSE
        /* Käytetään kun on johdettu paikallisesta aineistosta lämmitys/energiamuototiedot ruututasolle */
        /* Used when local building register data has been used to derive grid level information incl. heating methods of building */
        IF lammitysmuoto IS NOT NULL THEN
            EXECUTE 'SELECT ' || lammitysmuoto || ' FROM rakymp.tilat_hyotysuhde WHERE rakennus_tyyppi = $1 AND rakv = $2' INTO hyotysuhde USING rakennustyyppi, rakennusvuosi;
        ELSE 
        /* Käytetään kun käytössä on pelkkää YKR-dataa */
        /* Used when basing the analysis on pure YKR data */
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO lammitysosuus FROM rakymp.lammitysmuotojakauma WHERE skenaario = scenario AND rakennus_tyyppi = rakennustyyppi AND rakv = rakennusvuosi AND vuosi = year;
            SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys]
                INTO hyotysuhde_a FROM rakymp.tilat_hyotysuhde WHERE rakennus_tyyppi = rakennustyyppi AND rakv = rakennusvuosi;
        END IF;

            /* Erityyppisten tilojen ominaislämmitystarve kerrosneliötä kohden */
            /* Energy demand for different building types, per square meter floor space */
            EXECUTE 'SELECT ' || rakennustyyppi || ' FROM rakymp.tilat_kwhm2 WHERE skenaario = $1 AND rakv = $2 AND vuosi = $3'
                INTO tilat_kwhm2 USING scenario, rakennusvuosi, year;

            /* Lasketaan päästöt tilanteessa, jossa käytetään paikallista rakennusaineistoa */
            /* Calculating final emission when using local building data */
            IF lammitysmuoto IS NOT NULL THEN
                SELECT (rakennus_ala * tilat_kwhm2 * lammitystarve) / hyotysuhde INTO lammitys_kwh;
                SELECT CASE
                    WHEN lammitysmuoto = 'kaukolampo' THEN gco2kwh_a[1]
                    WHEN lammitysmuoto = 'kevyt_oljy' THEN gco2kwh_a[2]
                    WHEN lammitysmuoto = 'raskas_oljy' THEN gco2kwh_a[3]
                    WHEN lammitysmuoto = 'kaasu' THEN gco2kwh_a[4]
                    WHEN lammitysmuoto = 'sahko' THEN gco2kwh_a[5]
                    WHEN lammitysmuoto = 'puu' THEN gco2kwh_a[6]
                    WHEN lammitysmuoto = 'turve' THEN gco2kwh_a[7]
                    WHEN lammitysmuoto = 'hiili' THEN gco2kwh_a[8]
                    WHEN lammitysmuoto = 'maalampo' THEN gco2kwh_a[9]
                    WHEN lammitysmuoto = 'muu_lammitys' THEN gco2kwh_a[10]
                    END INTO gco2kwh;
                RETURN lammitys_kwh * gco2kwh; -- lammitys_co2
            ELSE
            
            /* Lasketaan päästöt tilanteessa, jossa käytetään YKR-rakennusaineistoa */
            /* Calculating final emissions when using YKR-based building data */
                SELECT array(SELECT(rakennus_ala * tilat_kwhm2 * lammitystarve) / unnest(hyotysuhde_a)) INTO lammitys_kwh_a;
                SELECT array(SELECT unnest(gco2kwh_a) * unnest(lammitys_kwh_a) * unnest(lammitysosuus)) INTO lammitys_co2_a;

                /* Palauta CO2-ekvivalenttia */
                /* Return CO2-equivalents */
                RETURN SUM(a) FROM unnest(lammitys_co2_a) a;
            END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;