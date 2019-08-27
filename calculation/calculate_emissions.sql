DROP FUNCTION IF EXISTS il_calculate_emissions;
CREATE OR REPLACE FUNCTION
public.il_calculate_emissions(
    ykr_v text, -- YKR-väestödata | YKR population data
    ykr_tp text, -- YKR-työpaikkadata | YKR workplace data
    ykr_rakennukset text, -- ykr rakennusdatan taulunimi
    aoi text, -- Tutkimusalue | area of interest
    year integer, -- Laskennan viitearvojen vuosi || calculation reference year
    scenario varchar, -- PITKO-kehitysskenaario
    method varchar, -- Päästöallokoinnin laskentamenetelmä
    sahkolaji varchar, -- Sähkön päästölaji
    area varchar, -- Alue, jolle päästöjä ollaan laskemassa
    baseYear integer, -- Laskennan lähtövuosi
    targetYear integer default null, -- Laskennan tavoitevuosi
    kt_taulu text default null, -- Taulu, jossa käyttötarkoitusalueet tai vastaavat
    kv_taulu text default null, -- Taulu, jossa keskusverkkotiedot 
    jl_taulu text default null) -- Taulu, jossa intensiivinen joukkoliikennejärjestelmä    
RETURNS TABLE(
    xyind varchar(13),
    tilat_vesi_tco2 real,
    tilat_lammitys_tco2 real,
    tilat_jaahdytys_tco2 real,
    sahko_kiinteistot_tco2 real,
    sahko_kotitaloudet_tco2 real,
    sahko_palv_tco2 real,
    sahko_tv_tco2 real,
    liikenne_hlo_tco2 real,
    liikenne_tv_tco2 real,
    liikenne_palv_tco2 real,
    rak_korjaussaneeraus_tco2 real,
    rak_purku_tco2 real,
    rak_uudis_tco2 real,
    sum_yhteensa_tco2 real,
    sum_lammonsaato_tco2 real,
    sum_liikenne_tco2 real,
    sum_sahko_tco2 real,
    sum_rakentaminen_tco2 real,
    asukkaat int,
    vuosi date,
    geom geometry)
AS $$
DECLARE
    kaukolampotaulu text;
    localbuildings boolean;
    refined boolean;
    defaultdemolition boolean;
    initial_scenario varchar;
    initial_year integer;

    gco2kwh_a real[];
    lmuoto_apu1 real[]; -- Dummy-muuttuja, jonka avulla laskelmissa huomioidaan kaukolämmitetyissä rakennuksissa erikseen määritelty kaukolämmön ominaispäästökerroin. Arvo riippuu lämmitysmuodosta [ei yksikköä].
    lmuoto_apu2 real[]; -- Dummy-muuttuja, jonka avulla laskennassa huomioidaan sähkö- ja lämpöpumppulämmitteisten rakennuksissa erikseen määritelty sähkön ominaispäästökerroin. Lukuarvo riippuu lämmitysmuodosta [ei yksikköä].
    lmuoto_apu3 real[]; -- Dummy-muuttuja, jonka avulla laskennassa huomioidaan muilla kuin kaukolämmöllä, sähköllä ja maalämmöllä lämmitettyjen rakennusten lämmönlähteiden ominaispäästökertoimet. Riippuu lämmitysmuodosta [ei yksikköä].
    jmuoto_apu1 real[]; -- Dummy-kerroin, jonka avulla laskennassa huomioidaan rakennusten jäähdytyksessä käytetyn sähkön osalta erikseen määritelty sähkön ominaispäästökerroin. Arvo riippuu lämmitysmuodosta [ei yksikköä]
    jmuoto_apu2 real[];-- Dummy-kerroin, jolla laskennassa huomioidaan muut rakennusten jäähdytyksen energialähteet kuin sähkö. Arvo riippuu lämmitysmuodosta [ei yksikköä]
    tilat_gco2kwh real[]; -- Lämmönlähteiden kasvihuonekaasupäästöjen ominaispäästökertoimet. Lukuarvo riippuu laskentavuodesta ja lämmitysmuodosta [gCO2-ekv/kWh].
    lammitystarve real; -- Tilojen vuosittainen lämmitystarve
    klampo_gco2kwh real; -- Laskentavuonna kulutetun kaukolämmön ominaispäästökerroin. Arvo riippuu taustaskenaariosta ja laskentavuodesta [gCO2-ekv/kWh]
    j_gco2kwh real[]; -- Jäähdytystavasta riippuva kasvihuonekaasujen esiominaispäästökerroin. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta ja jäähdytystavasta [gCO2-ekv/kWh]
    jaahdytys_gco2kwh real[]; -- Lopulliset jäähdytyksen omainaispäästökertoimet jaahdytys_gco2kwh [gCO2-ekv/kWh] on jäähdytystavasta riippuva kasvihuonekaasujen ominaispäästökerroin.
    sahko_gco2kwh real;  -- Laskentavuonna kulutetun sähkön ominaispäästökerroin. Arvo riippuu taustaskenaariosta, laskentavuodesta, laskentametodista ja sähkön päästölajista [gCO2-ekv/kWh].
    sahko_as real;

    /* Traffic globals */
    new_lj boolean;
    kvoima_apu1 real[]; -- Dummy-muuttuja, jolla huomioidaan laskennassa sähkön käyttö sähköautoissa, pistokehybrideissä ja polttokennoautojen vedyn tuotannossa [ei yksikköä]. Lukuarvo riippuu käyttövoimasta ja laskentavuodesta.
    kvoima_foss_osa real[]; -- Käyttövoimien fossiiliset osuudet [ei yksikköä]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta ja käyttövoimasta.
    kvoima_gco2kwh real[]; -- Käyttövoimien  kasvihuonekaasujen ominaispäästökerroin käytettyä energiayksikköä kohti [gCO2-ekv/kWh]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta (ja käyttövoimasta).
    kvoima_apu2 real[]; -- Dummy-muuttuja, jonka avulla huomioidaan laskennassa muut käyttövoimat kuin sähkö [ei yksikköä]. Lukuarvo riippuu käyttövoimasta ja laskentavuodesta.
    gco2kwh_matrix real[];

    muunto_massa real default 0.000001; -- Muuntaa grammat tonneiksi (0.000001) [t/g].
BEGIN

    /* Ei voi edetä, mikäli tavoitevuosi annettu, mutta ei kaavadataa */
    /* Cannot proceed if target year given but no plan data */
    IF targetYear IS NOT NULL AND kt_taulu IS NULL THEN
        RAISE EXCEPTION 'Failed attempt to calculate future emissions - no plan data given.';
    END IF;

    /* Jos valitaan 'static'-skenaario, eli huomioidaan laskennassa vain yhdyskuntarakenteen muutos, asetetaan PITKO-skenaarioksi 'wem'.
        Samalla sidotaan laskennan referenssivuodeksi laskennan aloitusvuosi.
        If the 'static' skenaario is selected, i.e. only changes in the urban structure are taken into account, set the PITKO skenaario to 'wem'.
        At the same time, fix the calculation reference year into current year / baseYear */
    IF scenario = 'static' THEN
        scenario := 'wem';
        initial_scenario := 'static';
    END IF;

    /* Tarkistetaan, onko käytössä paikallisesti johdettua rakennusdataa, joka sisältää energiamuototiedon */
    /* Checking, whether or not local building data with energy source information is present */
    SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = ykr_rakennukset AND column_name='energiam') INTO localbuildings;

    /* Tarkistetaan, onko käytössä paikallisesti johdettua rakennusdataa, joka sisältää tarkemmat TOL-johdannaiset */
    /* Checking, whether or not local building data with detailed usage source information is present */
    SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = ykr_rakennukset AND column_name='myymal_pien_ala') INTO refined;

    /* Luodaan väliaikainen taulu, joka sisältää mm. YKR väestö- ja työpaikkatiedot */
    /* Creating a temporary table with e.g. YKR population and workplace data */
    IF year = baseYear OR targetYear = NULL THEN
        RAISE NOTICE 'Preprocessing raw data';
        /* Luodaan väliaikainen taulu, joka sisältää mm. YKR väestö- ja työpaikkatiedot */
        /* Creating a temporary table with e.g. YKR population and workplace data */
        EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr1 AS SELECT * FROM (SELECT * FROM il_preprocess('''|| aoi ||''', '''|| ykr_v ||''', '''|| ykr_tp ||''')) ykvtp';
        CREATE INDEX ON ykr1 (vyoh);
    END IF;

    IF targetYear IS NOT NULL THEN
        
        /* Numeeristetaan suunnitelma-aineistoa | 'Numerizing' the given plan data */
        CREATE TEMP TABLE IF NOT EXISTS ykr1_temp AS SELECT * FROM il_numerize('ykr1', baseYear, targetYear, year, area, kt_taulu, kv_taulu, jl_taulu);
        DROP TABLE IF EXISTS ykr1;
        ALTER TABLE ykr1_temp RENAME TO ykr1;

        /* Luodaan pohja-aineisto rakennusdatan työstölle */
        /* Building a template for manipulating building data */
        IF year = baseYear THEN
            EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS rak_initial AS SELECT * FROM ' || quote_ident(ykr_rakennukset) || ' WHERE rakv::int != 0 AND xyind IN (SELECT ykr1.xyind FROM ykr1) AND rakv::int < ' || year;
        ELSE 
            ALTER TABLE ykr2 RENAME to rak_initial;
        END IF;

    END IF;

    /* Kaukolämmön ominaispäästökertoimet (etsitään ensin oikean kaukolämpötaulun nimi) */
    /* Emission values for district heating (first finding out the name of the correct district heating table) */
    SELECT kaukolampo FROM aluejaot.alueet WHERE kunta = area OR maakunta = area INTO kaukolampotaulu;
    EXECUTE 'SELECT gco2kwh FROM energia.'||kaukolampotaulu||' kl WHERE kl.vuosi = $1 AND kl.skenaario = $2 AND kl.metodi = $3'
        INTO klampo_gco2kwh USING year, scenario, method;

    /* Sähkön ominaispäästökertoimet */
    /* Electricity emission values */
    SELECT sahko.gco2kwh INTO sahko_gco2kwh FROM energia.sahko AS sahko WHERE
        sahko.vuosi = year AND
        sahko.skenaario = scenario AND
        sahko.metodi = method AND
        sahko.paastolaji = sahkolaji;

    SELECT sahko_koti_as INTO sahko_as FROM energia.sahko_koti_as sas WHERE sas.vuosi = year AND sas.skenaario = scenario;

    /* Lämmitystarve vuositasolla | Annual heating demand
        lammitys_korjaus_vkunta [ei yksikköä] on paikkakuntakohtainen lämmitystarpeen korjauskerroin suhteessa lämmitystarvelaskennan vertailupaikkakuntaan. Lukuarvo riippuu laskentavuodesta. 
        lammitystarve_vuosi [ei yksikköä] on tarkastelupaikkakunnan lämmitystarvelaskennan vertailupaikkakunnan lämmitystarveluku tai sen ennuste. Lukuarvo riippuu laskentavuodesta.
        lammitystarve_vertailu [ei yksikköä] on tarkastelupaikkakunnan lämmitystarvelaskennan vertailupaikkakunnan lämmitystarveluvun vertailuarvo. Lukuarvo riippuu laskentavuodesta.
        
        Tilanteessa, jossa lämmitysenergian ominaiskulutuksesta tilat_kwhm2 ei ole paikallisia parametreja,
        käytetään vertailupaikkakunnan kertoimen sijaan paikkakuntakohtaista lämmitystarpeen korjauskerrointa Jyväskylän suhteen.
            lammitys_korjaus_jkl [ei yksikköä] on paikkakuntakohtainen lämmitystarpeen korjauskerroin Jyväskylään. Lukuarvo riippuu laskentavuodesta.
    */
    SELECT (lammitystarve_vuosi::real / lammitystarve_vertailu::real / lammitys_korjaus_vkunta::real)
        INTO lammitystarve FROM energia.lammitystarve as lt
        WHERE lt.vuosi = year;
   
    /* Dummy-kertoimet lämmitysmuodoille | Dummy multipliers by method of heating */
    SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys] INTO lmuoto_apu1 FROM energia.lmuoto_apu WHERE type = 'lmuoto_apu1';
    SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys] INTO lmuoto_apu2 FROM energia.lmuoto_apu WHERE type = 'lmuoto_apu2';
    SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys] INTO lmuoto_apu3 FROM energia.lmuoto_apu WHERE type = 'lmuoto_apu3';
    SELECT array[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys] INTO tilat_gco2kwh FROM energia.tilat_gco2kwh t WHERE t.vuosi = year;

    SELECT array(SELECT unnest(lmuoto_apu1) * klampo_gco2kwh + unnest(lmuoto_apu2) * sahko_gco2kwh + unnest(lmuoto_apu3) * unnest(tilat_gco2kwh)) INTO gco2kwh_a;

    /* Dummy-kertoimet jäähdytysmuodoille | Dummy multipliers by method of cooling */
    SELECT array[kaukok, sahko, pumput, muu] INTO jmuoto_apu1 FROM energia.jmuoto_apu WHERE type = 'jmuoto_apu1';
    SELECT array[kaukok, sahko, pumput, muu] INTO jmuoto_apu2 FROM energia.jmuoto_apu WHERE type = 'jmuoto_apu2';
    
    /* Jäähdytyksen ominaispäästökertoimet | Emission values for cooling */
    SELECT array[kaukok,sahko,pumput,muu] FROM energia.jaahdytys_gco2kwh ej WHERE ej.vuosi = year AND ej.skenaario = scenario INTO j_gco2kwh;
    SELECT array(SELECT unnest(jmuoto_apu1) * sahko_gco2kwh + unnest(j_gco2kwh) * unnest(jmuoto_apu2)) INTO jaahdytys_gco2kwh;

    --------------------------------------------------------
    /* Liikenteen globaalimuuttujat - Ominaispäästötietojen esikäsittely, kulkumuodosta riippumattomia */ 
    /* Global parameters for traffic - emission values preprocessing, independent of traffic mode */

    SELECT array[bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu]
        INTO kvoima_apu1 FROM liikenne.kvoima_apu1 a1
        WHERE a1.vuosi = year;
    SELECT array[bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu]
        INTO kvoima_foss_osa FROM liikenne.kvoima_foss_osa f
        WHERE f.vuosi = year AND f.skenaario = scenario;
    SELECT array[bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu]
        INTO kvoima_gco2kwh FROM liikenne.kvoima_gco2kwh v
        WHERE v.vuosi = year AND v.skenaario = scenario;
    SELECT array[bensiini, etanoli, diesel, kaasu, phev_b, phev_d, ev, kv_muu]
        INTO kvoima_apu2 FROM liikenne.kvoima_apu2 a2
        WHERE a2.vuosi = year;

    /* Kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] määritellään
    käyttövoimien ominaispäästökertoimien suoriteosuuksilla painotettuna keskiarvona huomioiden samalla niiden bio-osuudet. */

    SELECT array(SELECT sahko_gco2kwh * unnest(kvoima_apu1) + unnest(kvoima_gco2kwh) * unnest(kvoima_foss_osa) * unnest(kvoima_apu2)) INTO gco2kwh_matrix;

    SELECT EXISTS (SELECT 1 FROM ykr1 WHERE vyoh = 9993) INTO new_lj;

    --------------------------------------------------------

    IF targetYear IS NOT NULL THEN
        SELECT CASE WHEN k_poistuma > 999998 AND k_poistuma < 1000000 THEN TRUE ELSE FALSE END FROM ykr1 LIMIT 1 INTO defaultdemolition;

        /* Luodaan väliaikainen taulu rakennusten purkamisen päästölaskentaa varten */
        /* Creating a temporary table for emission calculations of demolishing buildings */
        IF defaultdemolition = TRUE THEN
            CREATE TEMP TABLE poistuma_alat AS 
            WITH poistuma AS (
                SELECT ykr1.xyind FROM ykr1 GROUP BY ykr1.xyind
            ),
            buildings AS (
                SELECT rak_initial.xyind, 
                    /* Default demolishing rate: 0.15% annually of existing building stock.
                    Huuhka, S. & Lahdensivu J. Statistical and geographical study on demolish buildings. Building research and information vol 44:1, 73-96. */
                    0.0015 * SUM(rakyht_ala)::real rakyht,
                    0.0015 * SUM(erpien_ala)::real erpien,
                    0.0015 * SUM(rivita_ala)::real rivita,
                    0.0015 * SUM(askert_ala)::real askert,
                    0.0015 * SUM(liike_ala)::real liike,
                    0.0015 * SUM(tsto_ala)::real tsto,
                    0.0015 * SUM(liiken_ala)::real liiken,
                    0.0015 * SUM(hoito_ala)::real hoito,
                    0.0015 * SUM(kokoon_ala)::real kokoon,
                    0.0015 * SUM(opetus_ala)::real opetus,
                    0.0015 * SUM(teoll_ala)::real teoll,
                    0.0015 * SUM(varast_ala)::real varast,
                    0.0015 * SUM(muut_ala)::real muut
                FROM rak_initial GROUP BY rak_initial.xyind
            )
            SELECT poistuma.xyind, erpien, rivita, askert, liike, tsto, liiken, hoito, kokoon, opetus, teoll, varast, muut
            FROM poistuma LEFT JOIN buildings ON buildings.xyind = poistuma.xyind;

        ELSE

            CREATE TEMP TABLE poistuma_alat AS 
            WITH poistuma AS (
                SELECT ykr1.xyind, SUM(k_poistuma) AS poistuma FROM ykr1 GROUP BY ykr1.xyind
            ),
            buildings AS (
                SELECT rak_initial.xyind, 
                    SUM(rakyht_ala) rakyht_ala,
                    SUM(erpien_ala) erpien_ala,
                    SUM(rivita_ala) rivita_ala,
                    SUM(askert_ala) askert_ala,
                    SUM(liike_ala) liike_ala,
                    SUM(tsto_ala) tsto_ala,
                    SUM(liiken_ala) liiken_ala,
                    SUM(hoito_ala) hoito_ala,
                    SUM(kokoon_ala) kokoon_ala,
                    SUM(opetus_ala) opetus_ala,
                    SUM(teoll_ala) teoll_ala,
                    SUM(varast_ala) varast_ala,
                    SUM(muut_ala) muut_ala
                FROM rak_initial GROUP BY rak_initial.xyind
            )
            SELECT poistuma.xyind,
                COALESCE(poistuma * (erpien_ala::real / NULLIF(rakyht_ala::real,0)),0) erpien,
                COALESCE(poistuma * (rivita_ala::real / NULLIF(rakyht_ala::real,0)),0) rivita,
                COALESCE(poistuma * (askert_ala::real / NULLIF(rakyht_ala::real,0)),0) askert,
                COALESCE(poistuma * (liike_ala::real / NULLIF(rakyht_ala::real,0)),0) liike,
                COALESCE(poistuma * (tsto_ala::real / NULLIF(rakyht_ala::real,0)),0) tsto,
                COALESCE(poistuma * (liiken_ala::real / NULLIF(rakyht_ala::real,0)),0) liiken,
                COALESCE(poistuma * (hoito_ala::real / NULLIF(rakyht_ala::real,0)),0) hoito,
                COALESCE(poistuma * (kokoon_ala::real / NULLIF(rakyht_ala::real,0)),0) kokoon,
                COALESCE(poistuma * (opetus_ala::real / NULLIF(rakyht_ala::real,0)),0) opetus,
                COALESCE(poistuma * (teoll_ala::real / NULLIF(rakyht_ala::real,0)),0) teoll,
                COALESCE(poistuma * (varast_ala::real / NULLIF(rakyht_ala::real,0)),0) varast,
                COALESCE(poistuma * (muut_ala::real / NULLIF(rakyht_ala::real,0)),0) muut
            FROM poistuma LEFT JOIN buildings ON buildings.xyind = poistuma.xyind
            WHERE poistuma > 0;
        END IF;

        /* Kyselyt: Puretaan rakennukset datasta ja rakennetaan uusia */
        /* Valitaan ajettava kysely sen perusteella, millaista rakennusdataa on käytössä */
        /* Queries: Demolishing and buildings buildings from the building data */
        /* Choose correct query depending on the type of building data in use */
        RAISE NOTICE 'Updating building data';
        IF localbuildings = true THEN
            IF refined = true THEN 
                EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, energiam, rakyht_ala :: int, asuin_ala :: int, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, myymal_pien_ala :: int, myymal_super_ala :: int, myymal_hyper_ala :: int, myymal_muu_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, teoll_elint_ala :: int, teoll_tekst_ala :: int, teoll_puu_ala :: int, teoll_paper_ala :: int, teoll_miner_ala :: int, teoll_kemia_ala :: int, teoll_kone_ala :: int, teoll_mjalos_ala :: int, teoll_metal_ala :: int, teoll_vesi_ala :: int, teoll_energ_ala :: int, teoll_yhdysk_ala :: int, teoll_kaivos_ala :: int, teoll_muu_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM (SELECT * FROM il_update_buildings_refined(''rak_initial'', ''ykr1'', '|| year ||', '|| baseYear || ', '|| targetYear||', '''|| scenario ||''')) updatedbuildings';
            ELSE 
                EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, energiam, rakyht_ala :: int, asuin_ala :: int, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM (SELECT * FROM il_update_buildings_local(''rak_initial'', ''ykr1'', '|| year ||', '|| baseYear || ', '|| targetYear||', '''|| scenario ||''')) updatedbuildings';
            END IF;
            CREATE INDEX ON ykr2 (rakv, energiam);
        ELSE
            EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, rakyht_ala :: int, asuin_ala :: int, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM (SELECT * FROM il_update_buildings(''rak_initial'', ''ykr1'', ' || year ||')) updatedbuildings';
            CREATE INDEX ON ykr2 (rakv);        
        END IF;
        DROP TABLE IF EXISTS rak_initial;

    ELSE 

        /* Valitaan rakennustietojen väliaikaisen taulun generointikysely ajettavaksi sen perusteella, millaista rakennusdataa on käytössä */
        /* Choose correct query for creating a temporary building data table depending on the type of building data in use */
        IF localbuildings = true THEN
            IF refined = true THEN 
                EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, energiam, rakyht_ala :: int, asuin_ala :: int, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, myymal_pien_ala :: int, myymal_super_ala :: int, myymal_hyper_ala :: int, myymal_muu_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, teoll_elint_ala :: int, teoll_tekst_ala :: int, teoll_puu_ala :: int, teoll_paper_ala :: int, teoll_miner_ala :: int, teoll_kemia_ala :: int, teoll_kone_ala :: int, teoll_mjalos_ala :: int, teoll_metal_ala :: int, teoll_vesi_ala :: int, teoll_energ_ala :: int, teoll_yhdysk_ala :: int, teoll_kaivos_ala :: int, teoll_muu_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM '|| quote_ident(ykr_rakennukset) ||' WHERE rakv::int != 0 AND xyind IN (SELECT ykr1.xyind FROM ykr1)';
            ELSE 
                EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, energiam, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM '|| quote_ident(ykr_rakennukset) ||' WHERE rakv::int != 0 AND xyind IN (SELECT ykr1.xyind FROM ykr1)';
            END IF;
            CREATE INDEX ON ykr2 (rakv, energiam);
        ELSE
            EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr2 AS SELECT xyind, rakv::int, erpien_ala :: int, rivita_ala :: int, askert_ala :: int, liike_ala :: int, myymal_ala :: int, majoit_ala :: int, asla_ala :: int, ravint_ala :: int, tsto_ala :: int, liiken_ala :: int, hoito_ala :: int, kokoon_ala :: int, opetus_ala :: int, teoll_ala :: int, varast_ala :: int, muut_ala :: int, teoll_lkm :: int, varast_lkm :: int FROM '|| quote_ident(ykr_rakennukset) ||' WHERE rakv::int != 0 AND xyind IN (SELECT ykr1.xyind FROM ykr1)';
            CREATE INDEX ON ykr2 (rakv); -- update?
        END IF;

    END IF;
    
    /* Luodaan väliaikainen taulu laskennan tuloksille */
    /* Creating temporary table for analysis results */
    DROP TABLE IF EXISTS results;
    CREATE TEMP TABLE IF NOT EXISTS results as select y.xyind,
        0::real tilat_vesi_tco2,
        0::real tilat_lammitys_tco2,
        0::real tilat_jaahdytys_tco2,
        0::real sahko_kiinteistot_tco2,
        0::real sahko_kotitaloudet_tco2,
        0::real sahko_palv_tco2,
        0::real sahko_tv_tco2,
        0::real liikenne_hlo_tco2,
        0::real liikenne_tv_tco2,
        0::real liikenne_palv_tco2,
        0::real rak_korjaussaneeraus_tco2,
        0::real rak_purku_tco2,
        0::real rak_uudis_tco2
    FROM ykr1 y WHERE y.v_yht > 0 OR y.tp_yht > 0 OR y.xyind IN (SELECT ykr2.xyind FROM ykr2);
    
    /* Kun käytetään static-skenaariota tulevaisuuslaskennassa, aseta laskenta lähtövuoden referenssitasolle */
    /* When using a 'static' scenario in the future scenario calculation, set the calculation reference year to baseYear */
    IF initial_scenario = 'static' AND targetYear IS NOT NULL THEN
        initial_year := year;
        year := baseYear;
    END IF;

    /* Täytetään tulostaulukko laskennan tuloksilla */
    /* Fill results table with calculations */

    IF localbuildings = TRUE THEN

    UPDATE results SET 
        tilat_vesi_tco2 = rakennukset.tilat_vesi_co2 * muunto_massa,
        tilat_lammitys_tco2 = rakennukset.tilat_lammitys_co2 * muunto_massa
    FROM
        (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
        SUM((SELECT il_prop_water_co2(erpien_ala, year, 'erpien', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(rivita_ala, year, 'rivita', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(askert_ala, year, 'askert', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(liike_ala, year, 'liike', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(tsto_ala, year, 'tsto', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(liiken_ala, year, 'liiken', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(hoito_ala, year, 'hoito', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(opetus_ala, year, 'opetus', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(teoll_ala, year, 'teoll', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(varast_ala, year, 'varast', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_water_co2(muut_ala, year, 'muut', ykr2.rakv, gco2kwh_a, scenario, ykr2.energiam)))
        AS tilat_vesi_co2,
        /* Rakennusten lämmitys | Heating of buildings */
        SUM((SELECT il_prop_heat_co2(erpien_ala, year, 'erpien', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(rivita_ala, year, 'rivita', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(askert_ala, year, 'askert', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(liike_ala, year, 'liike', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(tsto_ala, year, 'tsto', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(liiken_ala, year, 'liiken', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(hoito_ala, year, 'hoito', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(opetus_ala, year, 'opetus', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(teoll_ala, year, 'teoll', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(varast_ala, year, 'varast', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)) +
            (SELECT il_prop_heat_co2(muut_ala, year, 'muut', ykr2.rakv, lammitystarve, gco2kwh_a, scenario, ykr2.energiam)))
        AS tilat_lammitys_co2
        FROM ykr2
        GROUP BY ykr2.xyind) rakennukset
        WHERE rakennukset.xyind = results.xyind;

    ELSE 

        UPDATE results SET 
            tilat_vesi_tco2 = rakennukset.tilat_vesi_co2 * muunto_massa,
            tilat_lammitys_tco2 = rakennukset.tilat_lammitys_co2 * muunto_massa
        FROM
            (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
            SUM((SELECT il_prop_water_co2(erpien_ala, year, 'erpien', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(rivita_ala, year, 'rivita', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(askert_ala, year, 'askert', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(liike_ala, year, 'liike', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(tsto_ala, year, 'tsto', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(liiken_ala, year, 'liiken', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(hoito_ala, year, 'hoito', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(opetus_ala, year, 'opetus', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(teoll_ala, year, 'teoll', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(varast_ala, year, 'varast', ykr2.rakv, gco2kwh_a, scenario)) +
                (SELECT il_prop_water_co2(muut_ala, year, 'muut', ykr2.rakv, gco2kwh_a, scenario)))
            AS tilat_vesi_co2,
            /* Rakennusten lämmitys | Heating of buildings */
            SUM((SELECT il_prop_heat_co2(erpien_ala, year, 'erpien', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(rivita_ala, year, 'rivita', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(askert_ala, year, 'askert', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(liike_ala, year, 'liike', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(tsto_ala, year, 'tsto', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(liiken_ala, year, 'liiken', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(hoito_ala, year, 'hoito', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(opetus_ala, year, 'opetus', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(teoll_ala, year, 'teoll', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(varast_ala, year, 'varast', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)) +
                (SELECT il_prop_heat_co2(muut_ala, year, 'muut', ykr2.rakv, lammitystarve, gco2kwh_a, scenario)))
            AS tilat_lammitys_co2
            FROM ykr2
            GROUP BY ykr2.xyind) rakennukset
            WHERE rakennukset.xyind = results.xyind;
    END IF;
    
    UPDATE results
    SET 
        tilat_jaahdytys_tco2 = rakennukset.tilat_jaahdytys_co2 * muunto_massa,
        sahko_kiinteistot_tco2 = rakennukset.sahko_kiinteistot_co2 * muunto_massa,
        sahko_kotitaloudet_tco2 = rakennukset.sahko_kotitaloudet_co2 * muunto_massa,
        rak_korjaussaneeraus_tco2 = rakennukset.rak_korjaussaneeraus_co2 * muunto_massa
    FROM
        (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
        /* Käyttöveden lämmitys | Heating of water */
        /* Rakennusten jäähdytys | Cooling of buildings */
        SUM((SELECT il_prop_cool_co2(erpien_ala, year, 'erpien', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(rivita_ala, year, 'rivita', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(askert_ala, year, 'askert', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(liike_ala, year, 'liike', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(tsto_ala, year, 'tsto', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(liiken_ala, year, 'liiken', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(hoito_ala, year, 'hoito', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(opetus_ala, year, 'opetus', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(teoll_ala, year, 'teoll', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(varast_ala, year, 'varast', ykr2.rakv, scenario, jaahdytys_gco2kwh)) +
            (SELECT il_prop_cool_co2(muut_ala, year, 'muut', ykr2.rakv, scenario, jaahdytys_gco2kwh)))
        AS tilat_jaahdytys_co2,
        /* Kiinteistösähkö | Electricity consumption of property technology */
        -- SUM(il_el_property_co2a(array[erpien_ala, rivita_ala, askert_ala, liike_ala, tsto_ala, liiken_ala, hoito_ala, kokoon_ala, opetus_ala, teoll_ala, varast_ala, muut_ala], year, ykr2.rakv, scenario, sahko_gco2kwh))
        SUM((SELECT il_el_property_co2(erpien_ala, year, 'erpien', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(rivita_ala, year, 'rivita', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(askert_ala, year, 'askert', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(liike_ala, year, 'liike', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(tsto_ala, year, 'tsto', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(liiken_ala, year, 'liiken', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(hoito_ala, year, 'hoito', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(opetus_ala, year, 'opetus', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(teoll_ala, year, 'teoll', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(varast_ala, year, 'varast', ykr2.rakv, scenario, sahko_gco2kwh)) +
            (SELECT il_el_property_co2(muut_ala, year, 'muut', ykr2.rakv, scenario, sahko_gco2kwh)))
        AS sahko_kiinteistot_co2,
        /* Kotitalouksien sähkönkulutus | Energy consumption of households */
        SUM((SELECT il_el_household_co2(erpien_ala, year, 'erpien', scenario, sahko_gco2kwh)) +
            (SELECT il_el_household_co2(rivita_ala, year, 'rivita', scenario, sahko_gco2kwh)) +
            (SELECT il_el_household_co2(askert_ala, year, 'askert', scenario, sahko_gco2kwh)))
        AS sahko_kotitaloudet_co2,
        /* Korjausrakentaminen ja saneeraus | Renovations and large-scale overhauls of buildings */
        SUM((SELECT il_build_renovate_co2(erpien_ala, year, 'erpien', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(rivita_ala, year, 'rivita', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(askert_ala, year, 'askert', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(liike_ala, year, 'liike', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(tsto_ala, year, 'tsto', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(liiken_ala, year, 'liiken', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(hoito_ala, year, 'hoito', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(kokoon_ala, year, 'kokoon', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(opetus_ala, year, 'opetus', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(teoll_ala, year, 'teoll', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(varast_ala, year, 'varast', ykr2.rakv, scenario)) +
            (SELECT il_build_renovate_co2(muut_ala, year, 'muut', ykr2.rakv, scenario)))
        AS rak_korjaussaneeraus_co2
        FROM ykr2
        GROUP BY ykr2.xyind) rakennukset
        WHERE rakennukset.xyind = results.xyind;
   
    IF targetYear IS NOT NULL THEN
        UPDATE results
        SET 
            rak_uudis_tco2 = rakennukset.rak_uudis_co2 * muunto_massa
        FROM
            (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
            SUM(
                (SELECT il_build_new_co2(erpien_ala, year, 'erpien', scenario)) +
                (SELECT il_build_new_co2(rivita_ala, year, 'rivita', scenario)) +
                (SELECT il_build_new_co2(askert_ala, year, 'askert', scenario)) +
                (SELECT il_build_new_co2(liike_ala, year, 'liike', scenario)) +
                (SELECT il_build_new_co2(tsto_ala, year, 'tsto', scenario)) +
                (SELECT il_build_new_co2(liiken_ala, year, 'liiken', scenario)) +
                (SELECT il_build_new_co2(hoito_ala, year, 'hoito', scenario)) +
                (SELECT il_build_new_co2(kokoon_ala, year, 'kokoon', scenario)) +
                (SELECT il_build_new_co2(opetus_ala, year, 'opetus', scenario)) +
                (SELECT il_build_new_co2(teoll_ala, year, 'teoll', scenario)) +
                (SELECT il_build_new_co2(varast_ala, year, 'varast', scenario)) +
                (SELECT il_build_new_co2(muut_ala, year, 'muut', scenario)))
            AS rak_uudis_co2
            FROM ykr2 WHERE ykr2.rakv = year
            GROUP BY ykr2.xyind) rakennukset
            WHERE rakennukset.xyind = results.xyind;

    END IF;

    IF refined = FALSE THEN
        UPDATE results
        SET 
            sahko_palv_tco2 = rakennukset.sahko_palv_co2 * muunto_massa,
            sahko_tv_tco2 = rakennukset.sahko_tv_co2 * muunto_massa,
            liikenne_tv_tco2 = rakennukset.liikenne_tv_co2 * muunto_massa,
            liikenne_palv_tco2 = rakennukset.liikenne_palv_co2 * muunto_massa
        FROM
            (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
            /* Palveluiden sähkönkulutus | Electricity consumption of services */
            SUM((SELECT il_el_iwhs_co2(liike_ala, year, 'liike', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(tsto_ala, year, 'tsto', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(liiken_ala, year, 'liiken', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(hoito_ala, year, 'hoito', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(kokoon_ala, year, 'kokoon', scenario, sahko_gco2kwh)) +	
                (SELECT il_el_iwhs_co2(opetus_ala, year, 'opetus', scenario, sahko_gco2kwh)))
            AS sahko_palv_co2,
            /* Teollisuus ja varastot, sähkönkulutus | Electricity consumption of industry and warehouses */
            SUM((SELECT il_el_iwhs_co2(teoll_ala, year, 'teoll', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(varast_ala, year, 'varast', scenario, sahko_gco2kwh)))
            AS sahko_tv_co2,
            /* Teollisuus- ja varastoliikenne | Industry and logistics traffic */
            SUM((SELECT il_traffic_iwhs_co2(teoll_lkm, year, 'teoll', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(teoll_lkm, year, 'teoll', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(varast_lkm, year, 'varast', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(varast_lkm, year, 'varast', 'kauto', scenario, gco2kwh_matrix)))
            AS liikenne_tv_co2,
            /* Palveluliikenne | Service traffic */
            SUM((SELECT il_traffic_iwhs_co2(myymal_ala, year, 'myymal', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_ala, year, 'myymal', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(majoit_ala, year, 'majoit', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(majoit_ala, year, 'majoit', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(asla_ala, year, 'asla', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(asla_ala, year, 'asla', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(ravint_ala, year, 'ravint', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(ravint_ala, year, 'ravint', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(tsto_ala, year, 'tsto', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(tsto_ala, year, 'tsto', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(liiken_ala, year, 'liiken', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(liiken_ala, year, 'liiken', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(hoito_ala, year, 'hoito', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(hoito_ala, year, 'hoito', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(kokoon_ala, year, 'kokoon', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(kokoon_ala, year, 'kokoon', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(opetus_ala, year, 'opetus', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(opetus_ala, year, 'opetus', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(muut_ala, year, 'muut', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(muut_ala, year, 'muut', 'kauto', scenario, gco2kwh_matrix)))
            AS liikenne_palv_co2
            FROM ykr2
            GROUP BY ykr2.xyind) rakennukset
            WHERE rakennukset.xyind = results.xyind;
    ELSE 
     UPDATE results
        SET 
            sahko_palv_tco2 = rakennukset.sahko_palv_co2 * muunto_massa,
            sahko_tv_tco2 = rakennukset.sahko_tv_co2 * muunto_massa,
            liikenne_tv_tco2 = rakennukset.liikenne_tv_co2 * muunto_massa,
            liikenne_palv_tco2 = rakennukset.liikenne_palv_co2 * muunto_massa
        FROM
            (SELECT DISTINCT ON (ykr2.xyind) ykr2.xyind,
            /* Palveluiden sähkönkulutus | Electricity consumption of services */
            SUM((SELECT il_el_iwhs_co2(myymal_hyper_ala, year, 'myymal_hyper', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(myymal_super_ala, year, 'myymal_super', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(myymal_pien_ala, year, 'myymal_pien', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(myymal_muu_ala, year, 'myymal_muu', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(majoit_ala, year, 'majoit', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(asla_ala, year, 'asla', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(ravint_ala, year, 'ravint', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(tsto_ala, year, 'tsto', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(liiken_ala, year, 'liiken', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(hoito_ala, year, 'hoito', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(kokoon_ala, year, 'kokoon', scenario, sahko_gco2kwh)) +	
                (SELECT il_el_iwhs_co2(opetus_ala, year, 'opetus', scenario, sahko_gco2kwh)))
            AS sahko_palv_co2,
            /* Teollisuus ja varastot, sähkönkulutus | Electricity consumption of industry and warehouses */
            SUM((SELECT il_el_iwhs_co2(teoll_kaivos_ala, year, 'teoll_kaivos', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_elint_ala, year, 'teoll_elint', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_tekst_ala, year, 'teoll_tekst', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_puu_ala, year, 'teoll_puu', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_paper_ala, year, 'teoll_paper', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_kemia_ala, year, 'teoll_kemia', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_miner_ala, year, 'teoll_miner', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_mjalos_ala, year, 'teoll_mjalos', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_metal_ala, year, 'teoll_metal', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_kone_ala, year, 'teoll_kone', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_muu_ala, year, 'teoll_muu', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_energ_ala, year, 'teoll_energ', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_vesi_ala, year, 'teoll_vesi', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(teoll_yhdysk_ala, year, 'teoll_yhdysk', scenario, sahko_gco2kwh)) +
                (SELECT il_el_iwhs_co2(varast_ala, year, 'varast', scenario, sahko_gco2kwh)))
            AS sahko_tv_co2,
            /* Teollisuus- ja varastoliikenne | Industry and logistics traffic */
            SUM((SELECT il_traffic_iwhs_co2(teoll_lkm, year, 'teoll', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(teoll_lkm, year, 'teoll', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(varast_lkm, year, 'varast', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(varast_lkm, year, 'varast', 'kauto', scenario, gco2kwh_matrix)))
            AS liikenne_tv_co2,
            /* Palveluliikenne | Service traffic */
            SUM((SELECT il_traffic_iwhs_co2(myymal_hyper_ala, year, 'myymal_hyper', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_hyper_ala, year, 'myymal_hyper', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_super_ala, year, 'myymal_super', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_super_ala, year, 'myymal_super', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_pien_ala, year, 'myymal_pien', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_pien_ala, year, 'myymal_pien', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_muu_ala, year, 'myymal_muu', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(myymal_muu_ala, year, 'myymal_muu', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(majoit_ala, year, 'majoit', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(majoit_ala, year, 'majoit', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(asla_ala, year, 'asla', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(asla_ala, year, 'asla', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(ravint_ala, year, 'ravint', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(ravint_ala, year, 'ravint', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(tsto_ala, year, 'tsto', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(tsto_ala, year, 'tsto', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(liiken_ala, year, 'liiken', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(liiken_ala, year, 'liiken', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(hoito_ala, year, 'hoito', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(hoito_ala, year, 'hoito', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(kokoon_ala, year, 'kokoon', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(kokoon_ala, year, 'kokoon', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(opetus_ala, year, 'opetus', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(opetus_ala, year, 'opetus', 'kauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(muut_ala, year, 'muut', 'pauto', scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_iwhs_co2(muut_ala, year, 'muut', 'kauto', scenario, gco2kwh_matrix)))
            AS liikenne_palv_co2
            FROM ykr2
            GROUP BY ykr2.xyind) rakennukset
            WHERE rakennukset.xyind = results.xyind;
        END IF;

    IF targetYear IS NOT NULL THEN 
            /* Lasketaan rakennusten purkamisen päästöt */
        /* Calculating emissions for demolishing buildings */
        UPDATE results SET rak_purku_tco2 = poistot.rak_purku_co2 * muunto_massa
            FROM (SELECT p.xyind,
                SUM(
                (SELECT il_build_demolish_co2(p.erpien::real, year, 'erpien', scenario)) +
                (SELECT il_build_demolish_co2(p.rivita::real, year, 'rivita', scenario)) +
                (SELECT il_build_demolish_co2(p.askert::real, year, 'askert', scenario)) +
                (SELECT il_build_demolish_co2(p.liike::real, year, 'liike', scenario)) +
                (SELECT il_build_demolish_co2(p.tsto::real, year, 'tsto', scenario)) +
                (SELECT il_build_demolish_co2(p.liiken::real, year, 'liiken', scenario)) +
                (SELECT il_build_demolish_co2(p.hoito::real, year, 'hoito', scenario)) +
                (SELECT il_build_demolish_co2(p.kokoon::real, year, 'kokoon', scenario)) +
                (SELECT il_build_demolish_co2(p.opetus::real, year, 'opetus', scenario)) +
                (SELECT il_build_demolish_co2(p.teoll::real, year, 'teoll', scenario)) +
                (SELECT il_build_demolish_co2(p.varast::real, year, 'varast', scenario)) +
                (SELECT il_build_demolish_co2(p.muut::real, year, 'muut', scenario)))
                AS rak_purku_co2
                FROM poistuma_alat p
                GROUP BY p.xyind) poistot
        WHERE results.xyind = poistot.xyind;
        /* Poistetaan purkulaskennoissa käytetty väliaikainen taulu */
        /* Remove the temporary table used in demolishing calculationg */
        DROP TABLE IF EXISTS poistuma_alat;
    END IF;

    UPDATE results SET
        liikenne_hlo_tco2 = COALESCE(pop.liikenne_hlo_co2 * muunto_massa, 0),
        sahko_kotitaloudet_tco2 = COALESCE(results.sahko_kotitaloudet_tco2 + NULLIF(pop.sahko_kotitaloudet_co2_as * muunto_massa, 0), 0)
    FROM
        (SELECT ykr1.xyind,
            SUM((SELECT il_traffic_personal_co2(v_yht, tp_yht, year, 'bussi', centdist, vyoh, area, scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_personal_co2(v_yht, tp_yht, year, 'raide', centdist, vyoh, area, scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_personal_co2(v_yht, tp_yht, year, 'hlauto', centdist, vyoh, area, scenario, gco2kwh_matrix)) +
                (SELECT il_traffic_personal_co2(v_yht, tp_yht, year, 'muu', centdist, vyoh, area, scenario, gco2kwh_matrix)))
            AS liikenne_hlo_co2,
            SUM((SELECT il_el_household_co2(v_yht, year, NULL, scenario, sahko_gco2kwh, sahko_as)))
            AS sahko_kotitaloudet_co2_as
            FROM ykr1 WHERE v_yht IS NOT NULL OR v_yht > 0 OR tp_yht IS NOT NULL OR tp_yht > 0
        GROUP BY ykr1.xyind) pop
    WHERE pop.xyind = results.xyind;

    IF initial_scenario = 'static' AND targetYear IS NOT NULL THEN
        year := initial_year;
    END IF;

    ALTER TABLE results
        ADD COLUMN IF NOT EXISTS sum_yhteensa_tco2 real,
        ADD COLUMN IF NOT EXISTS sum_lammonsaato_tco2 real,
        ADD COLUMN IF NOT EXISTS sum_liikenne_tco2 real,
        ADD COLUMN IF NOT EXISTS sum_sahko_tco2 real,
        ADD COLUMN IF NOT EXISTS sum_rakentaminen_tco2 real,
        ADD COLUMN IF NOT EXISTS asukkaat int,
        ADD COLUMN IF NOT EXISTS vuosi date,
        ADD COLUMN IF NOT EXISTS geom geometry(MultiPolygon, 3067);

    UPDATE results r SET
        vuosi = to_date(year::varchar, 'YYYY'),
        sum_yhteensa_tco2 = r.tilat_vesi_tco2 + r.tilat_lammitys_tco2 + r.tilat_jaahdytys_tco2 + r.sahko_kiinteistot_tco2 + r.sahko_kotitaloudet_tco2 +
        r.sahko_palv_tco2 + r.sahko_tv_tco2 + r.liikenne_hlo_tco2 + r.liikenne_tv_tco2 + r.liikenne_palv_tco2 + r.rak_korjaussaneeraus_tco2 + r.rak_purku_tco2 + r.rak_uudis_tco2,
        sum_lammonsaato_tco2 = r.tilat_vesi_tco2 + r.tilat_lammitys_tco2 + r.tilat_jaahdytys_tco2,
        sum_liikenne_tco2 = r.liikenne_hlo_tco2 + r.liikenne_tv_tco2 + r.liikenne_palv_tco2, 
        sum_sahko_tco2 = r.sahko_kiinteistot_tco2 + r.sahko_kotitaloudet_tco2 + r.sahko_palv_tco2 + r.sahko_tv_tco2,
        sum_rakentaminen_tco2 = r.rak_korjaussaneeraus_tco2 + r.rak_purku_tco2 + r.rak_uudis_tco2;
    
    UPDATE results res SET geom = ykr1.geom, asukkaat = ykr1.v_yht FROM ykr1 WHERE res.xyind = ykr1.xyind;

    RETURN QUERY SELECT * from results WHERE results.sum_yhteensa_tco2 > 0;
    DROP TABLE results;

    IF targetYear IS NULL THEN
        DROP TABLE ykr1, ykr2;
    ELSE 
        IF year = targetYear THEN
            DROP TABLE ykr1, ykr2;
        END IF;
    END IF;

    --EXCEPTION WHEN others THEN
     --   DROP TABLE IF EXISTS results, ykr1, ykr2;

END;
$$ LANGUAGE plpgsql;