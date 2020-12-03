 CREATE TABLE user_output."output_uuid3" AS SELECT * FROM il_laske_co2paastot('ykr_uuid', 'ykr_rak_Tampere_rahu',2019,'wem','em','hankinta','Tampere') ;
 alter table output_uuid3 add column geom geometry('MultiPolygon', 3067);
 UPDATE output_uuid3 results set geom = ykr.geom from user_input."ykr_uuid" ykr WHERE ykr.xyind = results.xyind;

SELECT * FROM il_calculate_emissions_new(
    'YKR_vaesto_2019_Pirkanmaa', -- YKR-väestödata | YKR population data
    'ykr_tyopaikat_2017_tampere', -- YKR-työpaikkadata | YKR workplace data
    'ykr_rak_tampere_rahu_uusi_toimipaikat_25092020', -- ykr rakennusdatan taulunimi
    'tampere_kantakaupunki', -- Tutkimusalue | area of interest
    2019, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    'kasvu', -- PITKO:n mukainen kehitysskenaario
    'em', -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    'hankinta' , -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    'Tampere',  -- Alue, jolle päästöjä ollaan laskemassa
    2019  -- Laskennan lähtövuosi
)



CREATE TABLE tests.rak_vesi_rahu_energiam AS
SELECT ykr2.xyind, SUM((SELECT il_lamminkayttovesi_co2(erpien_ala, 2019, 'erpien', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(rivita_ala, 2019, 'rivita', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(askert_ala, 2019, 'askert', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em','hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(liike_ala, 2019, 'liike', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(tsto_ala, 2019, 'tsto', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(liiken_ala, 2019, 'liiken', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(hoito_ala, 2019, 'hoito', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(kokoon_ala, 2019, 'kokoon', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(opetus_ala, 2019, 'opetus', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(teoll_ala, 2019, 'teoll', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(varast_ala, 2019, 'varast', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_lamminkayttovesi_co2(muut_ala, 2019, 'muut', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)))
FROM user_input."ykr_rak_Tampere_rahu" ykr2 WHERE rakv != 0 GROUP BY ykr2.xyind;

CREATE TABLE tests.rak_vesi_rahu_eienergiam AS
SELECT  ykr2.xyind,SUM((SELECT il_lamminkayttovesi_co2(erpien_ala, 2019, 'erpien', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(rivita_ala, 2019, 'rivita', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(askert_ala, 2019, 'askert', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em','hankinta')) +
            (SELECT il_lamminkayttovesi_co2(liike_ala, 2019, 'liike', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(tsto_ala, 2019, 'tsto', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(liiken_ala, 2019, 'liiken', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(hoito_ala, 2019, 'hoito', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(kokoon_ala, 2019, 'kokoon', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(opetus_ala, 2019, 'opetus', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(teoll_ala, 2019, 'teoll', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(varast_ala, 2019, 'varast', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_lamminkayttovesi_co2(muut_ala, 2019, 'muut', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')))
FROM user_input."ykr_rak_Tampere_rahu" ykr2 WHERE rakv != 0 group by ykr2.xyind;



CREATE TABLE tests.rak_lampo_eienergiam AS
SELECT ykr2.xyind, SUM((SELECT il_tilat_lammitys_co2(erpien_ala, 2019, 'erpien', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(rivita_ala, 2019, 'rivita', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(askert_ala, 2019, 'askert', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em','hankinta')) +
            (SELECT il_tilat_lammitys_co2(liike_ala, 2019, 'liike', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(tsto_ala, 2019, 'tsto', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(liiken_ala, 2019, 'liiken', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(hoito_ala, 2019, 'hoito', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(kokoon_ala, 2019, 'kokoon', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(opetus_ala, 2019, 'opetus', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(teoll_ala, 2019, 'teoll', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(varast_ala, 2019, 'varast', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(muut_ala, 2019, 'muut', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')))
FROM user_input."ykr_rak_Tampere" ykr2 WHERE rakv != 0 GROUP by ykr2.xyind;

CREATE TABLE tests.rak_lampo_rahu_eienergiam AS
SELECT ykr2.xyind, SUM((SELECT il_tilat_lammitys_co2(erpien_ala, 2019, 'erpien', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(rivita_ala, 2019, 'rivita', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(askert_ala, 2019, 'askert', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em','hankinta')) +
            (SELECT il_tilat_lammitys_co2(liike_ala, 2019, 'liike', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(tsto_ala, 2019, 'tsto', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(liiken_ala, 2019, 'liiken', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(hoito_ala, 2019, 'hoito', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(kokoon_ala, 2019, 'kokoon', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(opetus_ala, 2019, 'opetus', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(teoll_ala, 2019, 'teoll', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(varast_ala, 2019, 'varast', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')) +
            (SELECT il_tilat_lammitys_co2(muut_ala, 2019, 'muut', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta')))
FROM user_input."ykr_rak_Tampere_rahu" ykr2 WHERE rakv != 0 GROUP by ykr2.xyind;

 CREATE TABLE tests.rak_lampo_rahu_energiam AS
 SELECT ykr2.xyind, SUM((SELECT il_tilat_lammitys_co2(erpien_ala, 2019, 'erpien', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(rivita_ala, 2019, 'rivita', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(askert_ala, 2019, 'askert', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em','hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(liike_ala, 2019, 'liike', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(tsto_ala, 2019, 'tsto', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(liiken_ala, 2019, 'liiken', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(hoito_ala, 2019, 'hoito', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(kokoon_ala, 2019, 'kokoon', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(opetus_ala, 2019, 'opetus', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(teoll_ala, 2019, 'teoll', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(varast_ala, 2019, 'varast', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)) +
            (SELECT il_tilat_lammitys_co2(muut_ala, 2019, 'muut', ykr2.rakv, 'kaukolampo_tre', 'wem', 'em', 'hankinta', ykr2.energiam)))
FROM user_input."ykr_rak_Tampere_rahu" ykr2 WHERE rakv != 0 GROUP by ykr2.xyind;

alter table rak_lampo_eienergiam add column geom geometry('MultiPolygon', 3067);
UPDATE rak_lampo_eienergiam results set geom = ykr.geom from user_input."ykr_uuid2" ykr WHERE ykr.xyind = results.xyind;

alter table rak_lampo_rahu_energiam add column geom geometry('MultiPolygon', 3067);
UPDATE rak_lampo_rahu_energiam results set geom = ykr.geom from user_input."ykr_uuid2" ykr WHERE ykr.xyind = results.xyind;

alter table rak_lampo_rahu_eienergiam add column geom geometry('MultiPolygon', 3067);
UPDATE rak_lampo_rahu_eienergiam results set geom = ykr.geom from user_input."ykr_uuid2" ykr WHERE ykr.xyind = results.xyind;   


create table user_input.ykr_uuid2 as 
SELECT * FROM il_numerize('ykr_uuid', 2019, 2040, 2035, 'Tampere', 'kt_uuid', 'kv_uuid', 'jl_uuid')





SELECT * FROM il_calculate_future_emissions(
    'YKR_vaesto_2017_Tampere', -- YKR-väestödata | YKR population data
    'YKR_tyopaikat_2015_Tampere', -- YKR-työpaikkadata | YKR workplace data
    'ykr_rak_Tampere', -- ykr rakennusdatan taulunimi
    'tutkimusalue_uuid', -- Tutkimusalue | area of interest
    2019, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    'wem', -- PITKO:n mukainen kehitysskenaario
    'em', -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    'hankinta' , -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    'Tampere',  -- Alue, jolle päästöjä ollaan laskemassa
    2019,  -- Laskennan lähtövuosi
    2040, -- Laskennan tavoitevuosi
    'kt_uuid', -- Käyttötarkoitusalueet -taulun nimi (kaava/suunnitelmatieto)
    'kv_uuid', -- Keskusverkko -taulun nimi (valinnainen)
    'jl_uuid') -- Joukkoliikenne -taulun nimi (valinnainen)


SELECT * FROM il_calculate_emissions(
    'YKR_vaesto_2017_Tampere', -- YKR-väestödata | YKR population data
    'YKR_tyopaikat_2015_Tampere', -- YKR-työpaikkadata | YKR workplace data
    'ykr_rak_Tampere', -- ykr rakennusdatan taulunimi
    'tutkimusalue_uuid', -- Tutkimusalue | area of interest
    2019, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    'wem', -- PITKO:n mukainen kehitysskenaario
    'em', -- Päästöallokointimenetelmä, 'em' tai 'hjm'
    'hankinta' , -- Sähkön päästölaji, 'hankinta' tai 'tuotanto'
    'Tampere',  -- Alue, jolle päästöjä ollaan laskemassa
    2019  -- Laskennan lähtövuosi
)


-- -- alter table output_uuid3 add column geom geometry('MultiPolygon', 3067);
-- UPDATE output_uuid3 results set geom = ykr.geom from user_input."ykr_uuid" ykr WHERE ykr.xyind = results.xyind;



CREATE TABLE rakymp."sahko_kiinteisto_kwhm2a" AS 
WITH index AS (SELECT DISTINCT ON (skenaario, rakv) skenaario, rakv FROM rakymp."sahko_kiinteisto_kwhm2" WHERE skenaario != 'static'),
erpien as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as erpien FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'erpien'),
rivita as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as rivita FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'rivita'),
askert as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as askert FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'askert'),
liike as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as liike FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'liike'),
tsto as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as tsto FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'tsto'),
liiken as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as liiken FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'liiken'),
hoito as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as hoito FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'hoito'),
kokoon as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as kokoon FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'kokoon'),
opetus as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as opetus FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'opetus'),
teoll as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as teoll FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'teoll'),
varast as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as varast FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'varast'),
muut as (SELECT skenaario, rakv, sahko_kiinteisto_kwhm2 as muut FROM rakymp."sahko_kiinteisto_kwhm2" WHERE rakennus_tyyppi = 'muut')
SELECT DISTINCT ON (i.skenaario, i.rakv) i.skenaario, i.rakv, erpien.erpien, rivita.rivita, askert.askert, liike.liike, tsto.tsto, liiken.liiken, hoito.hoito, kokoon.kokoon, opetus.opetus, teoll.teoll, varast.varast, muut.muut
FROM index i 
LEFT JOIN erpien on i.skenaario = erpien.skenaario AND i.rakv = erpien.rakv
LEFT JOIN rivita on i.skenaario = rivita.skenaario AND i.rakv = rivita.rakv
LEFT JOIN askert on i.skenaario = askert.skenaario AND i.rakv = askert.rakv
LEFT JOIN liike on i.skenaario = liike.skenaario AND i.rakv = liike.rakv
LEFT JOIN tsto on i.skenaario = tsto.skenaario AND i.rakv = tsto.rakv
LEFT JOIN liiken on i.skenaario = liiken.skenaario AND i.rakv = liiken.rakv
LEFT JOIN hoito on i.skenaario = hoito.skenaario AND i.rakv = hoito.rakv
LEFT JOIN kokoon on i.skenaario = kokoon.skenaario AND i.rakv = kokoon.rakv
LEFT JOIN opetus on i.skenaario = opetus.skenaario AND i.rakv = opetus.rakv
LEFT JOIN teoll on i.skenaario = teoll.skenaario AND i.rakv = teoll.rakv
LEFT JOIN varast on i.skenaario = varast.skenaario AND i.rakv = varast.rakv
LEFT JOIN muut on i.skenaario = muut.skenaario AND i.rakv = muut.rakv


create table user_output."ykrgrid" as select * from il_preprocess('kuntaraja_Tampere', 'YKR_vaesto_2017_Tampere', 'YKR_tyopaikat_2015_Tampere');
select sum(v_yht) from ykrgrid;

sum
229654

sum
230505