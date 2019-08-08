CREATE OR REPLACE FUNCTION
public.il_update_buildings_local(
    rak_taulu text,
    ykr_taulu text,
    calculationYear integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    baseYear integer,
	targetYear integer,
    kehitysskenaario varchar -- PITKO:n mukainen kehitysskenaario
)
RETURNS TABLE (
    xyind varchar,
    rakv int,
	energiam varchar,
    rakyht_ala integer, 
    asuin_ala integer,
    erpien_ala integer,
    rivita_ala integer,
    askert_ala integer,
    liike_ala integer,
    myymal_ala integer,
    majoit_ala integer,
    asla_ala integer,
    ravint_ala integer,
    tsto_ala integer,
    liiken_ala integer,
    hoito_ala integer,
    kokoon_ala integer,
    opetus_ala integer,
    teoll_ala integer,
    varast_ala integer,
    muut_ala integer,
    teoll_lkm smallint,
    varast_lkm smallint
) AS $$
DECLARE
	energiamuoto varchar;
	laskentavuodet integer[];
	laskenta_length integer;
	step real;
	localweight real;
	globalweight real;
    teoll_koko real;
    varast_koko real;
BEGIN

-- energiamuodot := ARRAY [kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys];
SELECT array(select generate_series(baseYear,targetYear)) INTO laskentavuodet;
SELECT array_length(laskentavuodet,1) into laskenta_length;
SELECT 1::real / laskenta_length INTO step;
SELECT (calculationYear - baseYear + 1) * step INTO globalweight;
SELECT 1 - globalweight INTO localweight;

EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr AS SELECT xyind, vyoh, k_ap_ala, k_ar_ala, k_ak_ala, k_muu_ala, k_poistuma FROM ' || quote_ident(ykr_taulu) || ' WHERE (k_ap_ala IS NOT NULL AND k_ap_ala != 0) OR (k_ar_ala IS NOT NULL AND k_ar_ala != 0) OR (k_ak_ala IS NOT NULL AND k_ak_ala != 0) OR (k_muu_ala IS NOT NULL AND k_muu_ala != 0) OR (k_poistuma IS NOT NULL AND k_poistuma != 0)';
EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS rak AS SELECT xyind, rakv::int, energiam, rakyht_ala::int, asuin_ala::int, erpien_ala::int, rivita_ala::int, askert_ala::int, liike_ala::int, myymal_ala::int, majoit_ala::int, asla_ala::int, ravint_ala::int, tsto_ala::int, liiken_ala::int, hoito_ala::int, kokoon_ala::int, opetus_ala::int, teoll_ala::int, varast_ala::int, muut_ala::int, teoll_lkm::smallint, varast_lkm::smallint FROM ' || quote_ident(rak_taulu) ||' WHERE rakv::int != 0 AND xyind IN (SELECT ykr.xyind from ykr)';

/* Haetaan globaalit lämmitysmuotojakaumat laskentavuodelle ja -skenaariolle */
/* Fetching global heating ratios for current calculation year and scenario */
CREATE TEMP TABLE IF NOT EXISTS global_jakauma AS
	SELECT rakennus_tyyppi, kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys FROM rakymp.lammitysmuotojakauma lmj WHERE lmj.vuosi = calculationYear AND lmj.rakv = calculationYear AND lmj.skenaario = kehitysskenaario;
INSERT INTO global_jakauma (rakennus_tyyppi, kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys)
	SELECT 'rakyht', avg(kaukolampo), avg(kevyt_oljy), avg(raskas_oljy), avg(kaasu), avg(sahko), avg(puu), avg(turve), avg(hiili), avg(maalampo), avg(muu_lammitys)
	FROM global_jakauma;

/* Lasketaan teollisuus- ja varastorakennusten koot */
/* Calculating the sizes of industrial and storage buildings */
SELECT SUM(rak.teoll_ala)/SUM(rak.teoll_lkm) FROM rak into teoll_koko;
SELECT SUM(rak.varast_ala)/SUM(rak.varast_lkm) FROM rak into varast_koko;


/* Puretaan rakennuksia  */
/* Demolishing buildings */
UPDATE rak b SET
    asuin_ala = (CASE WHEN asuin > b.asuin_ala THEN 0 ELSE b.asuin_ala - asuin END),
    erpien_ala = (CASE WHEN erpien > b.erpien_ala THEN 0 ELSE b.erpien_ala - erpien END),
    rivita_ala = (CASE WHEN rivita > b.rivita_ala THEN 0 ELSE b.rivita_ala - rivita END),
    askert_ala = (CASE WHEN askert > b.askert_ala THEN 0 ELSE b.askert_ala - askert END),
    liike_ala = (CASE WHEN liike > b.liike_ala THEN 0 ELSE b.liike_ala - liike END),
    myymal_ala = (CASE WHEN myymal > b.myymal_ala THEN 0 ELSE b.myymal_ala - myymal END),
    majoit_ala = (CASE WHEN majoit > b.majoit_ala THEN 0 ELSE b.majoit_ala - majoit END),
    asla_ala = (CASE WHEN asla > b.asla_ala THEN 0 ELSE b.asla_ala - asla END),
    ravint_ala = (CASE WHEN ravint > b.ravint_ala THEN 0 ELSE b.ravint_ala - ravint END),
    tsto_ala = (CASE WHEN tsto > b.tsto_ala THEN 0 ELSE b.tsto_ala - tsto END),
    liiken_ala = (CASE WHEN liiken > b.liiken_ala THEN 0 ELSE b.liiken_ala - liiken END),
    hoito_ala = (CASE WHEN hoito > b.hoito_ala THEN 0 ELSE b.hoito_ala - hoito END),
    kokoon_ala = (CASE WHEN kokoon > b.kokoon_ala THEN 0 ELSE b.kokoon_ala - kokoon END),
    opetus_ala = (CASE WHEN opetus > b.opetus_ala THEN 0 ELSE b.opetus_ala - opetus END),
    teoll_ala = (CASE WHEN teoll > b.teoll_ala THEN 0 ELSE b.teoll_ala - teoll END),
    varast_ala = (CASE WHEN varast > b.varast_ala THEN 0 ELSE b.varast_ala - varast END),
    muut_ala = (CASE WHEN muut > b.muut_ala THEN 0 ELSE b.muut_ala - muut END),
    teoll_lkm = (CASE WHEN teoll > 0 AND b.teoll_lkm - CEIL(teoll / teoll_koko) > 0 THEN b.teoll_lkm - CEIL(teoll / teoll_koko) ELSE 0 END),
    varast_lkm = (CASE WHEN varast > 0 AND b.varast_lkm - CEIL(varast / varast_koko) > 0 THEN b.varast_lkm - CEIL(varast / varast_koko) ELSE 0 END)
FROM (
WITH poistuma AS (
    SELECT ykr.xyind, SUM(k_poistuma) AS poistuma FROM ykr GROUP BY ykr.xyind
),
buildings AS (
	SELECT rakennukset.xyind, rakennukset.rakv,
		rakennukset.asuin_ala :: real / NULLIF(grouped.rakyht_ala, 0) asuin,
		rakennukset.erpien_ala :: real / NULLIF(grouped.rakyht_ala, 0) erpien,
		rakennukset.rivita_ala :: real / NULLIF(grouped.rakyht_ala, 0) rivita,
		rakennukset.askert_ala :: real / NULLIF(grouped.rakyht_ala, 0) askert,
		rakennukset.liike_ala :: real / NULLIF(grouped.rakyht_ala, 0) liike,
        rakennukset.myymal_ala :: real / NULLIF(grouped.rakyht_ala, 0) myymal,
        rakennukset.majoit_ala :: real / NULLIF(grouped.rakyht_ala, 0) majoit,
        rakennukset.asla_ala :: real / NULLIF(grouped.rakyht_ala, 0) asla,
        rakennukset.ravint_ala :: real / NULLIF(grouped.rakyht_ala, 0) ravint,
		rakennukset.tsto_ala :: real / NULLIF(grouped.rakyht_ala, 0) tsto,
		rakennukset.liiken_ala :: real / NULLIF(grouped.rakyht_ala, 0) liiken,
		rakennukset.hoito_ala :: real / NULLIF(grouped.rakyht_ala, 0) hoito,
		rakennukset.kokoon_ala :: real / NULLIF(grouped.rakyht_ala, 0) kokoon,
		rakennukset.opetus_ala :: real / NULLIF(grouped.rakyht_ala, 0) opetus,
		rakennukset.teoll_ala :: real / NULLIF(grouped.rakyht_ala, 0) teoll,
		rakennukset.varast_ala:: real / NULLIF(grouped.rakyht_ala, 0) varast,
		rakennukset.muut_ala :: real / NULLIF(grouped.rakyht_ala, 0) muut
	FROM rak rakennukset JOIN
	(SELECT build2.xyind, SUM(build2.rakyht_ala) rakyht_ala FROM rak build2 GROUP BY build2.xyind) grouped
	ON grouped.xyind = rakennukset.xyind
	WHERE rakennukset.rakv != calculationYear
)
SELECT poistuma.xyind,
	buildings.rakv,
	poistuma * asuin asuin,
	poistuma * erpien erpien,
	poistuma * rivita rivita,
	poistuma * askert askert,
	poistuma * liike liike,
    poistuma * myymal myymal,
	poistuma * majoit majoit,
	poistuma * asla asla,
	poistuma * ravint ravint,
	poistuma * tsto tsto,
	poistuma * liiken liiken,
	poistuma * hoito hoito,
	poistuma * kokoon kokoon,
	poistuma * opetus opetus,
	poistuma * teoll teoll,
	poistuma * varast varast,
	poistuma * muut muut
FROM poistuma LEFT JOIN buildings ON buildings.xyind = poistuma.xyind
WHERE poistuma > 0 AND buildings.rakv IS NOT NULL) poistumat
WHERE b.xyind = poistumat.xyind AND b.rakv = poistumat.rakv;


/* Lisätään puuttuvat sarakkeet väliaikaiseen YKR-dataan */
/* Adding new columns into the temporary YKR data */
ALTER TABLE ykr
    ADD COLUMN liike_osuus real,
    ADD COLUMN myymal_osuus real,
    ADD COLUMN majoit_osuus real,
    ADD COLUMN asla_osuus real,
    ADD COLUMN ravint_osuus real,
    ADD COLUMN tsto_osuus real,
    ADD COLUMN liiken_osuus real,
    ADD COLUMN hoito_osuus real,
    ADD COLUMN kokoon_osuus real,
    ADD COLUMN opetus_osuus real,
    ADD COLUMN teoll_osuus real,
    ADD COLUMN varast_osuus real,
    ADD COLUMN muut_osuus real,
    ADD COLUMN muu_ala real;


/* Lasketaan eri käyttömuotojen osuudet väliaikaiseen YKR-dataan */
/* Calculating the distribution of building uses into the temporary YKR data */
/* Skipattu tässä vaiheessa */
/* Skipped at this point
UPDATE ykr SET 
    muu_ala = sq.muu_ala,
    liike_osuus = COALESCE(sq.liike_ala / sq.muu_ala, 0),
    myymal_osuus = COALESCE(sq.myymal_ala / sq.muu_ala, 0),
    majoit_osuus = COALESCE(sq.majoit_ala / sq.muu_ala, 0),
    asla_osuus = COALESCE(sq.asla_ala / sq.muu_ala, 0),
    ravint_osuus = COALESCE(sq.ravint_ala / sq.muu_ala, 0),
    tsto_osuus = COALESCE(sq.tsto_ala / sq.muu_ala, 0),
    liiken_osuus = COALESCE(sq.liiken_ala / sq.muu_ala, 0),
    hoito_osuus = COALESCE(sq.hoito_ala / sq.muu_ala, 0),
    kokoon_osuus = COALESCE(sq.kokoon_ala / sq.muu_ala, 0),
    opetus_osuus = COALESCE(sq.opetus_ala / sq.muu_ala, 0),
    teoll_osuus = COALESCE(sq.teoll_ala / sq.muu_ala, 0),
    varast_osuus = COALESCE(sq.varast_ala / sq.muu_ala, 0),
    muut_osuus = COALESCE(sq.muut_ala /sq.muu_ala, 0)
FROM
    (SELECT DISTINCT ON (r.xyind) r.xyind,
		NULLIF(SUM(COALESCE(r.liike_ala,0) + COALESCE(r.tsto_ala,0) + COALESCE(r.liiken_ala,0) + COALESCE(r.hoito_ala,0) + 
			COALESCE(r.kokoon_ala,0) + COALESCE(r.opetus_ala,0) + COALESCE(r.teoll_ala,0) + COALESCE(r.varast_ala,0) + COALESCE(r.muut_ala,0)),0)::real AS muu_ala,
		SUM(r.liike_ala) AS liike_ala,
		SUM(r.myymal_ala) AS myymal_ala,
		SUM(r.majoit_ala) AS majoit_ala,
		SUM(r.asla_ala) AS asla_ala,
		SUM(r.ravint_ala) AS ravint_ala,
		SUM(r.tsto_ala) AS tsto_ala,
		SUM(r.liiken_ala) AS liiken_ala,
		SUM(r.hoito_ala) AS hoito_ala,
		SUM(r.kokoon_ala) AS kokoon_ala,
		SUM(r.opetus_ala) AS opetus_ala,
		SUM(r.teoll_ala) AS teoll_ala,
		SUM(r.varast_ala) AS varast_ala,
		SUM(r.muut_ala) AS muut_ala
	FROM rak r GROUP BY r.xyind ) sq
WHERE sq.xyind = ykr.xyind;
*/
/* Lasketaan myös vakiokäyttötapausjakaumat uusia alueita varten */
/* Käyttöalaperusteinen käyttötapajakauma generoidaan rakennusdatasta UZ-vyöhykkeittäin */
/* Calculate default proportions of building usage for new areas as well */
CREATE TEMP TABLE IF NOT EXISTS kayttotapajakauma AS 
SELECT ykr.vyoh,
	COALESCE(SUM(r.liike_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as liike_osuus,
	COALESCE(SUM(r.myymal_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as myymal_osuus,
	COALESCE(SUM(r.majoit_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as majoit_osuus,
	COALESCE(SUM(r.asla_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as asla_osuus,
	COALESCE(SUM(r.ravint_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as ravint_osuus,
	COALESCE(SUM(r.tsto_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as tsto_osuus,
	COALESCE(SUM(r.liiken_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as liiken_osuus,
	COALESCE(SUM(r.hoito_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as hoito_osuus,
	COALESCE(SUM(r.kokoon_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as kokoon_osuus,
	COALESCE(SUM(r.opetus_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as opetus_osuus,
	COALESCE(SUM(r.teoll_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as teoll_osuus,
	COALESCE(SUM(r.varast_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as varast_osuus,
	COALESCE(SUM(r.muut_ala)::real / NULLIF(SUM(r.liike_ala) + SUM(r.tsto_ala) + SUM(r.liiken_ala) + SUM(r.hoito_ala) + SUM(r.kokoon_ala) + SUM(r.opetus_ala) + SUM(r.teoll_ala) + SUM(r.varast_ala) + SUM(r.muut_ala),0),0) as muut_osuus
FROM rak r JOIN ykr ON r.xyind = ykr.xyind
GROUP BY ykr.vyoh;

UPDATE kayttotapajakauma j SET 
	liike_osuus = ktj.liike_osuus,
	myymal_osuus = ktj.myymal_osuus,
	majoit_osuus = ktj.majoit_osuus,
	asla_osuus = ktj.asla_osuus,
	ravint_osuus = ktj.ravint_osuus,
	tsto_osuus = ktj.tsto_osuus,
	liiken_osuus = ktj.liiken_osuus,
	hoito_osuus = ktj.hoito_osuus,
	kokoon_osuus = ktj.kokoon_osuus,
	opetus_osuus = ktj.opetus_osuus,
	teoll_osuus = ktj.teoll_osuus,
	varast_osuus = ktj.varast_osuus,
	muut_osuus = ktj.muut_osuus
FROM
(SELECT
	AVG(k.liike_osuus) liike_osuus, AVG(k.myymal_osuus) myymal_osuus, AVG(k.majoit_osuus) majoit_osuus, AVG(k.asla_osuus) asla_osuus, AVG(k.ravint_osuus) ravint_osuus,
	AVG(k.tsto_osuus) tsto_osuus, AVG(k.liiken_osuus) liiken_osuus, AVG(k.hoito_osuus) hoito_osuus, AVG(k.kokoon_osuus) kokoon_osuus, AVG(k.opetus_osuus) opetus_osuus, AVG(k.teoll_osuus) teoll_osuus,
	AVG(k.varast_osuus) varast_osuus, AVG(k.muut_osuus) muut_osuus
FROM kayttotapajakauma k
	WHERE k.liike_osuus + k.tsto_osuus + k.liiken_osuus + k.hoito_osuus + k.kokoon_osuus + k.opetus_osuus + k.teoll_osuus + k.varast_osuus + k.muut_osuus = 1
) ktj
WHERE j.liike_osuus + j.tsto_osuus + j.liiken_osuus + j.hoito_osuus + j.kokoon_osuus + j.opetus_osuus + j.teoll_osuus + j.varast_osuus + j.muut_osuus < 0.99;

UPDATE ykr y SET
    liike_osuus = ktj.liike_osuus,
    myymal_osuus = ktj.myymal_osuus,
    majoit_osuus = ktj.majoit_osuus,
    asla_osuus = ktj.asla_osuus,
    ravint_osuus = ktj.ravint_osuus,
    tsto_osuus = ktj.tsto_osuus,
    liiken_osuus = ktj.liiken_osuus,
    hoito_osuus = ktj.hoito_osuus,
    kokoon_osuus = ktj.kokoon_osuus,
    opetus_osuus = ktj.opetus_osuus,
    teoll_osuus = ktj.teoll_osuus,
    varast_osuus = ktj.varast_osuus,
    muut_osuus = ktj.muut_osuus
FROM kayttotapajakauma ktj
WHERE y.vyoh = ktj.vyoh;

/* -- Mikäli käytetään myös ruututason tietoja : 
    (y.liike_osuus IS NULL OR y.liike_osuus = 0) AND
    (y.myymal_osuus IS NULL OR y.myymal_osuus = 0) AND
    (y.majoit_osuus IS NULL OR y.majoit_osuus = 0) AND
    (y.asla_osuus IS NULL OR y.asla_osuus = 0) AND
    (y.ravint_osuus IS NULL OR y.ravint_osuus = 0) AND
    (y.tsto_osuus IS NULL OR y.tsto_osuus = 0) AND
    (y.liiken_osuus IS NULL OR y.liiken_osuus = 0) AND
    (y.hoito_osuus IS NULL OR y.hoito_osuus = 0) AND
    (y.kokoon_osuus IS NULL OR y.kokoon_osuus = 0) AND
    (y.opetus_osuus IS NULL OR y.opetus_osuus = 0) AND
    (y.teoll_osuus IS NULL OR y.teoll_osuus = 0) AND
    (y.varast_osuus IS NULL OR y.varast_osuus = 0) AND
    (y.muut_osuus IS NULL OR y.muut_osuus = 0);
*/

/* Lasketaan nykyisen paikallisesta rakennusdatasta muodostetun ruutuaineiston mukainen ruutukohtainen energiajakauma rakennustyypeittäin */
/* Laskenta tehdään vain 2000-luvulta eteenpäin rakennetuille tai rakennettaville rakennuksille */
CREATE TEMP TABLE IF NOT EXISTS local_jakauma AS
WITH cte AS (
WITH
	index AS (
	SELECT distinct on (rak.xyind) rak.xyind
	FROM rak
	WHERE rak.rakv > 2000
	), kaukolampo AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert,
		SUM(rak.liike_ala) as liike,
		SUM(rak.tsto_ala) as tsto,
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='kaukolampo' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), kevyt_oljy AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='kevyt_oljy' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), raskas_oljy AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='raskas_oljy' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), kaasu AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='kaasu' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), sahko AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='sahko' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), puu AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='puu' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), turve AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='turve' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), hiili AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='hiili' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), maalampo AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert, 
		SUM(rak.liike_ala) as liike, 
		SUM(rak.tsto_ala) as tsto, 
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,		 
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='maalampo' AND rak.rakv > 2000
    GROUP BY rak.xyind
    ), muu_lammitys AS (
    SELECT rak.xyind,
		SUM(rak.rakyht_ala) as rakyht,
		SUM(rak.erpien_ala) as erpien,
		SUM(rak.rivita_ala) as rivita,
		SUM(rak.askert_ala) as askert,
		SUM(rak.liike_ala) as liike,
		SUM(rak.tsto_ala) as tsto,
		SUM(rak.liiken_ala) as liiken,
		SUM(rak.hoito_ala) as hoito,
		SUM(rak.kokoon_ala) as kokoon,
		SUM(rak.opetus_ala) as opetus,
		SUM(rak.teoll_ala) as teoll,
		SUM(rak.varast_ala) as varast,
		SUM(rak.muut_ala) as muut
	FROM rak WHERE rak.energiam='muu_lammitys' AND rak.rakv > 2000
    GROUP BY rak.xyind
)
	
SELECT index.xyind, 'rakyht' as rakennus_tyyppi,
kaukolampo.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS kaukolampo,
kevyt_oljy.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS kevyt_oljy,
raskas_oljy.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS raskas_oljy,
kaasu.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS kaasu,
sahko.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS sahko,
puu.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS puu,
turve.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS turve,
hiili.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS hiili,
maalampo.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS maalampo,
muu_lammitys.rakyht :: float(4)/ NULLIF(COALESCE(kaukolampo.rakyht,0) + COALESCE(kevyt_oljy.rakyht,0) + COALESCE(raskas_oljy.rakyht,0) + COALESCE(kaasu.rakyht,0) + COALESCE(sahko.rakyht,0) + COALESCE(puu.rakyht,0) + COALESCE(turve.rakyht,0) + COALESCE(hiili.rakyht,0) + COALESCE(maalampo.rakyht,0) + COALESCE(muu_lammitys.rakyht,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION
SELECT index.xyind, 'erpien' as rakennus_tyyppi,
kaukolampo.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS kaukolampo,
kevyt_oljy.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS kevyt_oljy,
raskas_oljy.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS raskas_oljy,
kaasu.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS kaasu,
sahko.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS sahko,
puu.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS puu,
turve.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS turve,
hiili.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS hiili,
maalampo.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS maalampo,
muu_lammitys.erpien :: float(4)/ NULLIF(COALESCE(kaukolampo.erpien,0) + COALESCE(kevyt_oljy.erpien,0) + COALESCE(raskas_oljy.erpien,0) + COALESCE(kaasu.erpien,0) + COALESCE(sahko.erpien,0) + COALESCE(puu.erpien,0) + COALESCE(turve.erpien,0) + COALESCE(hiili.erpien,0) + COALESCE(maalampo.erpien,0) + COALESCE(muu_lammitys.erpien,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind 
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION
SELECT index.xyind, 'rivita' as rakennus_tyyppi,
kaukolampo.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS kaukolampo,
kevyt_oljy.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS kevyt_oljy,
raskas_oljy.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS raskas_oljy,
kaasu.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS kaasu,
sahko.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS sahko,
puu.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS puu,
turve.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS turve,
hiili.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS hiili,
maalampo.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS maalampo,
muu_lammitys.rivita :: float(4)/ NULLIF(COALESCE(kaukolampo.rivita,0) + COALESCE(kevyt_oljy.rivita,0) + COALESCE(raskas_oljy.rivita,0) + COALESCE(kaasu.rivita,0) + COALESCE(sahko.rivita,0) + COALESCE(puu.rivita,0) + COALESCE(turve.rivita,0) + COALESCE(hiili.rivita,0) + COALESCE(maalampo.rivita,0) + COALESCE(muu_lammitys.rivita,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind
	  
UNION
SELECT index.xyind, 'askert' as rakennus_tyyppi,
kaukolampo.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS kaukolampo,
kevyt_oljy.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS kevyt_oljy,
raskas_oljy.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS raskas_oljy,
kaasu.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS kaasu,
sahko.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS sahko,
puu.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS puu,
turve.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS turve,
hiili.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS hiili,
maalampo.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS maalampo,
muu_lammitys.askert :: float(4)/ NULLIF(COALESCE(kaukolampo.askert,0) + COALESCE(kevyt_oljy.askert,0) + COALESCE(raskas_oljy.askert,0) + COALESCE(kaasu.askert,0) + COALESCE(sahko.askert,0) + COALESCE(puu.askert,0) + COALESCE(turve.askert,0) + COALESCE(hiili.askert,0) + COALESCE(maalampo.askert,0) + COALESCE(muu_lammitys.askert,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind
	  
UNION
SELECT index.xyind, 'liike' as rakennus_tyyppi,
kaukolampo.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS kaukolampo,
kevyt_oljy.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS kevyt_oljy,
raskas_oljy.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS raskas_oljy,
kaasu.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS kaasu,
sahko.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS sahko,
puu.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS puu,
turve.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS turve,
hiili.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS hiili,
maalampo.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS maalampo,
muu_lammitys.liike :: float(4)/ NULLIF(COALESCE(kaukolampo.liike,0) + COALESCE(kevyt_oljy.liike,0) + COALESCE(raskas_oljy.liike,0) + COALESCE(kaasu.liike,0) + COALESCE(sahko.liike,0) + COALESCE(puu.liike,0) + COALESCE(turve.liike,0) + COALESCE(hiili.liike,0) + COALESCE(maalampo.liike,0) + COALESCE(muu_lammitys.liike,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION
SELECT index.xyind, 'tsto' as rakennus_tyyppi,
kaukolampo.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS kaukolampo,
kevyt_oljy.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS kevyt_oljy,
raskas_oljy.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS raskas_oljy,
kaasu.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS kaasu,
sahko.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS sahko,
puu.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS puu,
turve.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS turve,
hiili.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS hiili,
maalampo.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS maalampo,
muu_lammitys.tsto :: float(4)/ NULLIF(COALESCE(kaukolampo.tsto,0) + COALESCE(kevyt_oljy.tsto,0) + COALESCE(raskas_oljy.tsto,0) + COALESCE(kaasu.tsto,0) + COALESCE(sahko.tsto,0) + COALESCE(puu.tsto,0) + COALESCE(turve.tsto,0) + COALESCE(hiili.tsto,0) + COALESCE(maalampo.tsto,0) + COALESCE(muu_lammitys.tsto,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION 
SELECT index.xyind, 'liiken' as rakennus_tyyppi,
kaukolampo.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS kaukolampo,
kevyt_oljy.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS kevyt_oljy,
raskas_oljy.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS raskas_oljy,
kaasu.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS kaasu,
sahko.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS sahko,
puu.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS puu,
turve.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS turve,
hiili.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS hiili,
maalampo.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS maalampo,
muu_lammitys.liiken :: float(4)/ NULLIF(COALESCE(kaukolampo.liiken,0) + COALESCE(kevyt_oljy.liiken,0) + COALESCE(raskas_oljy.liiken,0) + COALESCE(kaasu.liiken,0) + COALESCE(sahko.liiken,0) + COALESCE(puu.liiken,0) + COALESCE(turve.liiken,0) + COALESCE(hiili.liiken,0) + COALESCE(maalampo.liiken,0) + COALESCE(muu_lammitys.liiken,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind  
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION 
SELECT index.xyind, 'hoito' as rakennus_tyyppi,
kaukolampo.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS kaukolampo,
kevyt_oljy.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS kevyt_oljy,
raskas_oljy.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS raskas_oljy,
kaasu.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS kaasu,
sahko.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS sahko,
puu.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS puu,
turve.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS turve,
hiili.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS hiili,
maalampo.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS maalampo,
muu_lammitys.hoito :: float(4)/ NULLIF(COALESCE(kaukolampo.hoito,0) + COALESCE(kevyt_oljy.hoito,0) + COALESCE(raskas_oljy.hoito,0) + COALESCE(kaasu.hoito,0) + COALESCE(sahko.hoito,0) + COALESCE(puu.hoito,0) + COALESCE(turve.hoito,0) + COALESCE(hiili.hoito,0) + COALESCE(maalampo.hoito,0) + COALESCE(muu_lammitys.hoito,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION 
SELECT index.xyind, 'kokoon' as rakennus_tyyppi,
kaukolampo.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS kaukolampo,
kevyt_oljy.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS kevyt_oljy,
raskas_oljy.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS raskas_oljy,
kaasu.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS kaasu,
sahko.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS sahko,
puu.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS puu,
turve.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS turve,
hiili.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS hiili,
maalampo.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS maalampo,
muu_lammitys.kokoon :: float(4)/ NULLIF(COALESCE(kaukolampo.kokoon,0) + COALESCE(kevyt_oljy.kokoon,0) + COALESCE(raskas_oljy.kokoon,0) + COALESCE(kaasu.kokoon,0) + COALESCE(sahko.kokoon,0) + COALESCE(puu.kokoon,0) + COALESCE(turve.kokoon,0) + COALESCE(hiili.kokoon,0) + COALESCE(maalampo.kokoon,0) + COALESCE(muu_lammitys.kokoon,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION 
SELECT index.xyind, 'opetus' as rakennus_tyyppi,
kaukolampo.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS kaukolampo,
kevyt_oljy.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS kevyt_oljy,
raskas_oljy.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS raskas_oljy,
kaasu.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS kaasu,
sahko.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS sahko,
puu.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS puu,
turve.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS turve,
hiili.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS hiili,
maalampo.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS maalampo,
muu_lammitys.opetus :: float(4)/ NULLIF(COALESCE(kaukolampo.opetus,0) + COALESCE(kevyt_oljy.opetus,0) + COALESCE(raskas_oljy.opetus,0) + COALESCE(kaasu.opetus,0) + COALESCE(sahko.opetus,0) + COALESCE(puu.opetus,0) + COALESCE(turve.opetus,0) + COALESCE(hiili.opetus,0) + COALESCE(maalampo.opetus,0) + COALESCE(muu_lammitys.opetus,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind 
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind 
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind 
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION 
SELECT index.xyind, 'teoll' as rakennus_tyyppi,
kaukolampo.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS kaukolampo,
kevyt_oljy.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS kevyt_oljy,
raskas_oljy.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS raskas_oljy,
kaasu.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS kaasu,
sahko.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS sahko,
puu.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS puu,
turve.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS turve,
hiili.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS hiili,
maalampo.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS maalampo,
muu_lammitys.teoll :: float(4)/ NULLIF(COALESCE(kaukolampo.teoll,0) + COALESCE(kevyt_oljy.teoll,0) + COALESCE(raskas_oljy.teoll,0) + COALESCE(kaasu.teoll,0) + COALESCE(sahko.teoll,0) + COALESCE(puu.teoll,0) + COALESCE(turve.teoll,0) + COALESCE(hiili.teoll,0) + COALESCE(maalampo.teoll,0) + COALESCE(muu_lammitys.teoll,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind 
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind 
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind 
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind 
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind 
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind 

UNION 
SELECT index.xyind, 'varast' as rakennus_tyyppi,
kaukolampo.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS kaukolampo,
kevyt_oljy.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS kevyt_oljy,
raskas_oljy.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS raskas_oljy,
kaasu.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS kaasu,
sahko.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS sahko,
puu.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS puu,
turve.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS turve,
hiili.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS hiili,
maalampo.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS maalampo,
muu_lammitys.varast :: float(4)/ NULLIF(COALESCE(kaukolampo.varast,0) + COALESCE(kevyt_oljy.varast,0) + COALESCE(raskas_oljy.varast,0) + COALESCE(kaasu.varast,0) + COALESCE(sahko.varast,0) + COALESCE(puu.varast,0) + COALESCE(turve.varast,0) + COALESCE(hiili.varast,0) + COALESCE(maalampo.varast,0) + COALESCE(muu_lammitys.varast,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind 
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind 
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind

UNION
SELECT index.xyind, 'muut' as rakennus_tyyppi,
kaukolampo.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS kaukolampo,
kevyt_oljy.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS kevyt_oljy,
raskas_oljy.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS raskas_oljy,
kaasu.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS kaasu,
sahko.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS sahko,
puu.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS puu,
turve.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS turve,
hiili.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS hiili,
maalampo.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS maalampo,
muu_lammitys.muut :: float(4)/ NULLIF(COALESCE(kaukolampo.muut,0) + COALESCE(kevyt_oljy.muut,0) + COALESCE(raskas_oljy.muut,0) + COALESCE(kaasu.muut,0) + COALESCE(sahko.muut,0) + COALESCE(puu.muut,0) + COALESCE(turve.muut,0) + COALESCE(hiili.muut,0) + COALESCE(maalampo.muut,0) + COALESCE(muu_lammitys.muut,0),0) AS muu_lammitys
FROM index
      FULL OUTER JOIN kaukolampo ON kaukolampo.xyind = index.xyind
	  FULL OUTER JOIN kevyt_oljy ON kevyt_oljy.xyind = index.xyind
	  FULL OUTER JOIN raskas_oljy ON raskas_oljy.xyind = index.xyind
	  FULL OUTER JOIN kaasu ON kaasu.xyind = index.xyind
	  FULL OUTER JOIN sahko ON sahko.xyind = index.xyind
	  FULL OUTER JOIN puu ON puu.xyind = index.xyind
	  FULL OUTER JOIN turve ON turve.xyind = index.xyind
	  FULL OUTER JOIN hiili ON hiili.xyind = index.xyind
	  FULL OUTER JOIN maalampo ON maalampo.xyind = index.xyind
	  FULL OUTER JOIN muu_lammitys ON muu_lammitys.xyind = index.xyind
)
SELECT * FROM cte WHERE 
cte.kaukolampo IS NOT NULL OR
cte.kevyt_oljy IS NOT NULL OR
cte.raskas_oljy IS NOT NULL OR
cte.kaasu IS NOT NULL OR
cte.sahko IS NOT NULL OR
cte.puu IS NOT NULL OR
cte.turve IS NOT NULL OR
cte.hiili IS NOT NULL OR
cte.maalampo IS NOT NULL OR
cte.muu_lammitys IS NOT NULL;

/* Päivitetään paikallisen lämmitysmuotojakauman ja kansallisen lämmitysmuotojakauman erot */
/* Updating differences between local and "global" heating distributions */
UPDATE local_jakauma l SET
kaukolampo = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	 g.kaukolampo ELSE localweight * COALESCE(l.kaukolampo,0) + globalweight * g.kaukolampo END),
kevyt_oljy = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.kevyt_oljy ELSE localweight * COALESCE(l.kevyt_oljy,0) + globalweight * g.kevyt_oljy END),
raskas_oljy = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.raskas_oljy ELSE localweight * COALESCE(l.raskas_oljy,0) + globalweight * g.raskas_oljy END),
kaasu = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.kaasu ELSE localweight * COALESCE(l.kaasu,0) + globalweight * g.kaasu END),
sahko = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.sahko ELSE localweight * COALESCE(l.sahko,0) + globalweight * g.sahko END),
puu = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.puu ELSE localweight* COALESCE(l.puu,0) + globalweight * g.puu END),
turve = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	g.turve ELSE localweight * COALESCE(l.turve,0) + globalweight * g.turve END),
hiili = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	 g.hiili ELSE localweight * COALESCE(l.hiili,0) + globalweight * g.hiili END),
maalampo = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	 g.maalampo ELSE localweight * COALESCE(l.maalampo,0) + globalweight * g.maalampo END),
muu_lammitys = (CASE WHEN
	l.kaukolampo IS NULL AND l.kevyt_oljy IS NULL AND l.raskas_oljy IS NULL AND l.kaasu IS NULL AND l.sahko IS NULL AND l.puu IS NULL AND l.turve IS NULL AND l.hiili IS NULL AND l.maalampo IS NULL AND l.muu_lammitys IS NULL THEN
	 g.muu_lammitys ELSE localweight * COALESCE(l.muu_lammitys,0) + globalweight * g.muu_lammitys END)
FROM global_jakauma g
WHERE l.rakennus_tyyppi =  g.rakennus_tyyppi;

/* Rakennetaan uudet rakennukset energiamuodoittain */
/* Building new buildings, per primary energy source */
FOREACH energiamuoto IN ARRAY ARRAY['kaukolampo', 'kevyt_oljy', 'raskas_oljy', 'kaasu', 'sahko', 'puu', 'turve', 'hiili', 'maalampo', 'muu_lammitys']
LOOP

	WITH cte AS (
	WITH
		indeksi AS (
			SELECT DISTINCT ON (l.xyind) l.xyind FROM local_jakauma l
		),
		erpien_lammitysmuoto AS (
			SELECT l.xyind,
				(CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as erpien FROM local_jakauma l WHERE rakennus_tyyppi = 'erpien' ),
		rivita_lammitysmuoto AS ( SELECT l.xyind, 
			(CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as rivita FROM local_jakauma l WHERE rakennus_tyyppi = 'rivita' ),
		askert_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as askert FROM local_jakauma l WHERE rakennus_tyyppi = 'askert' ),
		liike_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as liike FROM local_jakauma l WHERE rakennus_tyyppi = 'liike' ),
		tsto_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as tsto FROM local_jakauma l WHERE rakennus_tyyppi = 'tsto' ),
		liiken_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as liiken FROM local_jakauma l WHERE rakennus_tyyppi = 'liiken' ),
		hoito_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as hoito FROM local_jakauma l WHERE rakennus_tyyppi = 'hoito' ),
		kokoon_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as kokoon FROM local_jakauma l WHERE rakennus_tyyppi = 'kokoon' ),
		opetus_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as opetus FROM local_jakauma l WHERE rakennus_tyyppi = 'opetus' ),
		teoll_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as teoll FROM local_jakauma l WHERE rakennus_tyyppi = 'teoll' ),
		varast_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as varast FROM local_jakauma l WHERE rakennus_tyyppi = 'varast' ),
		muut_lammitysmuoto AS ( SELECT l.xyind, (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) as muut FROM local_jakauma l WHERE rakennus_tyyppi = 'muut' )
	SELECT indeksi.*,
			COALESCE(erpien,(SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'erpien'))	AS erpien,
			COALESCE(rivita, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'rivita'))	AS rivita,
			COALESCE(askert, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'askert'))	AS askert,
			COALESCE(liike, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'liike'))	AS liike,
			COALESCE(tsto, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'tsto'))	AS tsto,
			COALESCE(liiken, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'liiken'))	AS liiken,
			COALESCE(hoito, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'hoito'))	AS hoito,
			COALESCE(kokoon, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'kokoon'))	AS kokoon,
			COALESCE(opetus, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'opetus'))	AS opetus,
			COALESCE(teoll, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'teoll'))	AS teoll,
			COALESCE(varast, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'varast'))	AS varast,
			COALESCE(muut, (SELECT (CASE WHEN energiamuoto = 'kaukolampo' THEN kaukolampo WHEN
					energiamuoto = 'kevyt_oljy' THEN kevyt_oljy WHEN
					energiamuoto = 'raskas_oljy' THEN raskas_oljy WHEN
					energiamuoto = 'kaasu' THEN kaasu WHEN
					energiamuoto = 'sahko' THEN sahko WHEN
					energiamuoto = 'puu' THEN puu WHEN
					energiamuoto = 'turve' THEN turve WHEN
					energiamuoto = 'hiili' THEN hiili WHEN
					energiamuoto = 'maalampo' THEN maalampo WHEN
					energiamuoto = 'muu_lammitys' THEN muu_lammitys END
				) FROM global_jakauma WHERE rakennus_tyyppi = 'muut'))	AS muut
		FROM indeksi
			LEFT JOIN erpien_lammitysmuoto ON erpien_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN rivita_lammitysmuoto ON rivita_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN askert_lammitysmuoto ON askert_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN liike_lammitysmuoto ON liike_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN tsto_lammitysmuoto ON tsto_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN liiken_lammitysmuoto ON liiken_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN hoito_lammitysmuoto ON hoito_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN kokoon_lammitysmuoto ON kokoon_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN opetus_lammitysmuoto ON opetus_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN teoll_lammitysmuoto ON teoll_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN varast_lammitysmuoto ON varast_lammitysmuoto.xyind = indeksi.xyind
			LEFT JOIN muut_lammitysmuoto ON muut_lammitysmuoto.xyind = indeksi.xyind
	)
    INSERT INTO rak (xyind, rakv, energiam, rakyht_ala, asuin_ala, erpien_ala, rivita_ala, askert_ala, liike_ala, myymal_ala, majoit_ala, asla_ala, ravint_ala, tsto_ala, liiken_ala, hoito_ala, kokoon_ala, opetus_ala, teoll_ala, varast_ala, muut_ala, teoll_lkm, varast_lkm)
	SELECT
        DISTINCT ON (ykr.xyind) ykr.xyind, -- xyind
        calculationYear, -- rakv
		energiamuoto, -- energiam
        NULL::int, -- rakyht_ala
        NULL::int, -- asuin_ala
        k_ap_ala::int * erpien, -- erpien_ala
        k_ar_ala::int * rivita, -- rivita_ala
        k_ak_ala::int * askert, -- askert_ala
        (liike_osuus * k_muu_ala)::int * liike, -- liike_ala
        NULL::int, -- myymal_ala
        NULL::int, -- majoit_ala
        NULL::int, -- asla_ala
        NULL::int, -- ravint_ala
        (tsto_osuus * k_muu_ala)::int * tsto, -- tsto_ala
        (liiken_osuus * k_muu_ala)::int * liiken, -- liiken_ala
        (hoito_osuus * k_muu_ala)::int * hoito, -- hoito_ala
        (kokoon_osuus * k_muu_ala)::int * kokoon, -- kokoon_ala
        (opetus_osuus * k_muu_ala)::int * opetus, -- opetus_ala
        (teoll_osuus * k_muu_ala)::int * teoll, -- teoll_ala
        (varast_osuus * k_muu_ala)::int * varast, -- varast_ala
        (muut_osuus * k_muu_ala)::int * muut, -- muut_ala
        NULL::smallint, --teoll_lkm
        NULL::smallint -- varast_lkm
    FROM ykr LEFT JOIN cte on ykr.xyind = cte.xyind;

RETURN NEXT;
END LOOP;


UPDATE rak SET
	asuin_ala = COALESCE(rak.erpien_ala,0) + COALESCE(rak.rivita_ala,0) + COALESCE(rak.askert_ala,0),
	rakyht_ala = COALESCE(rak.erpien_ala,0) + COALESCE(rak.rivita_ala,0) + COALESCE(rak.askert_ala,0) + 
	COALESCE(rak.liike_ala,0) + COALESCE(rak.tsto_ala,0) + COALESCE(rak.liiken_ala,0) + COALESCE(rak.hoito_ala,0) + COALESCE(rak.kokoon_ala,0) +
	COALESCE(rak.opetus_ala, 0) + COALESCE(rak.teoll_ala, 0) + COALESCE(rak.varast_ala, 0) + COALESCE(rak.muut_ala, 0),
	teoll_lkm = (CASE WHEN CEIL(rak.teoll_ala / teoll_koko) < 100 THEN 0 ELSE CEIL(rak.teoll_ala / teoll_koko) END),
	varast_lkm = (CASE WHEN CEIL(rak.varast_ala / varast_koko) < 100 THEN 0 ELSE CEIL(rak.varast_ala / varast_koko) END)
WHERE rak.rakv = calculationYear;

DELETE FROM rak where rak.xyind IS NULL AND rak.rakv IS NULL OR rak.rakyht_ala = 0;

UPDATE rak SET
	myymal_ala = rak.liike_ala * y.myymal_osuus,
	majoit_ala = rak.liike_ala * y.majoit_osuus,
	asla_ala = rak.liike_ala * y.asla_osuus,
	ravint_ala = rak.liike_ala * y.ravint_osuus
FROM ykr y WHERE y.xyind = rak.xyind AND rak.rakv = calculationYear;


/* Päivitetään vanhojen pytinkien lämmitysmuodot */
/* Updating heating characteristics of old buildings */

CREATE TEMP TABLE IF NOT EXISTS rak_temp AS
select distinct on (r.xyind, r.rakv, energiam) r.xyind, r.rakv,
UNNEST(CASE WHEN turve IS NULL AND hiili IS NULL AND muu_lammitys IS NULL AND raskas_oljy IS NULL AND kevyt_oljy IS NULL AND kaasu IS NULL THEN
	ARRAY['kaukolampo', 'sahko', 'puu', 'maalampo'] 
WHEN turve IS NULL AND hiili IS NULL AND muu_lammitys IS NULL AND raskas_oljy IS NULL and kaasu IS NULL THEN
	ARRAY['kaukolampo', 'kevyt_oljy', 'sahko', 'puu', 'maalampo'] 
WHEN turve IS NULL AND hiili IS NULL AND muu_lammitys IS NULL THEN
	ARRAY['kaukolampo', 'kevyt_oljy', 'raskas_oljy', 'kaasu', 'sahko', 'puu', 'maalampo'] 
ELSE ARRAY['kaukolampo', 'kevyt_oljy', 'raskas_oljy', 'kaasu', 'sahko', 'puu', 'turve', 'hiili', 'maalampo', 'muu_lammitys'] END)::varchar AS energiam,
NULL::int AS rakyht_ala, NULL::int AS asuin_ala, NULL::int AS erpien_ala, NULL::int AS rivita_ala, NULL::int AS askert_ala,
NULL::int AS liike_ala, NULL::int AS myymal_ala, NULL::int AS majoit_ala, NULL::int AS asla_ala, NULL::int AS ravint_ala, 
NULL::int AS tsto_ala, NULL::int AS liiken_ala, NULL::int AS hoito_ala, NULL::int AS kokoon_ala, NULL::int AS opetus_ala,
NULL::int AS teoll_ala, NULL::int AS varast_ala, NULL::int AS muut_ala, NULL::int AS teoll_lkm, NULL::int AS varast_lkm 
from rak r
LEFT JOIN 
(WITH
	kaukolampo AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='kaukolampo'),
	kevyt_oljy AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='kevyt_oljy'),
	raskas_oljy AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='raskas_oljy'),
	kaasu AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='kaasu'),
	sahko AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='sahko'),
	puu AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='puu'),
	turve AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='turve'),
	hiili AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='hiili'),
	maalampo AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='maalampo'),
	muu_lammitys AS (SELECT rak.xyind, rak.rakv FROM rak WHERE rak.energiam='muu_lammitys')
SELECT distinct on (r2.xyind, r2.rakv) r2.xyind, r2.rakv,
	kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, turve, hiili, maalampo, muu_lammitys
FROM rak r2
	LEFT JOIN kaukolampo on r2.xyind = kaukolampo.xyind AND r2.rakv = kaukolampo.rakv AND r2.xyind IN (SELECT kaukolampo.xyind FROM kaukolampo) AND r2.rakv IN (SELECT kaukolampo.rakv FROM kaukolampo)
	LEFT JOIN kevyt_oljy on r2.xyind = kevyt_oljy.xyind AND r2.rakv = kevyt_oljy.rakv AND r2.xyind IN (SELECT kevyt_oljy.xyind FROM kevyt_oljy) AND r2.rakv IN (SELECT kevyt_oljy.rakv FROM kevyt_oljy)
	LEFT JOIN raskas_oljy on r2.xyind = raskas_oljy.xyind AND r2.rakv = raskas_oljy.rakv AND r2.xyind IN (SELECT raskas_oljy.xyind FROM raskas_oljy) AND r2.rakv IN (SELECT raskas_oljy.rakv FROM raskas_oljy)
	LEFT JOIN kaasu on r2.xyind = kaasu.xyind AND r2.rakv = kaasu.rakv AND r2.xyind IN (SELECT kaasu.xyind FROM kaasu) AND r2.rakv IN (SELECT kaasu.rakv FROM kaasu)
	LEFT JOIN sahko on r2.xyind = sahko.xyind AND r2.rakv = sahko.rakv AND r2.xyind IN (SELECT sahko.xyind FROM sahko) AND r2.rakv IN (SELECT sahko.rakv FROM sahko)
	LEFT JOIN puu on r2.xyind = puu.xyind AND r2.rakv = puu.rakv AND r2.xyind IN (SELECT puu.xyind FROM puu) AND r2.rakv IN (SELECT puu.rakv FROM puu)
	LEFT JOIN turve on r2.xyind = turve.xyind AND r2.rakv = turve.rakv AND r2.xyind IN (SELECT turve.xyind FROM turve) AND r2.rakv IN (SELECT turve.rakv FROM turve)
	LEFT JOIN hiili on r2.xyind = hiili.xyind AND r2.rakv = hiili.rakv AND r2.xyind IN (SELECT hiili.xyind FROM hiili) AND r2.rakv IN (SELECT hiili.rakv FROM hiili)
	LEFT JOIN maalampo on r2.xyind = maalampo.xyind AND r2.rakv = maalampo.rakv AND r2.xyind IN (SELECT maalampo.xyind FROM maalampo) AND r2.rakv IN (SELECT maalampo.rakv FROM maalampo)
	LEFT JOIN muu_lammitys on r2.xyind = muu_lammitys.xyind AND r2.rakv = muu_lammitys.rakv AND r2.xyind IN (SELECT muu_lammitys.xyind FROM muu_lammitys) AND r2.rakv IN (SELECT muu_lammitys.rakv FROM muu_lammitys)
WHERE r2.rakv < 2019
	) sq 
ON sq.xyind = r.xyind AND sq.rakv = r.rakv where r.rakv < 2019;

UPDATE rak_temp future set rakyht_ala = past.rakyht_ala,
	asuin_ala = past.asuin_ala, erpien_ala = past.erpien_ala, rivita_ala = past.rivita_ala, askert_ala = past.askert_ala, liike_ala = past.liike_ala, myymal_ala = past.myymal_ala,
	majoit_ala = past.majoit_ala, asla_ala = past.asla_ala, ravint_ala = past.ravint_ala,  tsto_ala = past.tsto_ala, liiken_ala = past.liiken_ala, hoito_ala = past.hoito_ala, kokoon_ala = past.kokoon_ala,
	opetus_ala = past.opetus_ala, teoll_ala = past.teoll_ala, varast_ala = past.varast_ala, muut_ala = past.muut_ala, teoll_lkm = past.teoll_lkm, varast_lkm = past.varast_lkm
FROM rak past WHERE future.xyind = past.xyind AND future.rakv = past.rakv AND future.energiam = past.energiam;

CREATE TEMP TABLE IF NOT EXISTS rak_new AS 
SELECT * FROM
(WITH
muutos AS (
 SELECT sq.xyind, sq.rakv,
	ARRAY[sum(erpien[1]), sum(erpien[2]), sum(erpien[3]), sum(erpien[4]), sum(erpien[5]), sum(erpien[6]), sum(erpien[7])] as erpien,
	ARRAY[sum(rivita[1]), sum(rivita[2]), sum(rivita[3]), sum(rivita[4]), sum(rivita[5]), sum(rivita[6]), sum(rivita[7])] as rivita,
	ARRAY[sum(askert[1]), sum(askert[2]), sum(askert[3]), sum(askert[4]), sum(askert[5]), sum(askert[6]), sum(askert[7])] as askert,
	ARRAY[sum(liike[1]), sum(liike[2]), sum(liike[3]), sum(liike[4]), sum(liike[5]), sum(liike[6]), sum(liike[7])] as liike,
	ARRAY[sum(myymal[1]), sum(myymal[2]), sum(myymal[3]), sum(myymal[4]), sum(myymal[5]), sum(myymal[6]), sum(myymal[7])] as myymal,
	ARRAY[sum(majoit[1]), sum(majoit[2]), sum(majoit[3]), sum(majoit[4]), sum(majoit[5]), sum(majoit[6]), sum(majoit[7])] as majoit,
	ARRAY[sum(asla[1]), sum(asla[2]), sum(asla[3]), sum(asla[4]), sum(asla[5]), sum(asla[6]), sum(asla[7])] as asla,
	ARRAY[sum(ravint[1]), sum(ravint[2]), sum(ravint[3]), sum(ravint[4]), sum(ravint[5]), sum(ravint[6]), sum(ravint[7])] as ravint,
	ARRAY[sum(tsto[1]), sum(tsto[2]), sum(tsto[3]), sum(tsto[4]), sum(tsto[5]), sum(tsto[6]), sum(tsto[7])] as tsto,
	ARRAY[sum(liiken[1]), sum(liiken[2]), sum(liiken[3]), sum(liiken[4]), sum(liiken[5]), sum(liiken[6]), sum(liiken[7])] as liiken,
	ARRAY[sum(hoito[1]), sum(hoito[2]), sum(hoito[3]), sum(hoito[4]), sum(hoito[5]), sum(hoito[6]), sum(hoito[7])] as hoito,
	ARRAY[sum(kokoon[1]), sum(kokoon[2]), sum(kokoon[3]), sum(kokoon[4]), sum(kokoon[5]), sum(kokoon[6]), sum(kokoon[7])] as kokoon,
	ARRAY[sum(opetus[1]), sum(opetus[2]), sum(opetus[3]), sum(opetus[4]), sum(opetus[5]), sum(opetus[6]), sum(opetus[7])] as opetus,
	ARRAY[sum(teoll[1]), sum(teoll[2]), sum(teoll[3]), sum(teoll[4]), sum(teoll[5]), sum(teoll[6]), sum(teoll[7])] as teoll,
	ARRAY[sum(varast[1]), sum(varast[2]), sum(varast[3]), sum(varast[4]), sum(varast[5]), sum(varast[6]), sum(varast[7])] as varast,
	ARRAY[sum(muut[1]), sum(muut[2]), sum(muut[3]), sum(muut[4]), sum(muut[5]), sum(muut[6]), sum(muut[7])] as muut
 FROM (SELECT t.xyind, t.rakv, t.energiam,
 
 		(CASE WHEN t.erpien_ala IS NOT NULL AND NOT(t.erpien_ala <= 0) THEN ARRAY(SELECT t.erpien_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'erpien' AND lammitysmuoto = t.energiam)
        )) END) as erpien,
 		(CASE WHEN t.rivita_ala IS NOT NULL AND NOT(t.rivita_ala <= 0) THEN ARRAY(SELECT t.rivita_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'rivita' AND lammitysmuoto = t.energiam)
        )) END) as rivita,
 		(CASE WHEN t.askert_ala IS NOT NULL AND NOT(t.askert_ala <= 0) THEN ARRAY(SELECT t.askert_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'askert' AND lammitysmuoto = t.energiam)
        )) END) as askert, 
 		(CASE WHEN t.liike_ala IS NOT NULL AND NOT(t.liike_ala <= 0) THEN ARRAY(SELECT t.liike_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liike' AND lammitysmuoto = t.energiam)
        )) END) as liike,
 		(CASE WHEN t.myymal_ala IS NOT NULL AND NOT(t.myymal_ala <= 0) THEN ARRAY(SELECT t.myymal_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liike' AND lammitysmuoto = t.energiam)
        )) END) as myymal,
 		(CASE WHEN t.majoit_ala IS NOT NULL AND NOT(t.majoit_ala <= 0) THEN ARRAY(SELECT t.majoit_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liike' AND lammitysmuoto = t.energiam)
        )) END) as majoit,
 		(CASE WHEN t.asla_ala IS NOT NULL AND NOT(t.asla_ala <= 0) THEN ARRAY(SELECT t.asla_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liike' AND lammitysmuoto = t.energiam)
        )) END) as asla,
 		(CASE WHEN t.ravint_ala IS NOT NULL AND NOT(t.ravint_ala <= 0) THEN ARRAY(SELECT t.ravint_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liike' AND lammitysmuoto = t.energiam)
        )) END) as ravint,
 		(CASE WHEN t.tsto_ala IS NOT NULL AND NOT(t.tsto_ala <= 0) THEN ARRAY(SELECT t.tsto_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'tsto' AND lammitysmuoto = t.energiam)
        )) END) as tsto, 
 		(CASE WHEN t.liiken_ala IS NOT NULL AND NOT(t.liiken_ala <= 0) THEN ARRAY(SELECT t.liiken_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'liiken' AND lammitysmuoto = t.energiam)
        )) END) as liiken,
 		(CASE WHEN t.hoito_ala IS NOT NULL AND NOT(t.hoito_ala <= 0) THEN ARRAY(SELECT t.hoito_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'hoito' AND lammitysmuoto = t.energiam)
        )) END) as hoito,
 		(CASE WHEN t.kokoon_ala IS NOT NULL AND NOT(t.kokoon_ala <= 0) THEN ARRAY(SELECT t.kokoon_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'kokoon' AND lammitysmuoto = t.energiam)
        )) END) as kokoon,
 		(CASE WHEN t.opetus_ala IS NOT NULL AND NOT(t.opetus_ala <= 0) THEN ARRAY(SELECT t.opetus_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'opetus' AND lammitysmuoto = t.energiam)
        )) END) as opetus,
		(CASE WHEN t.teoll_ala IS NOT NULL AND NOT(t.teoll_ala <= 0) THEN ARRAY(SELECT t.teoll_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'teoll' AND lammitysmuoto = t.energiam)
		)) END) as teoll,
		(CASE WHEN t.varast_ala IS NOT NULL AND NOT(t.varast_ala <= 0) THEN ARRAY(SELECT t.varast_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'varast' AND lammitysmuoto = t.energiam)
		)) END) as varast,
		(CASE WHEN t.muut_ala IS NOT NULL AND NOT(t.muut_ala <= 0) THEN ARRAY(SELECT t.muut_ala * 
			UNNEST((SELECT ARRAY[kaukolampo, kevyt_oljy, raskas_oljy, kaasu, sahko, puu, maalampo] FROM rakymp."tilat_lmuoto_muutos" WHERE skenaario = kehitysskenaario AND rakennus_tyyppi = 'muut' AND lammitysmuoto = t.energiam)
		)) END) as muut
 	FROM rak_temp t WHERE t.rakv != 0
	) sq
GROUP BY sq.rakv, sq.xyind)

SELECT rak_temp.xyind, rak_temp.rakv, rak_temp.energiam, -- Seuraaviin voisi rakentaa kytkimen, jolla alle nollan menevät NULLAtaan, mutta nyt jätetty pois koska moiset pudotetaan laskennassa pois joka tapauksessa
	NULL::int as rakyht_ala, 
	NULL::int as asuin_ala,
	NULLIF(COALESCE(rak_temp.erpien_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN erpien[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN erpien[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN erpien[3]
		WHEN rak_temp.energiam = 'kaasu' THEN erpien[4]
		WHEN rak_temp.energiam = 'sahko' THEN erpien[5]
		WHEN rak_temp.energiam = 'puu' THEN erpien[6]
		WHEN rak_temp.energiam = 'maalampo' THEN erpien[7]
		ELSE 0 END),0)::int
	as erpien_ala,
	NULLIF(COALESCE(rak_temp.rivita_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN rivita[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN rivita[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN rivita[3]
		WHEN rak_temp.energiam = 'kaasu' THEN rivita[4]
		WHEN rak_temp.energiam = 'sahko' THEN rivita[5]
		WHEN rak_temp.energiam = 'puu' THEN rivita[6]
		WHEN rak_temp.energiam = 'maalampo' THEN rivita[7]
		ELSE 0 END),0)::int
	as rivita_ala,
	NULLIF(COALESCE(rak_temp.askert_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN askert[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN askert[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN askert[3]
		WHEN rak_temp.energiam = 'kaasu' THEN askert[4]
		WHEN rak_temp.energiam = 'sahko' THEN askert[5]
		WHEN rak_temp.energiam = 'puu' THEN askert[6]
		WHEN rak_temp.energiam = 'maalampo' THEN askert[7]
		ELSE 0 END),0)::int
	as askert_ala,
	NULLIF(COALESCE(rak_temp.liike_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN liike[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN liike[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN liike[3]
		WHEN rak_temp.energiam = 'kaasu' THEN liike[4]
		WHEN rak_temp.energiam = 'sahko' THEN liike[5]
		WHEN rak_temp.energiam = 'puu' THEN liike[6]
		WHEN rak_temp.energiam = 'maalampo' THEN liike[7]
		ELSE 0 END),0)::int
	as liike_ala,
	NULLIF(COALESCE(rak_temp.myymal_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN myymal[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN myymal[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN myymal[3]
		WHEN rak_temp.energiam = 'kaasu' THEN myymal[4]
		WHEN rak_temp.energiam = 'sahko' THEN myymal[5]
		WHEN rak_temp.energiam = 'puu' THEN myymal[6]
		WHEN rak_temp.energiam = 'maalampo' THEN myymal[7]
		ELSE 0 END),0)::int
	as myymal_ala,
	NULLIF(COALESCE(rak_temp.majoit_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN majoit[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN majoit[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN majoit[3]
		WHEN rak_temp.energiam = 'kaasu' THEN majoit[4]
		WHEN rak_temp.energiam = 'sahko' THEN majoit[5]
		WHEN rak_temp.energiam = 'puu' THEN majoit[6]
		WHEN rak_temp.energiam = 'maalampo' THEN majoit[7]
		ELSE 0 END),0)::int
	as majoit_ala,
	NULLIF(COALESCE(rak_temp.asla_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN asla[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN asla[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN asla[3]
		WHEN rak_temp.energiam = 'kaasu' THEN asla[4]
		WHEN rak_temp.energiam = 'sahko' THEN asla[5]
		WHEN rak_temp.energiam = 'puu' THEN asla[6]
		WHEN rak_temp.energiam = 'maalampo' THEN asla[7]
		ELSE 0 END),0)::int
	as asla_ala,
	NULLIF(COALESCE(rak_temp.ravint_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN ravint[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN ravint[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN ravint[3]
		WHEN rak_temp.energiam = 'kaasu' THEN ravint[4]
		WHEN rak_temp.energiam = 'sahko' THEN ravint[5]
		WHEN rak_temp.energiam = 'puu' THEN ravint[6]
		WHEN rak_temp.energiam = 'maalampo' THEN ravint[7]
		ELSE 0 END),0)::int
	as ravint_ala,
	NULLIF(COALESCE(rak_temp.tsto_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN tsto[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN tsto[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN tsto[3]
		WHEN rak_temp.energiam = 'kaasu' THEN tsto[4]
		WHEN rak_temp.energiam = 'sahko' THEN tsto[5]
		WHEN rak_temp.energiam = 'puu' THEN tsto[6]
		WHEN rak_temp.energiam = 'maalampo' THEN tsto[7]
		ELSE 0 END),0)::int
	as tsto_ala,
	NULLIF(COALESCE(rak_temp.liiken_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN liiken[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN liiken[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN liiken[3]
		WHEN rak_temp.energiam = 'kaasu' THEN liiken[4]
		WHEN rak_temp.energiam = 'sahko' THEN liiken[5]
		WHEN rak_temp.energiam = 'puu' THEN liiken[6]
		WHEN rak_temp.energiam = 'maalampo' THEN liiken[7]
		ELSE 0 END),0)::int
	as liiken_ala,
	NULLIF(COALESCE(rak_temp.hoito_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN hoito[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN hoito[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN hoito[3]
		WHEN rak_temp.energiam = 'kaasu' THEN hoito[4]
		WHEN rak_temp.energiam = 'sahko' THEN hoito[5]
		WHEN rak_temp.energiam = 'puu' THEN hoito[6]
		WHEN rak_temp.energiam = 'maalampo' THEN hoito[7]
		ELSE 0 END),0)::int
	as hoito_ala,
	NULLIF(COALESCE(rak_temp.kokoon_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN kokoon[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN kokoon[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN kokoon[3]
		WHEN rak_temp.energiam = 'kaasu' THEN kokoon[4]
		WHEN rak_temp.energiam = 'sahko' THEN kokoon[5]
		WHEN rak_temp.energiam = 'puu' THEN kokoon[6]
		WHEN rak_temp.energiam = 'maalampo' THEN kokoon[7]
		ELSE 0 END),0)::int
	as kokoon_ala,
	NULLIF(COALESCE(rak_temp.opetus_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN opetus[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN opetus[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN opetus[3]
		WHEN rak_temp.energiam = 'kaasu' THEN opetus[4]
		WHEN rak_temp.energiam = 'sahko' THEN opetus[5]
		WHEN rak_temp.energiam = 'puu' THEN opetus[6]
		WHEN rak_temp.energiam = 'maalampo' THEN opetus[7]
		ELSE 0 END),0)::int
	as opetus_ala,
	NULLIF(COALESCE(rak_temp.teoll_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN teoll[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN teoll[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN teoll[3]
		WHEN rak_temp.energiam = 'kaasu' THEN teoll[4]
		WHEN rak_temp.energiam = 'sahko' THEN teoll[5]
		WHEN rak_temp.energiam = 'puu' THEN teoll[6]
		WHEN rak_temp.energiam = 'maalampo' THEN teoll[7]
		ELSE 0 END),0)::int
	as teoll_ala,
	NULLIF(COALESCE(rak_temp.varast_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN varast[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN varast[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN varast[3]
		WHEN rak_temp.energiam = 'kaasu' THEN varast[4]
		WHEN rak_temp.energiam = 'sahko' THEN varast[5]
		WHEN rak_temp.energiam = 'puu' THEN varast[6]
		WHEN rak_temp.energiam = 'maalampo' THEN varast[7]
		ELSE 0 END),0)::int
	as varast_ala,
	NULLIF(COALESCE(rak_temp.muut_ala, 0) + (CASE
		WHEN rak_temp.energiam = 'kaukolampo' THEN muut[1]
		WHEN rak_temp.energiam = 'kevyt_oljy' THEN muut[2]
		WHEN rak_temp.energiam = 'raskas_oljy' THEN muut[3]
		WHEN rak_temp.energiam = 'kaasu' THEN muut[4]
		WHEN rak_temp.energiam = 'sahko' THEN muut[5]
		WHEN rak_temp.energiam = 'puu' THEN muut[6]
		WHEN rak_temp.energiam = 'maalampo' THEN muut[7]
		ELSE 0 END),0)::int
	as muut_ala,
	rak_temp.teoll_lkm::smallint,
	rak_temp.varast_lkm::smallint
FROM rak_temp
LEFT JOIN muutos ON rak_temp.xyind = muutos.xyind AND rak_temp.rakv = muutos.rakv) query
WHERE NOT (query.erpien_ala IS NULL AND query.rivita_ala IS NULL and query.askert_ala IS NULL and query.liike_ala IS NULL and query.tsto_ala IS NULL and query.hoito_ala IS NULL and query.liiken_ala IS NULL AND
	query.kokoon_ala IS NULL and query.opetus_ala IS NULL and query.teoll_ala IS NULL AND query.varast_ala IS NULL AND query.muut_ala IS NULL AND query.teoll_lkm IS NULL AND query.varast_lkm IS NULL);

UPDATE rak_new SET asuin_ala = COALESCE(rak_new.erpien_ala,0) + COALESCE(rak_new.rivita_ala,0) + COALESCE(rak_new.askert_ala,0),
rakyht_ala = COALESCE(rak_new.erpien_ala,0) + COALESCE(rak_new.rivita_ala,0) + COALESCE(rak_new.askert_ala,0) + COALESCE(rak_new.liike_ala,0) + COALESCE(rak_new.tsto_ala,0) + COALESCE(rak_new.liiken_ala,0) +
COALESCE(rak_new.hoito_ala,0) + COALESCE(rak_new.kokoon_ala,0) + COALESCE(rak_new.opetus_ala,0) + COALESCE(rak_new.teoll_ala,0) + COALESCE(rak_new.varast_ala,0) + COALESCE(rak_new.muut_ala,0);

RETURN QUERY SELECT * FROM rak_new UNION SELECT * FROM rak WHERE rak.rakv >= 2019;
DROP TABLE IF EXISTS ykr, rak_new, rak_temp, local_jakauma, global_jakauma, kayttotapajakauma;
/*
EXCEPTION WHEN OTHERS THEN
	DROP TABLE IF EXISTS ykr, rak, rak_new, rak_temp, local_jakauma, global_jakauma, kayttotapajakauma;
*/
END;
$$ LANGUAGE plpgsql;
