CREATE OR REPLACE FUNCTION
public.CO2_UpdateBuildings(
    rak_taulu text,
    ykr_taulu text,
    calculationYears integer[] -- [year based on which emission values are calculated, min, max calculation years]
)
RETURNS TABLE (
    xyind varchar,
    rakv int,
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
    muut_ala integer
) AS $$
DECLARE
    calculationYear integer; 
    teoll_koko numeric;
    varast_koko numeric;
BEGIN

    calculationYear := CASE WHEN calculationYears[1] < calculationYears[2] THEN calculationYears[2]
    WHEN calculationYears[1] > calculationYears[3] THEN calculationYears[3]
    ELSE calculationYears[1]
    END;

EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS rak AS SELECT xyind, rakv::int, rakyht_ala::int, asuin_ala::int, erpien_ala::int, rivita_ala::int, askert_ala::int, liike_ala::int, myymal_ala::int, majoit_ala::int, asla_ala::int, ravint_ala::int, tsto_ala::int, liiken_ala::int, hoito_ala::int, kokoon_ala::int, opetus_ala::int, teoll_ala::int, varast_ala::int, muut_ala::int FROM ' || quote_ident(rak_taulu) ||' WHERE rakv::int != 0';
EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr AS SELECT xyind, k_ap_ala, k_ar_ala, k_ak_ala, k_muu_ala, k_poistuma FROM ' || quote_ident(ykr_taulu) || ' WHERE (k_ap_ala IS NOT NULL AND k_ap_ala != 0) OR (k_ar_ala IS NOT NULL AND k_ar_ala != 0) OR  (k_ak_ala IS NOT NULL AND k_ak_ala != 0) OR (k_muu_ala IS NOT NULL AND k_muu_ala != 0) OR (k_poistuma IS NOT NULL AND k_poistuma != 0)';

/* Lisätään puuttuvat sarakkeet väliaikaiseen YKR-dataan */
/* Adding new columns into the temporary YKR data */
ALTER TABLE ykr
    ADD COLUMN liike_osuus numeric,
    ADD COLUMN myymal_osuus numeric,
    ADD COLUMN majoit_osuus numeric,
    ADD COLUMN asla_osuus numeric,
    ADD COLUMN ravint_osuus numeric,
    ADD COLUMN tsto_osuus numeric,
    ADD COLUMN liiken_osuus numeric,
    ADD COLUMN hoito_osuus numeric,
    ADD COLUMN kokoon_osuus numeric,
    ADD COLUMN opetus_osuus numeric,
    ADD COLUMN teoll_osuus numeric,
    ADD COLUMN varast_osuus numeric,
    ADD COLUMN muut_osuus numeric,
    ADD COLUMN muu_ala numeric;
    
/* Lasketaan eri käyttömuotojen osuudet väliaikaiseen YKR-dataan */
/* Calculating the distribution of building uses into the temporary YKR data */
UPDATE ykr SET 
    muu_ala = COALESCE(sq.muu_ala, 0),
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


/* Asetetaan myös vakiojakaumat uusia alueita varten */
/* Käyttöalaperusteinen käyttötapajakauma generoitu Tampereen alueen YKR-datasta */
/* Set default proportions of building usage for new areas as well */
UPDATE ykr SET
    liike_osuus = 0.1771,
    myymal_osuus = 0.1245,
    majoit_osuus = 0.0235,
    asla_osuus = 0.0265,
    ravint_osuus = 0.0025,
    tsto_osuus = 0.167,
    liiken_osuus = 0.072,
    hoito_osuus = 0.0577,
    kokoon_osuus = 0.0596,
    opetus_osuus = 0.1391,
    teoll_osuus = 0.2392,
    varast_osuus = 0.0823,
    muut_osuus = 0.006
WHERE 
    (liike_osuus IS NULL OR liike_osuus = 0) AND
    (myymal_osuus IS NULL OR myymal_osuus = 0) AND
    (majoit_osuus IS NULL OR majoit_osuus = 0) AND
    (asla_osuus IS NULL OR asla_osuus = 0) AND
    (ravint_osuus IS NULL OR ravint_osuus = 0) AND
    (tsto_osuus IS NULL OR tsto_osuus = 0) AND
    (liiken_osuus IS NULL OR liiken_osuus = 0) AND
    (hoito_osuus IS NULL OR hoito_osuus = 0) AND
    (kokoon_osuus IS NULL OR kokoon_osuus = 0) AND
    (opetus_osuus IS NULL OR opetus_osuus = 0) AND
    (teoll_osuus IS NULL OR teoll_osuus = 0) AND
    (varast_osuus IS NULL OR varast_osuus = 0) AND
    (muut_osuus IS NULL OR muut_osuus = 0);

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
    muut_ala = (CASE WHEN muut > b.muut_ala THEN 0 ELSE b.muut_ala - muut END)
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


/* Rakennetaan uusia rakennuksia */
/* Building new buildings */
INSERT INTO rak(xyind, rakv, rakyht_ala, asuin_ala, erpien_ala, rivita_ala, askert_ala, liike_ala, myymal_ala, majoit_ala, asla_ala, ravint_ala, tsto_ala, liiken_ala, hoito_ala, kokoon_ala, opetus_ala, teoll_ala, varast_ala, muut_ala   )
SELECT
    DISTINCT ON (ykr.xyind) ykr.xyind, -- xyind
    calculationYear, -- rakv
    (k_ap_ala + k_ar_ala + k_ak_ala + k_muu_ala)::int, -- rakyht_ala
    (k_ap_ala + k_ar_ala + k_ak_ala)::int, -- asuin_ala
    k_ap_ala::int, --erpien_ala
    k_ar_ala::int, -- rivita_ala
    k_ak_ala::int, -- askert_ala
    (liike_osuus * k_muu_ala)::int, -- liike_ala
    (myymal_osuus * k_muu_ala)::int, -- myymal_ala
    (majoit_osuus * k_muu_ala)::int, -- majoit_ala
    (asla_osuus * k_muu_ala)::int, -- asla_ala
    (ravint_osuus * k_muu_ala)::int, -- ravint_ala
    (tsto_osuus * k_muu_ala)::int, -- tsto_ala
    (liiken_osuus * k_muu_ala)::int, -- liiken_ala
    (hoito_osuus * k_muu_ala)::int, -- hoito_ala
    (kokoon_osuus * k_muu_ala)::int, -- kokoon_ala
    (opetus_osuus * k_muu_ala)::int, -- opetus_ala
    (teoll_osuus * k_muu_ala)::int, -- teoll_ala
    (varast_osuus * k_muu_ala)::int, -- varast_ala
    (muut_osuus * k_muu_ala)::int -- muut_ala
    FROM ykr;
ALTER TABLE ykr DROP COLUMN IF EXISTS muu_ala;
RETURN QUERY SELECT * FROM rak;
DROP TABLE IF EXISTS ykr, rak;

END;
$$ LANGUAGE plpgsql;