DROP FUNCTION IF EXISTS public.il_numerize_new;
CREATE OR REPLACE FUNCTION
public.il_numerize_new(
    ykr_taulu regclass,
    baseYear integer,
    targetYear integer,
    calculationYear integer,
    area varchar,
    kt_taulu regclass,
    kv_taulu regclass default null,
    jl_taulu regclass default null
)
RETURNS TABLE (
    geom geometry,
    xyind varchar,
    vyoh integer,
    centdist integer,
    v_yht integer,
    tp_yht integer,
    k_ap_ala real,
    k_ar_ala real,
    k_ak_ala real,
    k_muu_ala real,
    k_tp_yht integer,
    k_poistuma real,
    maa_ha real
    --alueteho real,
   -- alueteho_muutos real
) AS $$
DECLARE
    km2hm2 real;
    poistuma_exists boolean;
    aloitusv_exists boolean;
    valmisv_exists boolean;
    kt_gt text;
    kv_gt text;
    jl_gt text;
    
BEGIN

SELECT aoi.km2hm2 FROM aluejaot.alueet aoi WHERE kunta = area OR maakunta = area INTO km2hm2;

CREATE TEMP TABLE uz AS SELECT * FROM aluejaot."ykr_vyohykkeet";
    CREATE INDEX ON uz USING GIST (geom);

EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS ykr AS SELECT * FROM %s', ykr_taulu);
    CREATE INDEX ON ykr USING GIST (geom);

EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kt AS SELECT * FROM %s', kt_taulu);
    SELECT geometrytype(kt.geom) from kt into kt_gt;
    EXECUTE format('ALTER TABLE kt ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', kt_gt);
    CREATE INDEX ON kt USING GIST (geom);

IF kv_taulu IS NOT NULL THEN
    EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kv AS SELECT * FROM %s', kv_taulu);
    SELECT geometrytype(kv.geom) from kv into kv_gt;
    EXECUTE format('ALTER TABLE kv ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', kv_gt);
    CREATE INDEX ON kv USING GIST (geom);
END IF;

IF jl_taulu IS NOT NULL THEN
    EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS jl AS SELECT * FROM %s', jl_taulu);
    SELECT geometrytype(jl.geom) from jl into jl_gt;
    EXECUTE format('ALTER TABLE jl ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', jl_gt);
    CREATE INDEX ON jl USING GIST (geom);
END IF;

EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_taulu, 'k_poistuma') INTO poistuma_exists;
EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_taulu, 'k_aloitusv') INTO poistuma_exists;
EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_taulu, 'k_valmisv') INTO poistuma_exists;

ALTER TABLE ykr
    ADD COLUMN IF NOT EXISTS k_ap_ala real default 0,
    ADD COLUMN IF NOT EXISTS k_ar_ala real default 0,
    ADD COLUMN IF NOT EXISTS k_ak_ala real default 0,
    ADD COLUMN IF NOT EXISTS k_muu_ala real default 0,
    ADD COLUMN IF NOT EXISTS k_tp_yht integer default 0,
    ADD COLUMN IF NOT EXISTS k_poistuma real default 0,
    ADD COLUMN IF NOT EXISTS maa_ha real default 0,
    ADD COLUMN IF NOT EXISTS alueteho real default 0,
    ADD COLUMN IF NOT EXISTS alueteho_muutos real default 0;

/* Lasketaan käyttötarkoitusalueille pinta-alat hehtaareina */
ALTER TABLE kt ADD COLUMN IF NOT EXISTS area_ha real default 0;
UPDATE kt SET area_ha = ST_AREA(kt.geom)/10000;

/* Lasketaan käyttötarkoitusalueilta numeeriset arvot YKR-ruuduille. Tällä hetkellä tämä tehdään lineaarisesti. Seuraavissa kehitysversioissa tarkastellaan arvojen painotettua jakamista. */
IF aloitusv_exists AND valmisv_exists THEN
    UPDATE ykr
    SET k_ap_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
    ), k_ar_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
    ), k_ak_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
    ), k_muu_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
    ), k_tp_yht = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * kt.k_tp_yht / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
    );
        IF poistuma_exists THEN
            UPDATE ykr SET k_poistuma = (
                SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) *
                (CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) * (-1) ELSE kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
                FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
            );
        ELSE
            /* DUMMY for default demolition rate */
            UPDATE ykr SET k_poistuma = 999999;
        END IF;
ELSE 
    UPDATE ykr
    SET k_ap_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (targetYear - baseYear + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
    ), k_ar_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (targetYear - baseYear + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
    ), k_ak_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (targetYear - baseYear + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
    ), k_muu_ala = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (targetYear - baseYear + 1) END))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
    ), k_tp_yht = (
        SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (kt.k_tp_yht / (targetYear - baseYear + 1)))
        FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
    );
        IF poistuma_exists THEN
            UPDATE ykr SET k_poistuma = (
                SELECT SUM(ST_Area(ST_Intersection(ykr.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (targetYear - baseYear + 1) * (-1) ELSE kt.k_poistuma / (targetYear - baseYear + 1) END))
                FROM kt WHERE ST_Intersects(ykr.geom, kt.geom) AND baseYear <= calculationYear
            );
        ELSE
            UPDATE ykr SET k_poistuma = 999999;
        END IF;
END IF;


/*  Haetaan ruudukolle nykyisen maanpeitteen mukaiset maapinta-alatiedot.
    Päivitetään mahdolliset ranta- ja vesialueiden täytöt maa_ha -sarakkeeseen.
    Maa_ha -arvoksi täytöille asetetaan 5.9, joka on rakennettujen ruutujen keskimääräinen maa-ala.
    Tässä oletetaan, että jos alle 20% ruudusta (1.25 ha) on nykyisin maata, ja alueelle rakennetaan vuodessa yli 200 neliötä kerrostaloja,
    tehdään täyttöjä (laskettu 20%:lla keskimääräisestä n. 10000 m2 rakennusten pohja-alasta per ruutu, jaettuna 10 v. toteutusajalle).
    Lasketaan samalla aluetehokkuuden muutos ja päivitetään aluetehokkuus. */

UPDATE ykr SET maa_ha = CASE WHEN clc.maa_ha < 1.25 AND ykr.k_ak_ala >= 200 THEN 5.9 ELSE clc.maa_ha END
FROM aluejaot."YKR_CLC2018" clc WHERE clc.xyind = ykr.xyind;

UPDATE ykr SET alueteho_muutos = (CASE WHEN ykr.maa_ha != 0 THEN (COALESCE(ykr.k_ap_ala,0) + COALESCE(ykr.k_ar_ala,0) + COALESCE(ykr.k_ak_ala,0) + COALESCE(ykr.k_muu_ala,0)) / (10000 * ykr.maa_ha) ELSE 0 END);
UPDATE ykr SET alueteho = (CASE WHEN COALESCE(ykr.alueteho,0) + COALESCE(ykr.alueteho_muutos,0) > 0 THEN COALESCE(ykr.alueteho,0) + COALESCE(ykr.alueteho_muutos,0) ELSE 0 END);

/* Lasketaan väestön lisäys asumisväljyyden avulla. 1.25 = kerroin kerrosalasta huoneistoalaksi. */
UPDATE ykr SET
    v_yht = COALESCE(ykr.v_yht, 0) +
        (
            COALESCE(ykr.k_ap_ala, 0)::real / COALESCE(av.erpien,38)::real +
            COALESCE(ykr.k_ar_ala, 0)::real / COALESCE(av.rivita,35.5)::real +
            COALESCE(ykr.k_ak_ala, 0)::real / COALESCE(av.askert,35)::real
        ) / COALESCE(km2hm2, 1.25)::real,
    tp_yht = COALESCE(ykr.tp_yht,0) + COALESCE(ykr.k_tp_yht,0)
    FROM rakymp."asumisvaljyys" av WHERE av.vuosi = calculationYear AND av.alue = area;

/*  KESKUSVERKON PÄIVITTÄMINEN
    Luodaan väliaikainen taso valtakunnallisesta keskusta-alueaineistosta
    Poistetaan ylimääräiset / virheelliset keskustat
    Muutetaan valtakunnallinen keskusta-alueaineisto keskipisteiksi (Point). */

IF calculationYear = baseYear THEN

    CREATE TEMP TABLE IF NOT EXISTS keskusta_alueet_centroid AS
    SELECT ka.id, st_transform(st_centroid(ka.geom),3067) geom, ka.keskustyyp, ka.keskusnimi
    FROM aluejaot."KeskustaAlueet" ka WHERE keskustyyp != 'Kaupunkiseudun pieni alakeskus';

    /* Päivitetään geometriat kaupunkiseutujen keskustojen osalta (ydinkeskusten keskipisteet saadaan tarkimmin Urban Zone-vyöhykeaineiston kävelykeskustojen keskipisteistä). */
    UPDATE keskusta_alueet_centroid
    SET geom = ST_CENTROID(uz.geom)
        FROM uz
            WHERE ST_INTERSECTS(keskusta_alueet_centroid.geom, uz.geom)
            AND uz.vyoh = 1;
    CREATE INDEX ON keskusta_alueet_centroid USING GIST (geom);

    /* Rajataan valtakunnallinen keskusta-aineisto kattamaan vain tutkimusruuduille lähimmät kohteet. */
    CREATE TEMP TABLE IF NOT EXISTS keskusverkko AS
    SELECT DISTINCT ON (p2.geom) p2.* FROM
    (SELECT p1.xyind as g1,
        (SELECT p.id
            FROM keskusta_alueet_centroid AS p
            WHERE p1.xyind <> p.id::varchar
            ORDER BY p.geom <#> p1.geom ASC LIMIT 1
        ) AS g2
            FROM ykr AS p1
            OFFSET 0
    ) AS q
    JOIN ykr AS p1
    ON q.g1=p1.xyind
    JOIN keskusta_alueet_centroid AS p2
    ON q.g2=p2.id;

    DROP TABLE IF EXISTS keskusta_alueet_centroid;

END IF;

/* Lisätään uusia keskuksia keskusverkkoon vain mikäli käyttäjä on tällaisia syöttänyt! */
IF kv_taulu IS NOT NULL THEN
    INSERT INTO keskusverkko
    SELECT (SELECT MAX(k.id) FROM keskusverkko k) + row_number() over (order by suunnitelma.geom desc),
        st_force2d((ST_DUMP(suunnitelma.geom)).geom) as geom, k_ktyyp AS keskustyyp, k_knimi AS keskusnimi
    FROM kv suunnitelma
    WHERE NOT EXISTS (
        SELECT 1
        FROM keskusverkko keskustat
        WHERE ST_DWithin(suunnitelma.geom, keskustat.geom, 1500)
    ) AND suunnitelma.k_ktyyp = 'Kaupunkiseudun iso alakeskus'
    AND (COALESCE(suunnitelma.k_kalkuv, baseYear) + (COALESCE(suunnitelma.k_kvalmv,targetYear) - COALESCE(suunnitelma.k_kalkuv,baseYear))/2 >= calculationYear);
END IF;

CREATE INDEX ON keskusverkko USING GIST (geom);

/* Päivitetään YKR-perusruutuihin etäisyys (centdist, km, real) lähimpään keskukseen. */
UPDATE ykr
SET centdist = sq2.centdist FROM
    (SELECT ykr.xyind, keskusta.centdist
    FROM ykr
    CROSS JOIN LATERAL
        (SELECT ST_Distance(ST_CENTROID(keskustat.geom), ykr.geom)/1000 AS centdist
            FROM keskusverkko keskustat
        WHERE keskustat.keskustyyp != 'Kaupunkiseudun pieni alakeskus'
        ORDER BY ykr.geom <#> keskustat.geom
    LIMIT 1) AS keskusta) as sq2
WHERE ykr.xyind = sq2.xyind;

/* YHDYSKUNTARAKENTEEN VYÖHYKKEIDEN PÄIVITTÄMINEN */
CREATE TEMP TABLE IF NOT EXISTS uz_new AS
SELECT * FROM
    (SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 1 AS vyoh -- 'Keskustan jalankulkuvyöhyke'
    FROM ykr, uz
    /* Search for grid cells within current UZ central areas delineation */
    WHERE ykr.maa_ha != 0 AND
        (st_within(st_centroid(ykr.geom), uz.geom) AND uz.vyoh = 1)
        /* and those cells that touch the current centers - have to use d_within for fastest approximation, st_touches doesn't work due to false DE-9IM relations */
        OR (st_dwithin(YKR.geom, uz.geom,25)
        /* Main centers must be within 1.5 km from core */
        /* NYT vain Tampere - jatkossa pitäisi luetella tähän kaikki kaupunkiseutukeskustat SYKE:n oletusetäisyysarvoineen */
        AND (
            (uz.vyoh = 1
                AND st_dwithin(YKR.geom,
                    (SELECT keskusverkko.geom FROM keskusverkko WHERE keskusverkko.keskusnimi = 'Tampere'),
                    1500)
            )
        ))
        AND (ykr.alueteho > 0.05 AND ykr.tp_yht > 0)
        AND (ykr.alueteho > 0.2 AND ykr.v_yht >= 100 AND YKR.tp_yht > 0)
        /* Select only edge neighbours, no corner touchers */
        /* we have to use a buffer + area based intersection trick due to topological errors */
        AND st_area(st_intersection(ykr.geom,st_buffer(uz.geom,1))) > 1
        AND (0.014028 * ykr.v_yht + 0.821276 * ykr.tp_yht -3.67) > 10) uz1
        
    UNION
    
    /* Olemassaolevien alakeskusten reunojen kasvatus */
    SELECT * FROM
    (SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 10 AS vyoh
    FROM ykr, uz, keskusverkko /* keskus */
    /* Search for grid cells within current UZ central areas delineation */
    WHERE ykr.maa_ha != 0 AND (st_within(st_centroid(ykr.geom), uz.geom) AND uz.vyoh IN (10,11,12,6))
        OR (st_dwithin(YKR.geom, uz.geom, 25) AND uz.vyoh IN (10,11,12,6))
        AND (ykr.alueteho > 0.05 AND ykr.tp_yht > 0)
        AND (ykr.alueteho > 0.2 AND ykr.v_yht >= 100 AND YKR.tp_yht > 0)
        /* Select only edge neighbours, no corner touchers */
        /* we have to use a buffer + area based intersection trick due to topological errors */
        AND st_area(st_intersection(ykr.geom,st_buffer(uz.geom,1))) > 1
        AND (0.014028 * ykr.v_yht + 0.821276 * ykr.tp_yht -3.67) > 10) uz10
        
    UNION
    
    SELECT * FROM
        (SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 10 AS vyoh
        FROM ykr, keskusverkko
        WHERE ykr.maa_ha != 0
        AND (st_dwithin(YKR.geom, keskusverkko.geom, 250) AND keskusverkko.keskustyyp = 'Kaupunkiseudun iso alakeskus')
        OR (st_dwithin(YKR.geom, keskusverkko.geom, 500) AND keskusverkko.keskustyyp = 'Kaupunkiseudun iso alakeskus'
            AND (ykr.alueteho > 0.05 AND ykr.tp_yht > 0)
        AND (ykr.alueteho > 0.2 AND ykr.v_yht >= 100 AND YKR.tp_yht > 0))
        ) uz10new;

    CREATE INDEX ON uz_new USING GIST (geom);

    /* Erityistapaukset */
    UPDATE uz_new SET vyoh = 6 WHERE uz_new.vyoh = 10 AND st_dwithin(uz_new.geom,
            (SELECT keskusverkko.geom FROM keskusverkko WHERE keskusverkko.keskusnimi = 'Hervanta'), 2000);

    /* Keskustan reunavyöhykkeet */
    INSERT INTO uz_new
        SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 2 AS vyoh
        FROM ykr, uz_new
        WHERE NOT EXISTS (
            SELECT 1 FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom), uz_new.geom) AND uz_new.vyoh = 1
        ) AND st_dwithin(ykr.geom, uz_new.geom,1000) AND uz_new.vyoh = 1 AND (ykr.maa_ha/6.25 > 0.1 OR ykr.v_yht > 0 OR ykr.tp_yht > 0);
    

/* JOUKKOLIIKENNEVYÖHYKKEET */
/* Lasketaan ensin joli-vyöhykkeiden määrittelyyn väestön ja työpaikkojen naapuruussummat (k=9). */
ALTER TABLE ykr 
    ADD COLUMN IF NOT EXISTS v_yht_nn real,
    ADD COLUMN IF NOT EXISTS tp_yht_nn real;
UPDATE ykr AS targetykr
    SET v_yht_nn = n.v_yht_nn, tp_yht_nn = n.tp_yht_nn
    FROM (SELECT DISTINCT ON (nn.xyind) nn.xyind, nn.geom,
        SUM(COALESCE(ykr.v_yht,0)) OVER (PARTITION BY nn.xyind) AS v_yht_nn,
        SUM(COALESCE(ykr.tp_yht,0)) OVER (PARTITION BY nn.xyind) AS tp_yht_nn
    FROM ykr
    CROSS JOIN LATERAL
        (SELECT sqykr.xyind, sqykr.geom from ykr sqykr ORDER BY sqykr.geom <#> ykr.geom limit 9) AS nn
    ) AS n
WHERE targetykr.xyind = n.xyind;

/* Intensiiviset joukkoliikennevyöhykkeet - uudet raideliikenteen pysäkin/asemanseudut */
IF jl_taulu IS NOT NULL	THEN
    INSERT INTO uz_new
        SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind,
        CASE
            WHEN ykr.vyoh = 1 AND jl.k_jltyyp = 'juna' THEN CONCAT(99911,jl.k_liikv)::int
            WHEN ykr.vyoh = 2 AND jl.k_jltyyp = 'juna' THEN CONCAT(99921,jl.k_liikv)::int
            WHEN ykr.vyoh IN (3,12,41) AND jl.k_jltyyp = 'juna' THEN CONCAT(99931,jl.k_liikv)::int
            WHEN ykr.vyoh IN (4,11,40) AND jl.k_jltyyp = 'juna' THEN CONCAT(99941,jl.k_liikv)::int
            WHEN (ykr.vyoh = 5 OR ykr.vyoh IS NULL) AND jl.k_jltyyp = 'juna' THEN CONCAT(99951,jl.k_liikv)::int
            WHEN ykr.vyoh IN (10,11,12) AND jl.k_jltyyp = 'juna' THEN CONCAT(99901,jl.k_liikv)::int
            WHEN ykr.vyoh = 6 AND jl.k_jltyyp = 'juna' THEN CONCAT(99961,jl.k_liikv)::int
            WHEN ykr.vyoh = 1 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99912,jl.k_liikv)::int
            WHEN ykr.vyoh = 2 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99922,jl.k_liikv)::int
            WHEN ykr.vyoh IN (3,12,41) AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99932,jl.k_liikv)::int
            WHEN ykr.vyoh IN (4,11,40) AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99942,jl.k_liikv)::int
            WHEN (ykr.vyoh = 5 OR ykr.vyoh IS NULL) AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99952,jl.k_liikv)::int
            WHEN ykr.vyoh IN (10,11,12,6) AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99902,jl.k_liikv)::int
            WHEN ykr.vyoh = 6 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99962,jl.k_liikv)::int
            ELSE ykr.vyoh END
        AS vyoh
        FROM ykr, uz, jl
        /* Only those that are not already something else */
        WHERE NOT EXISTS (
            SELECT 1
            FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom)
        ) AND st_dwithin(ykr.geom, jl.geom, CASE WHEN jl.k_jltyyp = 'raitiotie' THEN 800 WHEN jl.k_jltyyp = 'juna' THEN 1000 ELSE 400 END) AND jl.k_liikv <= calculationYear;
END IF;

IF jl_taulu IS NOT NULL	THEN
    UPDATE uz_new
        SET vyoh =
        CASE
            WHEN uz_new.vyoh = 1 AND jl.k_jltyyp = 'juna' THEN CONCAT(99911,jl.k_liikv)::int
            WHEN uz_new.vyoh = 2 AND jl.k_jltyyp = 'juna' THEN CONCAT(99921,jl.k_liikv)::int
            WHEN uz_new.vyoh IN (10,11,12) AND jl.k_jltyyp = 'juna' THEN CONCAT(99901,jl.k_liikv)::int
            WHEN uz_new.vyoh = 6 AND jl.k_jltyyp = 'juna' THEN CONCAT(99961,jl.k_liikv)::int
            WHEN uz_new.vyoh = 1 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99912,jl.k_liikv)::int
            WHEN uz_new.vyoh = 2 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99922,jl.k_liikv)::int
            WHEN uz_new.vyoh IN (10,11,12,6) AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99902,jl.k_liikv)::int
            WHEN uz_new.vyoh = 6 AND jl.k_jltyyp = 'raitiotie' THEN CONCAT(99962,jl.k_liikv)::int
            ELSE uz_new.vyoh END
        FROM jl
        /* Only those that are not already something else */
        WHERE uz_new.vyoh IN (1,2,10,11,12,6)
        AND st_dwithin(uz_new.geom, jl.geom, CASE WHEN jl.k_jltyyp = 'raitiotie' THEN 800 WHEN jl.k_jltyyp = 'juna' THEN 1000 ELSE 400 END) AND jl.k_liikv <= calculationYear;
END IF;


/* Intensiiviset joukkoliikennevyöhykkeet - nykyisten kasvatus */
INSERT INTO uz_new
    SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 3 AS vyoh
    FROM ykr, uz
        /* Select only those that are not already something else */
        WHERE NOT EXISTS (
            SELECT 1
            FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom) /* select those that are currently intensiivinen joukkoliikennevyöhyke */
        ) AND (st_intersects(st_centroid(ykr.geom), uz.geom) AND uz.vyoh IN (3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902)
        OR (st_intersects(ykr.geom, uz.geom) AND uz.vyoh IN (3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902)
        AND (st_dwithin(ykr.geom, uz.geom,125) AND uz.vyoh IN (3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902)
        AND (ykr.v_yht_nn > 797 AND ykr.tp_yht_nn > 280))));

/* Intensiiviset joukkoliikennevyöhykkeet - uudet muualle syntyvät vyöhykkeet */
INSERT INTO uz_new
    SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 3 AS vyoh
    FROM ykr, uz
        /* Only those that are not already something else */
        WHERE NOT EXISTS (
            SELECT 1
            FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom) /* select those that are currently intensiivinen joukkoliikennevyöhyke */
        ) AND ykr.v_yht_nn > 797 AND ykr.tp_yht_nn > 280;

/* Poistetaan yksinäiset ruudut */
DELETE FROM uz_new uz1
WHERE uz1.xyind IN (SELECT uz1.xyind
FROM uz_new uz1
CROSS JOIN LATERAL
 (SELECT
    ST_Distance(uz1.geom, uz2.geom) as dist
    FROM uz_new uz2
 	WHERE uz1.xyind <> uz2.xyind AND uz1.vyoh = 3
    ORDER BY uz1.geom <#> uz2.geom
  LIMIT 1) AS test
  WHERE test.dist > 0);

/* Joukkoliikennevyöhykkeet - nykyisten kasvatus */
INSERT INTO uz_new
    SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 4 AS vyoh
    FROM ykr, uz
        /* Only those that are not already something else */
        WHERE NOT EXISTS (
            SELECT 1
            FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom)
        ) AND (st_intersects(st_centroid(ykr.geom), uz.geom) AND uz.vyoh IN (4,11,40)
        AND ykr.vyoh NOT IN (99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902)
        OR (st_intersects(ykr.geom, uz.geom) AND uz.vyoh IN (4,11,40)
        AND
     	(st_dwithin(ykr.geom, uz.geom,125) AND uz.vyoh IN (4,11,40)) AND
     	(ykr.v_yht_nn > 404 AND ykr.tp_yht_nn > 63)
     	));
     	
/* Joukkoliikennevyöhykkeet - uudet muualle syntyvät vyöhykkeet */
INSERT INTO uz_new
    SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 4 AS vyoh
    FROM ykr
        /* Only those that are not already something else */
        WHERE NOT EXISTS (
            SELECT 1
            FROM uz_new
            WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom)
        ) AND ykr.vyoh NOT IN (99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902)
        AND ykr.v_yht_nn > 404 AND ykr.tp_yht_nn > 63;


/* AUTOVYÖHYKKEET */
INSERT INTO uz_new
  SELECT DISTINCT ON (ykr.geom) ykr.geom, ykr.xyind, 5 AS vyoh
    FROM ykr, uz
      /* Only those that are not already something else */
    WHERE NOT EXISTS (
        SELECT 1
        FROM uz_new
        WHERE st_intersects(st_centroid(ykr.geom),uz_new.geom)
    ) AND ((ykr.maa_ha > 0 AND (ykr.v_yht > 0 OR ykr.tp_yht > 0))
    OR st_intersects(st_centroid(ykr.geom), uz.geom) AND uz.vyoh = 5);


/* Yhdistetään vyöhykkeet ykr-taulukkoon ja päivitetään keskustaetäisyydet tiettyihin minimi- ja maksimiarvoihin pakotettuina. */
CREATE TEMP TABLE centers as
SELECT st_centroid((st_dump(st_union(uz_new.geom))).geom) as geom, uz_new.vyoh from uz_new
    WHERE uz_new.vyoh IN (1,10)
    GROUP BY uz_new.vyoh;
ALTER TABLE centers ALTER COLUMN geom TYPE geometry(Point, 3067);
CREATE INDEX ON centers USING GIST (geom);

UPDATE ykr
SET vyoh = uz_new.vyoh
FROM uz_new WHERE ykr.xyind = uz_new.xyind;

UPDATE ykr
SET centdist = sq3.centdist FROM
    (SELECT ykr.xyind, center.centdist
        FROM ykr
        CROSS JOIN LATERAL
            (SELECT st_distance(centers.geom, ykr.geom)/1000 AS centdist
                FROM centers
        WHERE (centers.vyoh = 10 AND ykr.vyoh = 10) OR (centers.vyoh = 1 AND ykr.vyoh <> 10)
        ORDER BY ykr.geom <#> centers.geom
 	 LIMIT 1) AS center) as sq3
WHERE ykr.xyind = sq3.xyind;


/* Poistetaan väliaikaiset taulut ja sarakkeet */
ALTER TABLE ykr 
    DROP COLUMN IF EXISTS v_yht_nn,
    DROP COLUMN IF EXISTS tp_yht_nn,
    DROP COLUMN IF EXISTS alueteho,
    DROP COLUMN IF EXISTS alueteho_muutos;

RETURN QUERY SELECT * FROM ykr;
DROP TABLE IF EXISTS kt, kv, jl, centers, uz_new, uz, ykr;

IF calculationYear = targetYear THEN
    DROP TABLE IF EXISTS keskusverkko;
END IF;

/* Käsittele virhetilanteet */
/* Handle exceptions */
/*
EXCEPTION WHEN OTHERS THEN
    DROP TABLE IF EXISTS kt;
    DROP TABLE IF EXISTS kv;
    DROP TABLE IF EXISTS keskusta_alueet_centroid;
    DROP TABLE IF EXISTS keskusverkko;
    DROP TABLE IF EXISTS jl;
    DROP TABLE IF EXISTS centers;
    DROP TABLE IF EXISTS uz;
    DROP TABLE IF EXISTS uz_new;
*/

END;
$$ LANGUAGE plpgsql;