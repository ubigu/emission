drop table if exists tests.processing1, kt, kv, pubtrans;
create table tests.processing1 as 

SELECT
DISTINCT ON (grid.xyind, grid.geom)
grid.geom::geometry(MultiPolygon, 3067),
grid.xyind::varchar(13),
grid.mun::varchar(3),
grid.zone::int,
clc.maa_ha::real,
grid.centdist::smallint,
pop.v_yht::smallint AS pop,
employ.tp_yht::smallint AS employ,
0::int AS k_ap_ala,
0::int AS k_ar_ala,
0::int AS k_ak_ala,
0::int AS k_muu_ala,
0::int AS k_tp_yht,
0::int AS k_poistuma,
((coalesce(pop.v_yht, 0) + coalesce(employ.tp_yht,0)) * 50 * 1.25)::real / 62500 AS alueteho,
0::real AS alueteho_muutos
FROM delineations.grid grid
LEFT JOIN grid_globals.pop pop
	ON grid.xyind = pop.xyind
	AND grid.mun = pop.kunta
LEFT JOIN grid_globals.employ employ
	ON grid.xyind = employ.xyind
	AND grid.mun = employ.kunta
LEFT JOIN grid_globals.clc clc 
	ON grid.xyind = clc.xyind
WHERE ST_Intersects(
	st_centroid(grid.geom),
	(SELECT st_union(bounds.geom) FROM tests.aluerajaus_vuores bounds));
	
CREATE TEMP TABLE IF NOT EXISTS kt AS SELECT * FROM tests.kt_bau_kaavaehdotus_vuores;
ALTER TABLE kt ALTER COLUMN geom TYPE geometry(MultiPolygon, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON kt USING GIST (geom);

CREATE TEMP TABLE IF NOT EXISTS kv AS SELECT * FROM tests.kv_ehdotus_ve3_vuores;
	ALTER TABLE kv ALTER COLUMN geom TYPE geometry(MultiPoint, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON kv USING GIST (geom);

CREATE TEMP TABLE IF NOT EXISTS pubtrans AS SELECT * FROM tests.jl_ehdotus_ve4_vuores;
    ALTER TABLE pubtrans ALTER COLUMN geom TYPE geometry(MultiPoint, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON pubtrans USING GIST (geom);
	
ALTER TABLE kt ADD COLUMN IF NOT EXISTS area_ha real default 0;
UPDATE kt SET area_ha = ST_AREA(kt.geom)/10000;

 UPDATE tests.processing1 grid
    SET k_ap_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_ar_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_ak_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_muu_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_tp_yht = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (kt.k_tp_yht / (2025 - 2021+ 1)))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    );

	UPDATE tests.processing1 grid SET k_poistuma = (
		SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
			(kt.area_ha * 10000) *
			(CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (2025 - 2021 + 1) * (-1) ELSE kt.k_poistuma / (2025 - 2021 + 1) END))
		FROM kt
			WHERE ST_Intersects(grid.geom, kt.geom)
			AND 2021 <= 2022
	);
	
UPDATE tests.processing1 g
    SET maa_ha = 5.9
    WHERE g.k_ak_ala >= 200;
UPDATE tests.processing1 grid
    SET alueteho_muutos = (CASE WHEN grid.maa_ha != 0 THEN (COALESCE(grid.k_ap_ala,0) + COALESCE(grid.k_ar_ala,0) + COALESCE(grid.k_ak_ala,0) + COALESCE(grid.k_muu_ala,0)) / (10000 * grid.maa_ha) ELSE 0 END);
UPDATE tests.processing1  grid
    SET alueteho = (CASE WHEN COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0) > 0 THEN COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0) ELSE 0 END);

UPDATE  tests.processing1 grid SET
pop = COALESCE(grid.pop, 0) +
	(   COALESCE(grid.k_ap_ala, 0)::real / COALESCE(bo.erpien,38)::real +
		COALESCE(grid.k_ar_ala, 0)::real / COALESCE(bo.rivita,35.5)::real +
		COALESCE(grid.k_ak_ala, 0)::real / COALESCE(bo.askert,35)::real
	) /1.25::real,
employ = COALESCE(grid.employ,0) + COALESCE(grid.k_tp_yht,0)
FROM built.occupancy bo
WHERE bo.year = 2022
AND bo.mun = grid.mun;

/* Rajataan valtakunnallinen keskusta-aineisto kattamaan vain tutkimusruuduille lähimmät kohteet. */
DROP TABLE IF EXISTS centralnetwork;
CREATE TEMP TABLE IF NOT EXISTS centralnetwork AS
	SELECT DISTINCT ON (p2.geom) p2.* FROM
	(SELECT p1.xyind as g1,
		(SELECT p.id
			FROM delineations.centroids AS p
			WHERE p1.xyind <> p.id::varchar
			ORDER BY p.geom <#> p1.geom ASC LIMIT 1
		) AS g2
			FROM tests.processing1 AS p1
			OFFSET 0
	) AS q
	JOIN tests.processing1 AS p1
	ON q.g1=p1.xyind
	JOIN delineations.centroids AS p2
	ON q.g2=p2.id;

INSERT INTO centralnetwork
    SELECT (SELECT MAX(k.id) FROM centralnetwork k) + row_number() over (order by suunnitelma.geom desc),
        st_force2d((ST_DUMP(suunnitelma.geom)).geom) as geom,
        k_ktyyp AS keskustyyp,
        k_knimi AS keskusnimi
    FROM kv suunnitelma
    WHERE NOT EXISTS (
        SELECT 1
        FROM centralnetwork keskustat
        WHERE ST_DWithin(suunnitelma.geom, keskustat.geom, 1500)
    ) AND suunnitelma.k_ktyyp = 'Kaupunkiseudun iso alakeskus'
    AND (COALESCE(suunnitelma.k_kalkuv, 2021) + (COALESCE(suunnitelma.k_kvalmv,2025) - COALESCE(suunnitelma.k_kalkuv,2021))/2 >= 2022);

drop table if exists tests.processing1, kt, kv, pubtrans;
create table tests.processing1 as 

SELECT
DISTINCT ON (grid.xyind, grid.geom)
grid.geom::geometry(MultiPolygon, 3067),
grid.xyind::varchar(13),
grid.mun::varchar(3),
grid.zone::int,
clc.maa_ha::real,
grid.centdist::smallint,
pop.v_yht::smallint AS pop,
employ.tp_yht::smallint AS employ,
0::int AS k_ap_ala,
0::int AS k_ar_ala,
0::int AS k_ak_ala,
0::int AS k_muu_ala,
0::int AS k_tp_yht,
0::int AS k_poistuma,
((coalesce(pop.v_yht, 0) + coalesce(employ.tp_yht,0)) * 50 * 1.25)::real / 62500 AS alueteho,
0::real AS alueteho_muutos
FROM delineations.grid grid
LEFT JOIN grid_globals.pop pop
	ON grid.xyind = pop.xyind
	AND grid.mun = pop.kunta
LEFT JOIN grid_globals.employ employ
	ON grid.xyind = employ.xyind
	AND grid.mun = employ.kunta
LEFT JOIN grid_globals.clc clc 
	ON grid.xyind = clc.xyind
WHERE ST_Intersects(
	st_centroid(grid.geom),
	(SELECT st_union(bounds.geom) FROM tests.aluerajaus_vuores bounds));
	
CREATE TEMP TABLE IF NOT EXISTS kt AS SELECT * FROM tests.kt_bau_kaavaehdotus_vuores;
ALTER TABLE kt ALTER COLUMN geom TYPE geometry(MultiPolygon, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON kt USING GIST (geom);

CREATE TEMP TABLE IF NOT EXISTS kv AS SELECT * FROM tests.kv_ehdotus_ve3_vuores;
	ALTER TABLE kv ALTER COLUMN geom TYPE geometry(MultiPoint, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON kv USING GIST (geom);

CREATE TEMP TABLE IF NOT EXISTS pubtrans AS SELECT * FROM tests.jl_ehdotus_ve4_vuores;
    ALTER TABLE pubtrans ALTER COLUMN geom TYPE geometry(MultiPoint, 3067) USING ST_force2d(ST_Transform(geom, 3067));
    CREATE INDEX ON pubtrans USING GIST (geom);
	
ALTER TABLE kt ADD COLUMN IF NOT EXISTS area_ha real default 0;
UPDATE kt SET area_ha = ST_AREA(kt.geom)/10000;

 UPDATE tests.processing1 grid
    SET k_ap_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_ar_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_ak_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_muu_ala = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (2025 - 2021 + 1) END))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    ), k_tp_yht = (
        SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
            (kt.area_ha * 10000) *
            (kt.k_tp_yht / (2025 - 2021+ 1)))
        FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND 2021 <= 2022
    );

	UPDATE tests.processing1 grid SET k_poistuma = (
		SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
			(kt.area_ha * 10000) *
			(CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (2025 - 2021 + 1) * (-1) ELSE kt.k_poistuma / (2025 - 2021 + 1) END))
		FROM kt
			WHERE ST_Intersects(grid.geom, kt.geom)
			AND 2021 <= 2022
	);
	
UPDATE tests.processing1 g
    SET maa_ha = 5.9
    WHERE g.k_ak_ala >= 200;
UPDATE tests.processing1 grid
    SET alueteho_muutos = (CASE WHEN grid.maa_ha != 0 THEN (COALESCE(grid.k_ap_ala,0) + COALESCE(grid.k_ar_ala,0) + COALESCE(grid.k_ak_ala,0) + COALESCE(grid.k_muu_ala,0)) / (10000 * grid.maa_ha) ELSE 0 END);
UPDATE tests.processing1  grid
    SET alueteho = (CASE WHEN COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0) > 0 THEN COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0) ELSE 0 END);

UPDATE  tests.processing1 grid SET
pop = COALESCE(grid.pop, 0) +
	(   COALESCE(grid.k_ap_ala, 0)::real / COALESCE(bo.erpien,38)::real +
		COALESCE(grid.k_ar_ala, 0)::real / COALESCE(bo.rivita,35.5)::real +
		COALESCE(grid.k_ak_ala, 0)::real / COALESCE(bo.askert,35)::real
	) /1.25::real,
employ = COALESCE(grid.employ,0) + COALESCE(grid.k_tp_yht,0)
FROM built.occupancy bo
WHERE bo.year = 2022
AND bo.mun = grid.mun;

/* Rajataan valtakunnallinen keskusta-aineisto kattamaan vain tutkimusruuduille lähimmät kohteet. */
DROP TABLE IF EXISTS centralnetwork;
CREATE TEMP TABLE IF NOT EXISTS centralnetwork AS
	SELECT DISTINCT ON (p2.geom) p2.* FROM
	(SELECT p1.xyind as g1,
		(SELECT p.id
			FROM delineations.centroids AS p
			WHERE p1.xyind <> p.id::varchar
			ORDER BY p.geom <#> p1.geom ASC LIMIT 1
		) AS g2
			FROM tests.processing1 AS p1
			OFFSET 0
	) AS q
	JOIN tests.processing1 AS p1
	ON q.g1=p1.xyind
	JOIN delineations.centroids AS p2
	ON q.g2=p2.id;

INSERT INTO centralnetwork
    SELECT (SELECT MAX(k.id) FROM centralnetwork k) + row_number() over (order by suunnitelma.geom desc),
        st_force2d((ST_DUMP(suunnitelma.geom)).geom) as geom,
        k_ktyyp AS keskustyyp,
        k_knimi AS keskusnimi
    FROM kv suunnitelma
    WHERE NOT EXISTS (
        SELECT 1
        FROM centralnetwork keskustat
        WHERE ST_DWithin(suunnitelma.geom, keskustat.geom, 1500)
    ) AND suunnitelma.k_ktyyp = 'Kaupunkiseudun iso alakeskus'
    AND (COALESCE(suunnitelma.k_kalkuv, 2021) + (COALESCE(suunnitelma.k_kvalmv,2025) - COALESCE(suunnitelma.k_kalkuv,2021))/2 >= 2022);

CREATE INDEX ON centralnetwork USING GIST (geom);

/* Päivitetään grid-perusruutuihin etäisyys (centdist, km, real) lähimpään keskukseen. */
UPDATE tests.processing1 grid
SET centdist = sq2.centdist FROM
    (SELECT grid.xyind, keskusta.centdist
    FROM tests.processing1 grid
    CROSS JOIN LATERAL
        (SELECT ST_Distance(ST_CENTROID(keskustat.geom), grid.geom)/1000 AS centdist
            FROM centralnetwork keskustat
        WHERE keskustat.keskustyyp != 'Kaupunkiseudun pieni alakeskus'
        ORDER BY grid.geom <#> keskustat.geom
    LIMIT 1) AS keskusta) as sq2
WHERE grid.xyind = sq2.xyind;

DROP TABLE IF EXISTS tests.grid_new;
CREATE TABLE IF NOT EXISTS tests.grid_new AS
SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 1 AS zone -- 'Keskustan jalankulkuvyöhyke'
    FROM tests.processing1 grid
    /* Search for grid cells within current UZ central areas delineation */
    /* and those cells that touch the current centers - have to use d_within for fastest approximation, st_touches doesn't work due to false DE-9IM relations */
    WHERE  grid.zone = 1 OR grid.maa_ha != 0 AND
        st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM tests.processing1 grid
                WHERE grid.zone = 1
            ), 25)
        /* Main centers must be within 1.5 km from core */
        AND st_dwithin(grid.geom,
            (SELECT centralnetwork.geom
                FROM centralnetwork
                WHERE centralnetwork.keskustyyp = 'Kaupunkiseudun keskusta'
            ), 1500)
        AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0)
        /* Select only edge neighbours, no corner touchers */
        /* we have to use a buffer + area based intersection trick due to topological errors */
        AND st_area(
            st_intersection(
                grid.geom,
                st_buffer(
                    (SELECT st_union(grid.geom)
                        FROM tests.processing1 grid
                        WHERE grid.zone = 1
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz1
        
    UNION
    
    /* Olemassaolevien alakeskusten reunojen kasvatus */
    SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 10 AS zone
    FROM tests.processing1 grid, centralnetwork /* keskus */
    /* Search for grid cells within current UZ central areas delineation */
    WHERE grid.zone IN (10,11,12,6,837101) OR grid.maa_ha != 0
        AND st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM tests.processing1 grid
                WHERE grid.zone IN (10, 11, 12, 6, 837101)
            ), 25)
        AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0)
        /* Select only edge neighbours, no corner touchers */
        /* we have to use a buffer + area based intersection trick due to topological errors */
        AND st_area(
            st_intersection(
                grid.geom,
                st_buffer(
                    (SELECT st_union(grid.geom)
                        FROM tests.processing1 grid
                        WHERE grid.zone = 1
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz10
        
    UNION
    
    SELECT * FROM
        (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 10 AS zone
        FROM tests.processing1 grid, centralnetwork
        WHERE grid.maa_ha != 0
        AND (st_dwithin(grid.geom, centralnetwork.geom, 250) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus')
        OR (st_dwithin(grid.geom, centralnetwork.geom, 500) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus'
            AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0))
        ) uz10new;

    CREATE INDEX ON tests.grid_new USING GIST (geom);

    /* Erityistapaukset */
    UPDATE tests.grid_new SET zone = 6 WHERE grid_new.zone IN (837101, 10) AND st_dwithin(grid_new.geom,
            (SELECT centralnetwork.geom FROM centralnetwork WHERE centralnetwork.keskusnimi = 'Hervanta'), 2000);

    /* Keskustan reunavyöhykkeet */
    INSERT INTO tests.grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 2 AS zone
        FROM tests.processing1 grid, tests.grid_new grid_new
        WHERE NOT EXISTS (
            SELECT 1 FROM tests.grid_new grid_new
            WHERE st_intersects(st_centroid(grid.geom), grid_new.geom) AND grid_new.zone = 1
        ) AND st_dwithin(grid.geom, grid_new.geom,1000)
        AND grid_new.zone = 1
        AND (grid.maa_ha/6.25 > 0.1 OR grid.pop > 0 OR grid.employ > 0);
    
/* Lasketaan ensin joli-vyöhykkeiden määrittelyyn väestön ja työpaikkojen naapuruussummat (k=9). */
ALTER TABLE tests.processing1 
    ADD COLUMN IF NOT EXISTS pop_nn real,
    ADD COLUMN IF NOT EXISTS employ_nn real;
UPDATE tests.processing1 AS targetgrid
    SET pop_nn = n.pop_nn, employ_nn = n.employ_nn
    FROM (SELECT DISTINCT ON (nn.xyind) nn.xyind, nn.geom,
        SUM(COALESCE(grid.pop,0)) OVER (PARTITION BY nn.xyind) AS pop_nn,
        SUM(COALESCE(grid.employ,0)) OVER (PARTITION BY nn.xyind) AS employ_nn
    FROM tests.processing1  grid
    CROSS JOIN LATERAL (
            SELECT sqgrid.xyind, sqgrid.geom
            FROM tests.processing1 sqgrid ORDER BY sqgrid.geom <#> grid.geom
            LIMIT 9
        ) AS nn
    ) AS n
WHERE targetgrid.xyind = n.xyind;


INSERT INTO tests.grid_new
    SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, zone FROM tests.processing1 grid WHERE ZONE = 3 AND grid.xyind NOT IN (SELECT xyind FROM tests.grid_new);


INSERT INTO tests.grid_new
	SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
	CONCAT('999',
		CASE WHEN grid.zone IN (1,2,3,4,5,6) THEN grid.zone::varchar
			WHEN grid.zone IN (12,41,81) THEN '3'
			WHEN grid.zone IN (11,40,82) THEN '4'
			WHEN grid.zone IN (83,84,85,86,87)  THEN '5'
			WHEN grid.zone IN (10,11,12) THEN '0' END,
		pubtrans.k_jltyyp::varchar, -- 1 = 'juna', 2 = 'raitiotie, 3 = 'bussi',
		pubtrans.k_liikv::varchar
	)::int AS zone
	FROM tests.processing1 grid, pubtrans
	/* Only those that are not already something else */
	WHERE NOT EXISTS (
		SELECT 1
		FROM tests.grid_new
		WHERE st_intersects(st_centroid(grid.geom),grid_new.geom)
	) AND st_dwithin(
		grid.geom,
		pubtrans.geom,
		CASE WHEN pubtrans.k_jltyyp = 1 THEN 1000
			WHEN pubtrans.k_jltyyp = 2 THEN 800
			ELSE 400 END
	) AND pubtrans.k_liikv <= 2022;

INSERT INTO tests.grid_new
    SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
    3 AS zone
    FROM tests.processing1 grid
        /* Only select those that are not already something else */
        WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM tests.grid_new)
        AND grid.zone != ANY(ARRAY[3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902, 99913, 99923, 99933, 99943, 99953, 99963, 99903])
        AND grid.pop_nn > 797 AND grid.employ_nn > 280;

INSERT INTO tests.grid_new
	SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
	4 AS zone
	FROM tests.processing1 grid
	/* Only select those that are not already something else */
	WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM tests.grid_new)
        AND grid.zone != ANY(ARRAY[3,12,41,99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902, 99913, 99923, 99933, 99943, 99953, 99963, 99903])
	AND grid.zone = 4 OR (grid.pop_nn > 404 AND grid.employ_nn > 63);

DELETE FROM tests.grid_new uz1
WHERE uz1.xyind IN (SELECT uz1.xyind
FROM tests.grid_new uz1
CROSS JOIN LATERAL
 (SELECT
    ST_Distance(uz1.geom, uz2.geom) as dist
    FROM tests.grid_new uz2
 	WHERE uz1.xyind <> uz2.xyind AND uz1.zone IN (3,4)
    ORDER BY uz1.geom <#> uz2.geom
  LIMIT 1) AS test
  WHERE test.dist > 0);

/* Autovyöhykkeet */
INSERT INTO tests.grid_new
  SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
    CASE WHEN zone IN (5, 81,82,83,84,85,86,87) THEN zone ELSE 5 END as zone
    FROM tests.processing1 grid
    /* Only select those that are not already something else */
    WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM tests.grid_new)
    AND grid.maa_ha > 0 AND (grid.pop > 0 OR grid.employ > 0);
	
UPDATE tests.processing1 grid
SET centdist = sq3.centdist FROM
    (SELECT grid.xyind, center.centdist
        FROM tests.processing1 grid
        CROSS JOIN LATERAL
            (SELECT st_distance(centers.geom, grid.geom)/1000 AS centdist
                FROM delineations.centralnetwork centers
        ORDER BY grid.geom <#> centers.geom
 	 LIMIT 1) AS center) as sq3
WHERE grid.xyind = sq3.xyind;

UPDATE tests.processing1 grid SET
    zone = grid_new.zone
    FROM tests.grid_new
        WHERE grid.xyind = grid_new.xyind;