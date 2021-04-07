DROP FUNCTION IF EXISTS public.CO2_GridProcessing;

CREATE OR REPLACE FUNCTION
public.CO2_GridProcessing(
    aoi regclass, -- Area of interest
    calculationYear integer,
    baseYear integer,
    targetYear integer default null,
    kt_table regclass default null,
    kv_table regclass default null,
    pubtrans_table regclass default null
)

RETURNS TABLE (
    geom geometry(MultiPolygon, 3067),
    xyind varchar,
    mun int,
    zone integer,
    maa_ha real,
    centdist smallint,
    pop smallint,
    employ smallint,
    k_ap_ala int,
    k_ar_ala int,
    k_ak_ala int,
    k_muu_ala int,
    k_tp_yht integer,
    k_poistuma int,
    alueteho real,
    alueteho_muutos real
) AS $$

DECLARE
    km2hm2 real default 1.25;
    poistuma_exists boolean;
    aloitusv_exists boolean;
    valmisv_exists boolean;
    kt_gt text;
    kv_gt text;
    pubtrans_gt text;
    pubtrans_zones int[] default ARRAY[3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902, 99913, 99923, 99933, 99943, 99953, 99963, 99903];
BEGIN

IF calculationYear = baseYear OR targetYear IS NULL OR kt_table IS NULL THEN
    RAISE NOTICE 'Preprocessing raw data';
    /* Luodaan väliaikainen taulu, joka sisältää mm. YKR väestö- ja työpaikkatiedot */
    /* Creating a temporary table with e.g. YKR population and workplace data */
    EXECUTE format(
    'CREATE TEMP TABLE IF NOT EXISTS grid AS SELECT
        DISTINCT ON (grid.xyind, grid.geom)
        grid.geom::geometry(MultiPolygon, 3067),
        grid.xyind::varchar(13),
        grid.mun::int,
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
        (((coalesce(pop.v_yht, 0) + coalesce(employ.tp_yht,0)) * 50 * 1.25)::real / 62500)::real AS alueteho,
        0::real AS alueteho_muutos
        FROM delineations.grid grid
        LEFT JOIN grid_globals.pop pop
            ON grid.xyind::varchar = pop.xyind::varchar
            AND grid.mun::int = pop.kunta::int
        LEFT JOIN grid_globals.employ employ
            ON grid.xyind::varchar = employ.xyind::varchar
            AND grid.mun::int = employ.kunta::int
        LEFT JOIN grid_globals.clc clc 
            ON grid.xyind::varchar = clc.xyind::varchar
        WHERE ST_Intersects(
            st_centroid(grid.geom),
            (SELECT st_union(bounds.geom) FROM %s bounds)
        )'
    , aoi);
    CREATE INDEX ON grid USING GIST (geom);
    CREATE INDEX ON grid (xyind);
    CREATE INDEX ON grid (zone);
    CREATE INDEX ON grid (mun);

END IF;

IF targetYear IS NOT NULL THEN

    EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kt AS SELECT * FROM %s', kt_table);
        SELECT geometrytype(kt.geom) from kt into kt_gt;
        EXECUTE format('ALTER TABLE kt ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', kt_gt);
        CREATE INDEX ON kt USING GIST (geom);

    IF kv_table IS NOT NULL THEN
        EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kv AS SELECT * FROM %s', kv_table);
        SELECT geometrytype(kv.geom) from kv into kv_gt;
        EXECUTE format('ALTER TABLE kv ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', kv_gt);
        CREATE INDEX ON kv USING GIST (geom);
    END IF;

    IF pubtrans_table IS NOT NULL THEN
        EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS pubtrans AS SELECT * FROM %s', pubtrans_table);
        SELECT geometrytype(pubtrans.geom) from pubtrans into pubtrans_gt;
        EXECUTE format('ALTER TABLE pubtrans ALTER COLUMN geom TYPE geometry(%L, 3067) USING ST_force2d(ST_Transform(geom, 3067))', pubtrans_gt);
        CREATE INDEX ON pubtrans USING GIST (geom);
    END IF;

    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_table, 'k_poistuma') INTO poistuma_exists;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_table, 'k_aloitusv') INTO poistuma_exists;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', kt_table, 'k_valmisv') INTO poistuma_exists;

    /* Lasketaan käyttötarkoitusalueille pinta-alat hehtaareina */
    ALTER TABLE kt ADD COLUMN IF NOT EXISTS area_ha real default 0;
    UPDATE kt SET area_ha = ST_AREA(kt.geom)/10000;

    /* Lasketaan käyttötarkoitusalueilta numeeriset arvot grid-ruuduille. Tällä hetkellä tämä tehdään lineaarisesti. Seuraavissa kehitysversioissa tarkastellaan arvojen painotettua jakamista. */
    IF aloitusv_exists AND valmisv_exists THEN
        UPDATE grid
        SET k_ap_ala = (
            SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) *
                (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
            FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear
            AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
        ), k_ar_ala = (
            SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) * (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
            FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear
            AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
        ), k_ak_ala = (
            SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) *
                (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
            FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear
            AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
        ), k_muu_ala = (
            SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) *
                (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
            FROM kt
            WHERE ST_Intersects(grid.geom, kt.geom)
            AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear
            AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
        ), k_tp_yht = (
            SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) * kt.k_tp_yht / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1))
            FROM kt WHERE ST_Intersects(grid.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
        );
            IF poistuma_exists THEN
                UPDATE grid SET k_poistuma = (
                    SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) *
                    (CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) * (-1) ELSE kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
                    FROM kt WHERE ST_Intersects(grid.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
                );
            ELSE
                /* DUMMY for default demolition rate */
                UPDATE grid SET k_poistuma = 999999;
            END IF;
    ELSE 
        UPDATE grid
        SET k_ap_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.k_ap_ala <= 0 THEN 0 ELSE kt.k_ap_ala / (targetYear - baseYear + 1) END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        ), k_ar_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.k_ar_ala <= 0 THEN 0 ELSE kt.k_ar_ala / (targetYear - baseYear + 1) END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        ), k_ak_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.k_ak_ala <= 0 THEN 0 ELSE kt.k_ak_ala / (targetYear - baseYear + 1) END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        ), k_muu_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.k_muu_ala <= 0 THEN 0 ELSE kt.k_muu_ala / (targetYear - baseYear + 1) END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        ), k_tp_yht = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (kt.k_tp_yht / (targetYear - baseYear + 1))), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        );
            IF poistuma_exists THEN
                UPDATE grid SET k_poistuma = (
                    SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                        (kt.area_ha * 10000) *
                        (CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (targetYear - baseYear + 1) * (-1) ELSE kt.k_poistuma / (targetYear - baseYear + 1) END)), 0)
                    FROM kt
                        WHERE ST_Intersects(grid.geom, kt.geom)
                        AND baseYear <= calculationYear
                );
            ELSE
                UPDATE grid SET k_poistuma = 999999;
            END IF;
    END IF;

    /*  Haetaan ruudukolle nykyisen maanpeitteen mukaiset maapinta-alatiedot.
    Päivitetään mahdolliset ranta- ja vesialueiden täytöt maa_ha -sarakkeeseen.
    Maa_ha -arvoksi täytöille asetetaan 5.9, joka on rakennettujen ruutujen keskimääräinen maa-ala.
    Tässä oletetaan, että jos alle 20% ruudusta (1.25 ha) on nykyisin maata, ja alueelle rakennetaan vuodessa yli 200 neliötä kerrostaloja,
    tehdään täyttöjä (laskettu 20%:lla keskimääräisestä n. 10000 m2 rakennusten pohja-alasta per ruutu, jaettuna 10 v. toteutusajalle).
    Lasketaan samalla aluetehokkuuden muutos ja päivitetään aluetehokkuus. */

    UPDATE grid g
        SET maa_ha = 5.9
        WHERE g.k_ak_ala >= 200;
    UPDATE grid
        SET alueteho_muutos = CASE WHEN
            grid.maa_ha != 0
            THEN
            (COALESCE(grid.k_ap_ala,0) + COALESCE(grid.k_ar_ala,0) + COALESCE(grid.k_ak_ala,0) + COALESCE(grid.k_muu_ala,0)) / (10000 * grid.maa_ha)
            ELSE 0 END;
    UPDATE grid
        SET alueteho = CASE WHEN
            COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0) > 0
            THEN
            COALESCE(grid.alueteho,0) + COALESCE(grid.alueteho_muutos,0)
            ELSE 0 END;

    /* Lasketaan väestön lisäys asumisväljyyden avulla. 1.25 = kerroin kerrosalasta huoneistoalaksi. */
    UPDATE grid SET
    pop = COALESCE(grid.pop, 0) +
        (   COALESCE(grid.k_ap_ala, 0)::real / COALESCE(bo.erpien,38)::real +
            COALESCE(grid.k_ar_ala, 0)::real / COALESCE(bo.rivita,35.5)::real +
            COALESCE(grid.k_ak_ala, 0)::real / COALESCE(bo.askert,35)::real
        ) / COALESCE(km2hm2, 1.25)::real,
    employ = COALESCE(grid.employ,0) + COALESCE(grid.k_tp_yht,0)
    FROM built.occupancy bo
        WHERE bo.year = calculationYear
        AND bo.mun::int = grid.mun::int;

    /*  KESKUSVERKON PÄIVITTÄMINEN
    Luodaan väliaikainen taso valtakunnallisesta keskusta-alueaineistosta
    Poistetaan ylimääräiset / virheelliset keskustat
    Muutetaan valtakunnallinen keskusta-alueaineisto keskipisteiksi (Point). */

    IF calculationYear = baseYear THEN
        /* Rajataan valtakunnallinen keskusta-aineisto kattamaan vain tutkimusruuduille lähimmät kohteet. */
        CREATE TEMP TABLE IF NOT EXISTS centralnetwork AS
            SELECT DISTINCT ON (p2.geom) p2.* FROM
            (SELECT p1.xyind as g1,
                (SELECT p.id
                    FROM delineations.centroids AS p
                    WHERE p1.xyind <> p.id::varchar
                    ORDER BY p.geom <#> p1.geom ASC LIMIT 1
                ) AS g2
                    FROM grid AS p1
                    OFFSET 0
            ) AS q
            JOIN grid AS p1
                ON q.g1=p1.xyind
            JOIN delineations.centroids AS p2
                ON q.g2=p2.id;
    END IF;

    /* Lisätään uusia keskuksia keskusverkkoon vain mikäli käyttäjä on tällaisia syöttänyt! */
    IF kv_table IS NOT NULL THEN
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
        AND (COALESCE(suunnitelma.k_kalkuv, baseYear) + (COALESCE(suunnitelma.k_kvalmv,targetYear) - COALESCE(suunnitelma.k_kalkuv,baseYear))/2 >= calculationYear);
    END IF;

    CREATE INDEX ON centralnetwork USING GIST (geom);

    /* Päivitetään grid-perusruutuihin etäisyys (centdist, km, real) lähimpään keskukseen. */
    UPDATE grid
    SET centdist = sq2.centdist FROM
        (SELECT grid.xyind, keskusta.centdist
        FROM grid
        CROSS JOIN LATERAL
            (SELECT ST_Distance(ST_CENTROID(keskustat.geom), grid.geom)/1000 AS centdist
                FROM centralnetwork keskustat
            WHERE keskustat.keskustyyp != 'Kaupunkiseudun pieni alakeskus'
            ORDER BY grid.geom <#> keskustat.geom
        LIMIT 1) AS keskusta) as sq2
    WHERE grid.xyind = sq2.xyind;

    /* YHDYSKUNTARAKENTEEN VYÖHYKKEIDEN PÄIVITTÄMINEN */
    CREATE TEMP TABLE IF NOT EXISTS grid_new AS
    SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 1 AS zone -- 'Keskustan jalankulkuvyöhyke'
    FROM grid
    /* Search for grid cells within current UZ central areas delineation */
    /* and those cells that touch the current centers - have to use d_within for fastest approximation, st_touches doesn't work due to false DE-9IM relations */
    WHERE grid.zone = 1 OR grid.maa_ha != 0 AND
        st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM grid
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
                        FROM grid
                        WHERE grid.zone = 1
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz1
        
    UNION
    
    /* Olemassaolevien alakeskusten reunojen kasvatus */
    SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 10 AS zone
    FROM grid, centralnetwork /* keskus */
    /* Search for grid cells within current UZ central areas delineation */
    WHERE grid.zone IN (10,11,12,6,837101) OR grid.maa_ha != 0
        AND st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM grid
                WHERE grid.zone IN (10, 11, 12, 6)
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
                        FROM grid
                        WHERE grid.zone = 1
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz10
        
    UNION
    
    SELECT * FROM
        (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 10 AS zone
        FROM grid, centralnetwork
        WHERE grid.maa_ha != 0
        AND (st_dwithin(grid.geom, centralnetwork.geom, 250) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus')
        OR (st_dwithin(grid.geom, centralnetwork.geom, 500) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus'
            AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0))
        ) uz10new;

    CREATE INDEX ON grid_new USING GIST (geom);

    /* Erityistapaukset */
    UPDATE grid_new SET zone = 6 WHERE grid_new.zone IN (837101, 10) AND st_dwithin(grid_new.geom,
            (SELECT centralnetwork.geom FROM centralnetwork WHERE centralnetwork.keskusnimi = 'Hervanta'), 2000);

    /* Keskustan reunavyöhykkeet */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 2 AS zone
        FROM grid, grid_new
        WHERE NOT EXISTS (
            SELECT 1 FROM grid_new
            WHERE st_intersects(st_centroid(grid.geom), grid_new.geom) AND grid_new.zone = 1
        ) AND st_dwithin(grid.geom, grid_new.geom,1000)
        AND grid_new.zone = 1
        AND (grid.maa_ha/6.25 > 0.1 OR grid.pop > 0 OR grid.employ > 0);
    

    /* JOUKKOLIIKENNEVYÖHYKKEET */
    /* Lasketaan ensin joli-vyöhykkeiden määrittelyyn väestön ja työpaikkojen naapuruussummat (k=9). */
    ALTER TABLE grid 
        ADD COLUMN IF NOT EXISTS pop_nn real,
        ADD COLUMN IF NOT EXISTS employ_nn real;
    UPDATE grid AS targetgrid
        SET pop_nn = n.pop_nn, employ_nn = n.employ_nn
        FROM (SELECT DISTINCT ON (nn.xyind) nn.xyind, nn.geom,
            SUM(COALESCE(grid.pop,0)) OVER (PARTITION BY nn.xyind) AS pop_nn,
            SUM(COALESCE(grid.employ,0)) OVER (PARTITION BY nn.xyind) AS employ_nn
        FROM grid
        CROSS JOIN LATERAL (
                SELECT sqgrid.xyind, sqgrid.geom
                FROM grid sqgrid ORDER BY sqgrid.geom <#> grid.geom
                LIMIT 9
            ) AS nn
        ) AS n
    WHERE targetgrid.xyind = n.xyind;

    /* Intensiiviset joukkoliikennevyöhykkeet - uudet raideliikenteen pysäkin/asemanseudut */

    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, grid.zone FROM grid WHERE grid.ZONE = 3 AND grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new);

    IF pubtrans_table IS NOT NULL THEN
        INSERT INTO grid_new
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
            FROM grid, pubtrans
            /* Only those that are not already something else */
            WHERE NOT EXISTS (
                SELECT 1
                FROM grid_new
                WHERE st_intersects(st_centroid(grid.geom),grid_new.geom)
            ) AND st_dwithin(
                grid.geom,
                pubtrans.geom,
                CASE WHEN pubtrans.k_jltyyp = 1 THEN 1000
                    WHEN pubtrans.k_jltyyp = 2 THEN 800
                    ELSE 400 END
            ) AND pubtrans.k_liikv <= calculationYear;
    END IF;

    -- Päivitetään joukkoliikennevyöhykkeet aiemmin muodostettujen uusien keskustojen/alakeskusten osalta
    IF pubtrans_table IS NOT NULL THEN
        UPDATE grid_new
            SET zone =
            CONCAT('999',
                CASE WHEN grid_new.zone IN (1,2,6) THEN grid_new.zone::varchar
                    WHEN grid_new.zone = 12 THEN '3'
                    WHEN grid_new.zone = 11 THEN '4'
                    WHEN grid_new.zone = 10 THEN '0' END,
                pubtrans.k_jltyyp::varchar, -- 1 = 'juna', 2 = 'raitiotie, 3 = 'bussi',
                pubtrans.k_liikv::varchar
            )::int
            FROM pubtrans
            WHERE grid_new.zone IN (1,2,10,11,12,6)
            AND st_dwithin(
                grid_new.geom,
                pubtrans.geom,
                CASE WHEN pubtrans.k_jltyyp = 1 THEN 1000
                    WHEN pubtrans.k_jltyyp = 2 THEN 800 
                    ELSE 400 END
            ) AND pubtrans.k_liikv <= calculationYear;
    END IF;

    /* Intensiiviset joukkoliikennevyöhykkeet - nykyisten kasvatus ja uudet muualle syntyvät vyöhykkeet */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        3 AS zone
        FROM grid
            /* Only select those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND grid.zone != ANY(pubtrans_zones)
            AND grid.pop_nn > 797 AND grid.employ_nn > 280;

    /* Joukkoliikennevyöhykkeet - nykyisten kasvatus ja uudet muualle syntyvät vyöhykkeet*/
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        4 AS zone
        FROM grid
            /* Only select those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND grid.zone != ANY (pubtrans_zones)
            AND grid.zone = 4 OR (grid.pop_nn > 404 AND grid.employ_nn > 63);

    /* Poistetaan yksinäiset ruudut */
    DELETE FROM grid_new uz1
    WHERE uz1.xyind IN (SELECT uz1.xyind
    FROM grid_new uz1
    CROSS JOIN LATERAL
    (SELECT
        ST_Distance(uz1.geom, uz2.geom) as dist
        FROM grid_new uz2
        WHERE uz1.xyind <> uz2.xyind AND uz1.zone IN (3,4)
        ORDER BY uz1.geom <#> uz2.geom
    LIMIT 1) AS test
    WHERE test.dist > 0);

    /* Autovyöhykkeet */
    INSERT INTO grid_new
    SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        CASE WHEN grid.zone IN (5, 81,82,83,84,85,86,87) THEN grid.zone ELSE 5 END as zone
        FROM grid
        /* Only select those that are not already something else */
        WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
        AND grid.maa_ha > 0 AND (grid.pop > 0 OR grid.employ > 0);

    /* Yhdistetään vyöhykkeet grid-taulukkoon ja päivitetään keskustaetäisyydet tiettyihin minimi- ja maksimiarvoihin pakotettuina. */
    UPDATE grid SET
        zone = grid_new.zone
        FROM grid_new
            WHERE grid.xyind = grid_new.xyind;

    UPDATE grid
    SET centdist = sq3.centdist FROM
        (SELECT grid.xyind, center.centdist
            FROM grid
            CROSS JOIN LATERAL
                (SELECT st_distance(centers.geom, grid.geom)/1000 AS centdist
                    FROM centralnetwork centers
            ORDER BY grid.geom <#> centers.geom
        LIMIT 1) AS center) as sq3
    WHERE grid.xyind = sq3.xyind;

    /* Poistetaan väliaikaiset taulut ja sarakkeet */
    ALTER TABLE grid 
        DROP COLUMN IF EXISTS pop_nn,
        DROP COLUMN IF EXISTS employ_nn;

    RETURN QUERY SELECT * FROM grid;
    DROP TABLE IF EXISTS kt, kv, pubtrans, grid_new;

    IF calculationYear = targetYear THEN
        DROP TABLE IF EXISTS centralnetwork, grid;
    END IF;

ELSE 
    RETURN QUERY SELECT * FROM grid;
END IF;

END;
$$ LANGUAGE plpgsql;