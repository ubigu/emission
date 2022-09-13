DROP FUNCTION IF EXISTS public.CO2_GridProcessing;

CREATE OR REPLACE FUNCTION
public.CO2_GridProcessing(
    aoi regclass, -- Area of interest
    calculationYear integer,
    baseYear integer,
    km2hm2 real default 1.25,
    targetYear integer default null,
    plan_areas regclass default null,
    plan_centers regclass default null,
    plan_transit regclass default null
)

RETURNS TABLE (
    geom geometry(MultiPolygon, 3067),
    xyind varchar,
    mun int,
    zone bigint,
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
    demolitionsExist boolean;
    startYearExists boolean;
    endYearExists boolean;
    pubtrans_zones int[] default ARRAY[3,12,41, 99911, 99921, 99931, 99941, 99951, 99961, 99901, 99912, 99922, 99932, 99942, 99952, 99962, 99902, 99913, 99923, 99933, 99943, 99953, 99963, 99903];
BEGIN

IF calculationYear = baseYear OR targetYear IS NULL OR plan_areas IS NULL THEN
    /* Creating a temporary table with e.g. YKR population and workplace data */
    EXECUTE format(
    'CREATE TEMP TABLE IF NOT EXISTS grid AS SELECT
        DISTINCT ON (grid.xyind, grid.geom)
        grid.geom :: geometry(MultiPolygon, 3067),
        grid.xyind :: varchar(13),
        grid.mun :: int,
        grid.zone :: bigint,
        clc.maa_ha :: real,
        grid.centdist :: smallint,
        coalesce(pop.v_yht, 0) :: smallint AS pop,
        coalesce(employ.tp_yht, 0) :: smallint AS employ,
        0 :: int AS k_ap_ala,
        0 :: int AS k_ar_ala,
        0 :: int AS k_ak_ala,
        0 :: int AS k_muu_ala,
        0 :: int AS k_tp_yht,
        0 :: int AS k_poistuma,
        (((coalesce(pop.v_yht, 0) + coalesce(employ.tp_yht, 0)) * 50 * 1.25) :: real / 62500) :: real AS alueteho,
        0 :: real AS alueteho_muutos
        FROM delineations.grid grid
        LEFT JOIN grid_globals.pop pop
            ON grid.xyind :: varchar = pop.xyind :: varchar
            AND grid.mun :: int = pop.kunta :: int
        LEFT JOIN grid_globals.employ employ
            ON grid.xyind :: varchar = employ.xyind :: varchar
            AND grid.mun :: int = employ.kunta :: int
        LEFT JOIN grid_globals.clc clc 
            ON grid.xyind :: varchar = clc.xyind :: varchar
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

IF targetYear IS NOT NULL AND plan_areas IS NOT NULL THEN

    EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kt AS SELECT * FROM %s', plan_areas);
        ALTER TABLE kt
            ALTER COLUMN geom TYPE geometry(MultiPolygon, 3067)
                USING ST_force2d(ST_Transform(geom, 3067));
        /* Calculate plan surface areas */
        ALTER TABLE kt
            ADD COLUMN IF NOT EXISTS area_ha real default 0;
            UPDATE kt SET area_ha = ST_AREA(kt.geom)/10000;
        CREATE INDEX ON kt USING GIST (geom);

    IF plan_centers IS NOT NULL THEN
        EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS kv AS SELECT * FROM %s', plan_centers);
        ALTER TABLE kv
            ALTER COLUMN geom TYPE geometry(Point, 3067)
                USING ST_force2d(ST_Transform(geom, 3067));
        CREATE INDEX ON kv USING GIST (geom);
    END IF;

    IF plan_transit IS NOT NULL THEN
        EXECUTE format('CREATE TEMP TABLE IF NOT EXISTS pubtrans AS SELECT * FROM %s', plan_transit);
        ALTER TABLE pubtrans
            ALTER COLUMN geom TYPE geometry(Point, 3067)
                USING ST_force2d(ST_Transform(geom, 3067));
        CREATE INDEX ON pubtrans USING GIST (geom);
    END IF;

    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', plan_areas, 'k_poistuma') INTO demolitionsExist;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', plan_areas, 'k_aloitusv') INTO startYearExists;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', plan_areas, 'k_valmisv') INTO endYearExists;
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = %L::regclass AND attname = %L AND NOT attisdropped)', plan_areas, 'year_completion') INTO completionYearExists;

    /* Lasketaan käyttötarkoitusalueilta numeeriset arvot grid-ruuduille. */
    IF startYearExists AND endYearExists THEN
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
            IF demolitionsExist THEN
                UPDATE grid SET k_poistuma = (
                    SELECT SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) / (kt.area_ha * 10000) *
                    (CASE WHEN kt.k_poistuma < 0 THEN kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) * (-1) ELSE kt.k_poistuma / (COALESCE(kt.k_valmisv,targetYear) - COALESCE(kt.k_aloitusv,baseYear) + 1) END))
                    FROM kt WHERE ST_Intersects(grid.geom, kt.geom) AND COALESCE(kt.k_aloitusv,baseYear) <= calculationYear AND COALESCE(kt.k_valmisv,targetYear) >= calculationYear
                );
            ELSE
                /* DUMMY for default demolition rate */
                UPDATE grid SET k_poistuma = 999999;
            END IF;
    ELSE IF completionYearExists
      UPDATE grid
        SET k_ap_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.kem2 <= 0 THEN 0 ELSE kt.kem2 END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND kt.type IN ('ao','ap')
                AND kt.year_completion = calculationYear
        ), k_ar_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.kem2 <= 0 THEN 0 ELSE kt.kem2 END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND kt.type IN ('ar','kr')
                AND kt.year_completion = calculationYear
        ), k_ak_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.kem2 <= 0 THEN 0 ELSE kt.kem2 END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND kt.type IN ('ak','c')
                AND kt.year_completion = calculationYear
        ), k_muu_ala = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (CASE WHEN kt.kem2 <= 0 THEN 0 ELSE kt.kem2 END)), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND kt.type IN ('tp','muu')
                AND kt.year_completion = calculationYear
        ), k_tp_yht = (
            SELECT COALESCE(SUM(ST_Area(ST_Intersection(grid.geom, kt.geom)) /
                (kt.area_ha * 10000) *
                (kt.k_tp_yht / (targetYear - baseYear + 1))), 0)
            FROM kt
                WHERE ST_Intersects(grid.geom, kt.geom)
                AND baseYear <= calculationYear
        );
            IF demolitionsExist THEN
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
            IF demolitionsExist THEN
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
    UPDATE grid
    SET pop = grid.pop +
        (   COALESCE(grid.k_ap_ala, 0)::real / COALESCE(bo.erpien,38)::real +
            COALESCE(grid.k_ar_ala, 0)::real / COALESCE(bo.rivita,35.5)::real +
            COALESCE(grid.k_ak_ala, 0)::real / COALESCE(bo.askert,35)::real
        ) / COALESCE(km2hm2, 1.25)::real
    FROM built.occupancy bo
        WHERE bo.year = calculationYear
        AND bo.mun::int = grid.mun::int;

    UPDATE grid SET employ = grid.employ + COALESCE(grid.k_tp_yht,0);
    
    /*  KESKUSVERKON PÄIVITTÄMINEN
    Luodaan väliaikainen taso valtakunnallisesta keskusta-alueaineistosta
    Poistetaan ylimääräiset / virheelliset keskustat
    Muutetaan valtakunnallinen keskusta-alueaineisto keskipisteiksi (Point). */

    IF calculationYear = baseYear THEN
        /* Crop the national centroid data to cover only the nearest centers to AOI */
        /** TESTED OK **/
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

    /* Add new centers to the central network only if the user has added such! */
    /** TESTED OK */
    IF plan_centers IS NOT NULL THEN
        INSERT INTO centralnetwork
        SELECT (SELECT MAX(k.id) FROM centralnetwork k) + row_number() over (order by plan.geom desc),
            st_force2d((ST_DUMP(plan.geom)).geom) as geom,
            k_ktyyp AS keskustyyp,
            k_knimi AS keskusnimi
        FROM kv plan
        WHERE NOT EXISTS (
            SELECT 1
            FROM centralnetwork centers
            WHERE ST_DWithin(plan.geom, centers.geom, 1500)
        ) AND plan.k_ktyyp = 'Kaupunkiseudun iso alakeskus'
        AND ((COALESCE(plan.k_kvalmv,targetYear) + COALESCE(plan.k_kalkuv,baseYear))/2 <= calculationYear
            OR plan.k_kvalmv <= calculationYear);
    END IF;

    CREATE INDEX ON centralnetwork USING GIST (geom);

    /* Update closest distance to centroids into grid data. */
    /** TESTED OK */
    UPDATE grid
    SET centdist = sq2.centdist FROM
        (SELECT grid.xyind, center.centdist
        FROM grid
        CROSS JOIN LATERAL
            (SELECT ST_Distance(ST_CENTROID(centers.geom), grid.geom)/1000 AS centdist
                FROM centralnetwork centers
            WHERE centers.keskustyyp != 'Kaupunkiseudun pieni alakeskus'
            ORDER BY grid.geom <#> centers.geom
        LIMIT 1) AS center) as sq2
    WHERE grid.xyind = sq2.xyind;

    /* YHDYSKUNTARAKENTEEN VYÖHYKKEIDEN PÄIVITTÄMINEN */
    -- Keskustan jalankulkuvyöhyke
    /** Tested OK */
    CREATE TEMP TABLE IF NOT EXISTS grid_new AS
    SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
    CASE WHEN left(grid.zone::varchar,6)::int 
		IN (999112, 999212, 999312, 999412, 999512, 999612, 999101, 999811, 999821, 999831, 999841, 999851, 999861, 999871)
			THEN concat('99911',right(grid.zone::varchar,4))::bigint
    WHEN left(grid.zone::varchar,6)::int
		IN (999122, 999222, 999322, 999422, 999522, 999622, 999102, 999812, 999822, 999832, 999842, 999852, 999862, 999872)
			THEN concat('99912',right(grid.zone::varchar,4))::bigint
    ELSE 1 END AS zone
    FROM grid
    /* Search for grid cells within current UZ central areas delineation */
    /* and those cells that touch the current centers - have to use d_within for fastest approximation, st_touches doesn't work due to false DE-9IM relations */
    WHERE (grid.zone = 1 OR LEFT(grid.zone::varchar, 6)::int IN (999112, 999122)) OR (grid.maa_ha != 0 AND
        st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM grid
                WHERE (grid.zone = 1 OR LEFT(grid.zone::varchar, 6)::int IN (999112, 999122))
            ), 25))
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
                        WHERE (grid.zone = 1 OR LEFT(grid.zone::varchar, 6)::int IN (999112, 999122))
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz1;
        
    
    /* Olemassaolevien alakeskusten reunojen kasvatus */
    /** Tested OK */
    INSERT INTO grid_new
    SELECT * FROM
    (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, 
    CASE WHEN grid.zone IN (3,4,5, 81, 82, 83 ,84 ,85 ,86 ,87 ) THEN 10
    WHEN left(grid.zone::varchar,6)::int 
		IN (999312, 999412, 999512, 999612, 999101, 999811, 999821, 999831, 999841, 999851, 999861, 999871)
			THEN concat('999101',right(grid.zone::varchar,4))::bigint
    WHEN left(grid.zone::varchar,6)::int
		IN (999322, 999422, 999522, 999622, 999102, 999812, 999822, 999832, 999842, 999852, 999862, 999872)
			THEN concat('999102',right(grid.zone::varchar,4))::bigint
    ELSE grid.zone END AS zone
    FROM grid, centralnetwork
    /* Search for grid cells within current UZ central areas delineation */
    WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new) AND (grid.zone IN (10,11,12,6,837101) OR LEFT(grid.zone::varchar, 6)::int
	    IN (999101, 999102, 999111, 999112, 999121, 999122, 999612, 999622)) OR (grid.maa_ha != 0
        AND st_dwithin(grid.geom, 
            (SELECT st_union(grid.geom)
                FROM grid
                WHERE (grid.zone IN (10,11,12,6,837101) OR LEFT(grid.zone::varchar, 6)::int IN (999101, 999102, 999111, 999112, 999121, 999122, 999612, 999622))
            ), 25)
        AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0))
        /* Select only edge neighbours, no corner touchers */
        /* we have to use a buffer + area based intersection trick due to topological errors */
        AND st_area(
            st_intersection(
                grid.geom,
                st_buffer(
                    (SELECT st_union(grid.geom)
                        FROM grid
                        WHERE (grid.zone = 1 OR LEFT(grid.zone::varchar, 6)::int IN (999112, 999122))
                    ), 1)
            )) > 1
        AND (0.014028 * grid.pop + 0.821276 * grid.employ -3.67) > 10) uz10;
    
    INSERT INTO grid_new
    SELECT * FROM
        (SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        CASE WHEN grid.zone IN (3,4,5, 81, 82, 83 ,84 ,85 ,86 ,87 ) THEN 10
            WHEN left(grid.zone::varchar,6)::int 
                IN (999312, 999412, 999512, 999612, 999101, 999811, 999821, 999831, 999841, 999851, 999861, 999871)
                    THEN concat('999101',right(grid.zone::varchar,4))::bigint
            WHEN left(grid.zone::varchar,6)::int
                IN (999322, 999422, 999522, 999622, 999102, 999812, 999822, 999832, 999842, 999852, 999862, 999872)
                    THEN concat('999102',right(grid.zone::varchar,4))::bigint
            ELSE grid.zone END AS zone
        FROM grid, centralnetwork
        WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new) AND grid.maa_ha != 0 
        AND (st_dwithin(grid.geom, centralnetwork.geom, 250) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus')
        OR (st_dwithin(grid.geom, centralnetwork.geom, 500) AND centralnetwork.keskustyyp = 'Kaupunkiseudun iso alakeskus'
            AND (grid.alueteho > 0.05 AND grid.employ > 0)
        AND (grid.alueteho > 0.2 AND grid.pop >= 100 AND grid.employ > 0))
        ) uz10new;

    CREATE INDEX ON grid_new USING GIST (geom);

    /* Erityistapaukset */
    /** Tested OK */
    UPDATE grid_new SET zone = 6 WHERE grid_new.zone IN (837101, 10) AND st_dwithin(grid_new.geom,
            (SELECT centralnetwork.geom FROM centralnetwork WHERE centralnetwork.keskusnimi = 'Hervanta'), 2000);

    /* Keskustan reunavyöhykkeet */
    /** Tested OK */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        CASE WHEN grid.zone IN (3, 4, 5, 81, 82, 83 ,84 ,85 ,86 ,87 ) THEN 2
		WHEN left(grid.zone::varchar,6)::int
		IN (999112, 999212, 999312, 999412, 999512, 999612, 999101, 999811, 999821, 999831, 999841, 999851, 999861, 999871)
			THEN concat('99921',right(grid.zone::varchar,4))::bigint
		WHEN left(grid.zone::varchar,6)::int
		IN (999122, 999222, 999322, 999422, 999522, 999622, 999102, 999812, 999822, 999832, 999842, 999852, 999862, 999872)
			THEN concat('99922',right(grid.zone::varchar,4))::bigint
        ELSE grid.zone END AS zone
        FROM grid, grid_new
        WHERE
            grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new) AND st_dwithin(st_centroid(grid.geom), grid_new.geom,1000)
            AND ((grid_new.zone = 1 OR left(grid_new.zone::varchar,6)::int IN (999112, 999122))
            AND (grid.maa_ha/6.25 > 0.1 OR grid.pop > 0 OR grid.employ > 0))
		    OR grid.zone = 2;
    
    /* JOUKKOLIIKENNEVYÖHYKKEET */
    /* Lasketaan ensin joli-vyöhykkeiden määrittelyyn väestön ja työpaikkojen naapuruussummat (k=9). */
    /** Tested OK */
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
    /** Tested OK */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, grid.zone
        FROM grid WHERE (grid.zone = 3
            OR LEFT(grid.zone::varchar, 3) = '999' AND LEFT(RIGHT(grid.zone::varchar, 5),1)::int IN (1, 2))
        AND grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new);

    /** Pitää miettiä miten bussien ja päällekkäisten kohteiden kanssa toimitaan */
    IF plan_transit IS NOT NULL THEN
        INSERT INTO grid_new
            SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
            CONCAT('999',
                CASE WHEN grid.zone IN (1,2,3,4,5,6) THEN grid.zone::varchar
                    WHEN grid.zone IN (12,41,81) THEN '3'
                    WHEN grid.zone IN (11,40,82) THEN '4'
                    WHEN grid.zone IN (83,84,85,86,87)  THEN '5'
                    WHEN grid.zone IN (10,11,12) THEN '0'
                    END,
                COALESCE(pubtrans.k_jltyyp::varchar,'1'), -- 1 = 'juna'
                pubtrans.k_liikv::varchar
            )::bigint AS zone
            FROM grid, pubtrans
            /* Only those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND pubtrans.k_jltyyp::int = 1
            AND st_dwithin(grid.geom, pubtrans.geom, 1000) 
            AND pubtrans.k_liikv <= calculationYear
            ORDER BY grid.geom, st_distance(grid.geom, pubtrans.geom);

        INSERT INTO grid_new
            SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
            CONCAT('999',
                CASE WHEN grid.zone IN (1,2,3,4,5,6) THEN grid.zone::varchar
                    WHEN grid.zone IN (12,41,81) THEN '3'
                    WHEN grid.zone IN (11,40,82) THEN '4'
                    WHEN grid.zone IN (83,84,85,86,87)  THEN '5'
                    WHEN grid.zone IN (10,11,12) THEN '0' END,
                COALESCE(pubtrans.k_jltyyp::varchar, '2'), -- 2 = 'ratikka'
                pubtrans.k_liikv::varchar
            )::bigint AS zone
            FROM grid, pubtrans
            /* Only those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND pubtrans.k_jltyyp::int = 2
            AND st_dwithin(grid.geom, pubtrans.geom, 800) 
            AND pubtrans.k_liikv <= calculationYear
            ORDER BY grid.geom, st_distance(grid.geom, pubtrans.geom);
        
        INSERT INTO grid_new
            SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
            CONCAT('999',
                CASE WHEN grid.zone IN (1,2,3,4,5,6) THEN grid.zone::varchar
                    WHEN grid.zone IN (12,41,81) THEN '3'
                    WHEN grid.zone IN (11,40,82) THEN '4'
                    WHEN grid.zone IN (83,84,85,86,87)  THEN '5'
                    WHEN grid.zone IN (10,11,12) THEN '0' END,
                COALESCE(pubtrans.k_jltyyp::varchar,'3'), -- 3 = 'bussi'
                pubtrans.k_liikv::varchar
            )::bigint AS zone
            FROM grid, pubtrans
            /* Only those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND pubtrans.k_jltyyp::int = 3
            AND st_dwithin(grid.geom, pubtrans.geom, 400) 
            AND pubtrans.k_liikv <= calculationYear
            ORDER BY grid.geom, st_distance(grid.geom, pubtrans.geom);

    END IF;

    -- Päivitetään joukkoliikennevyöhykkeet aiemmin muodostettujen uusien keskustojen/alakeskusten osalta
    /** Tested OK */
    IF plan_transit IS NOT NULL THEN
        UPDATE grid_new
            SET zone =
            CONCAT('999',
                CASE WHEN grid_new.zone IN (1,2,6) THEN grid_new.zone::varchar
                    WHEN grid_new.zone = 12 THEN '3'
                    WHEN grid_new.zone = 11 THEN '4'
                    WHEN grid_new.zone = 10 THEN '0' END,
                coalesce(pubtrans.k_jltyyp::varchar,'3'), -- 1 = 'juna', 2 = 'raitiotie, 3 = 'bussi',
                pubtrans.k_liikv::varchar
            )::bigint
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
    /* Tested OK */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        3 AS zone
        FROM grid
            /* Only select those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND (grid.zone = 3 OR
            grid.pop_nn > 797 AND grid.employ_nn > 280);

    /* Joukkoliikennevyöhykkeet - nykyisten kasvatus ja uudet muualle syntyvät vyöhykkeet*/
    /* Tested OK */
    INSERT INTO grid_new
        SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind,
        4 AS zone
        FROM grid
            /* Only select those that are not already something else */
            WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new)
            AND (grid.zone = 4 OR (grid.pop_nn > 404 AND grid.employ_nn > 63));

    /* Poistetaan yksinäiset ruudut */
    /** Tested OK */
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
    SELECT DISTINCT ON (grid.geom) grid.geom, grid.xyind, grid.zone
        FROM grid
        /* Only select those that are not already something else */
        WHERE grid.xyind NOT IN (SELECT grid_new.xyind FROM grid_new);
        --AND grid.maa_ha > 0 AND (grid.pop > 0 OR grid.employ > 0);

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