CREATE OR REPLACE FUNCTION
public.il_preprocess(
    aoi text, -- Tutkimusalue | area of interest
    ykr_v text, -- YKR-väestödata | YKR population data
    ykr_tp text -- YKR-työpaikkadata | YKR workplace data
)
RETURNS TABLE (
    geom geometry,
    xyind varchar,
    vyoh integer,
    centdist integer,
    v_yht integer,
    tp_yht integer
) AS $$
DECLARE
    subquery varchar;
BEGIN

EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS ykr AS SELECT ykr.geom, ykr.xyind FROM aluejaot."YKR_perusruudukko" ykr WHERE ST_Intersects(ykr.geom, (SELECT rajaus.geom FROM ' || quote_ident(aoi) ||' rajaus))';

ALTER TABLE ykr
    ADD COLUMN IF NOT EXISTS vyoh integer,
    ADD COLUMN IF NOT EXISTS centdist integer,
    ADD COLUMN IF NOT EXISTS v_yht integer,
    ADD COLUMN IF NOT EXISTS tp_yht integer,
    ALTER COLUMN geom TYPE geometry(MultiPolygon, 3067) USING ST_Transform(geom, 3067);
CREATE INDEX ON ykr USING GIST (geom);
    
/* Liitetään UZ, väestö ja työpaikkatiedot */
UPDATE ykr SET vyoh = uz.vyoh FROM aluejaot."ykr_vyohykkeet" AS uz WHERE ST_WITHIN(ST_CENTROID(ykr.geom), uz.geom);
EXECUTE 'UPDATE ykr SET v_yht = v.v_yht FROM '|| quote_ident(ykr_v) ||' v WHERE v.xyind IN (SELECT ykr.xyind FROM ykr) AND v.xyind = ykr.xyind';
EXECUTE 'UPDATE ykr SET tp_yht = tp.tp_yht FROM '|| quote_ident(ykr_tp) ||' tp WHERE tp.xyind IN (SELECT ykr.xyind FROM ykr) AND tp.xyind = ykr.xyind';

/* Calculate distances to current centers */
UPDATE ykr SET centdist = sq.centdist FROM
    (SELECT ykr.xyind, keskusta.centdist
        FROM ykr
        CROSS JOIN LATERAL
        (SELECT ST_Distance(ST_CENTROID(keskustat.geom), ykr.geom)/1000
            AS centdist FROM aluejaot."KeskustaAlueet" keskustat
 	        WHERE keskustat.keskustyyp != 'Kaupunkiseudun pieni alakeskus'
            AND st_dwithin(keskustat.geom, ykr.geom, 50000) -- myöhempänä tarkempi suodatin, tässä rajoitettu nyt 50 km säteelle olevat keskustat.
            ORDER BY ykr.geom <#> keskustat.geom
            LIMIT 1
        ) AS keskusta) as sq
        WHERE ykr.xyind = sq.xyind;

/* Force YKR Urban zones into HLT-zone classification */
UPDATE ykr
SET vyoh = CASE WHEN ykr.vyoh IN (11,12) THEN 10 -- 'Alakeskuksen jalankulkuvyöhyke' 
	WHEN ykr.vyoh IN (40,41,42) THEN 2 -- 'Keskustan reunavyöhyke'
	WHEN ykr.vyoh IS NULL THEN 5 ELSE ykr.vyoh END; -- 'Autovyöhyke'
UPDATE ykr SET
    vyoh = 837101 WHERE st_dwithin(ykr.geom,(SELECT ST_centroid(keskusta.geom) FROM aluejaot."KeskustaAlueet" keskusta WHERE keskusta.keskusnimi = 'Hervanta'), 2000) AND ykr.vyoh = 10; --  'Alakeskuksen jalankulkuvyöhyke (Hervanta)'

RETURN QUERY SELECT * FROM ykr;
DROP TABLE ykr;

END;
$$ LANGUAGE plpgsql;