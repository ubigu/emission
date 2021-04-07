drop table if exists delineations.centroids;
CREATE TABLE delineations.centroids AS
	SELECT centers.id, st_transform(st_centroid(centers.geom),3067) geom, centers.keskustyyp, centers.keskusnimi
    FROM delineations.centers centers
    WHERE keskustyyp != 'Kaupunkiseudun pieni alakeskus';
    
/* Päivitetään geometriat kaupunkiseutujen keskustojen osalta (ydinkeskusten keskipisteet saadaan tarkimmin Urban Zone-vyöhykeaineiston kävelykeskustojen keskipisteistä). */
update delinations.centroids centroids
SET geom = ST_CENTROID(gsq.geom)
FROM (SELECT (st_dump(st_union(g.geom))).geom AS geom FROM grid g WHERE g.zone = 1) gsq
WHERE ST_INTERSECTS(centroids.geom, gsq.geom);
CREATE INDEX ON delineations.centroids USING GIST (geom);