/* Päästölaskennan tulosten aggregointi halutun kokoisille suunnittelualueille /
   Aggregation of emission calculation results to the planning zones of wanted size

   When the actual emission calculations have finished the emissions could be visualized for larger areas.
   This function can be used for the purpose.

   The emissions table should be the output table from the emissions calculation.

   The zoning table has to have the following columns geom geometry(MultiPolygon), kayttotarkoitus varchar, kaavamaarays_otsikko varchar, kaavamaarays_teksti varchar, nimi varchar. However, other fields than geom can have NULL values if not used in the visualizations created with the data returned from this function. The geom is transformed to EPSG:3067 projection that other parameter tables are assumed to have for geometries.

   If the ykr_v table is provided then also population data is aggregated to the result table returned by this function.

   Base year and target year can be equal if the emission calculation was for a specific year.

   Esimerkkikysely / example query:
     SELECT * FROM il_emissions_to_planning_zones('emissions_calc_output', 'statistics_area', 2019, 2050, 'YKR_vaesto_2018_Pirkanmaa');
*/

DROP FUNCTION IF EXISTS public.il_emissions_to_planning_zones;
CREATE OR REPLACE FUNCTION
public.il_emissions_to_planning_zones(
    emissions_table text, -- Päästölaskennan tulostaulu | Emissions table
    zoning_table text, -- Aluejako päästöjen aggregointia varten | Planning zones for emission aggregation,
    baseYear integer, -- Laskennan lähtövuosi | Emission calculation start year
    targetYear integer, -- Laskennan tavoitevuosi | Emission calculation finish year
    ykr_v text DEFAULT NULL -- YKR-väestödata | YKR population data
)
RETURNS TABLE (
    id bigint,
    geom geometry(MultiPolygon, 3067),
    kayttotarkoitus text,
    kaavamaarays_otsikko text,
    kaavamaarays_teksti text,
    nimi text,
    vuosi date,
    sum_tco2 bigint,
    sahko_sum_tco2 bigint,
    liikenne_sum_tco2 bigint,
    vesi_tco2 real,
    lammitys_tco2 real,
    jaahdytys_tco2 real,
    kiinteistosahko_tco2 real,
    sahko_kotitaloudet_tco2 real,
    sahko_palv_tco2 real,
    sahko_tv_tco2 real,
    hloliikenne_tco2 real,
    tvliikenne_tco2 real,
    palvliikenne_tco2 real,
    korjaussaneeraus_tco2 real,
    purkaminen_tco2 real,
    uudisrakentaminen_tco2 real,
    vaesto_sum bigint,
    ruutu_sum bigint
) AS $$
DECLARE
    laskentavuodet integer[];
    vuosi integer;
BEGIN

IF emissions_table IS NULL THEN
    RAISE EXCEPTION 'Failed attempt to aggregate emissions - no emssions table given.';
END IF;

IF zoning_table IS NULL THEN
    RAISE EXCEPTION 'Failed attempt to aggregate emissions - no planning zones table given.';
END IF;

DROP SEQUENCE IF EXISTS zoning_emissions_temp_seq;
CREATE TEMP SEQUENCE zoning_emissions_temp_seq;

DROP TABLE IF EXISTS emissions_temp_table;
EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS emissions_temp_table AS
    SELECT * FROM ' || quote_ident(emissions_table) ||' emissions_table';

CREATE INDEX ON emissions_temp_table USING GIST (geom);

ALTER TABLE emissions_temp_table
    ADD COLUMN IF NOT EXISTS vaesto_sum bigint DEFAULT(0);

IF ykr_v IS NOT NULL THEN
    EXECUTE 'UPDATE emissions_temp_table SET vaesto_sum = (SELECT v_yht FROM ' || quote_ident(ykr_v) || ' AS vaesto WHERE emissions_temp_table.xyind = vaesto.xyind)';
END IF;


SELECT array(SELECT generate_series(baseYear,targetYear)) INTO laskentavuodet;

FOREACH vuosi in ARRAY laskentavuodet
LOOP

    IF vuosi = baseYear THEN
        DROP TABLE IF EXISTS zoning_emissions_temp_table;
        EXECUTE 'CREATE TEMP TABLE IF NOT EXISTS zoning_emissions_temp_table AS
            SELECT nextval(''zoning_emissions_temp_seq'') AS id, zoning_table.geom, zoning_table.kayttotarkoitus::text, zoning_table.kaavamaarays_otsikko::text, zoning_table.kaavamaarays_teksti::text, zoning_table.nimi::text, to_date(' || vuosi || '::text, ''YYYY'') AS vuosi FROM ' || quote_ident(zoning_table) || ' zoning_table';

        ALTER TABLE zoning_emissions_temp_table
            ADD COLUMN IF NOT EXISTS sum_tco2 bigint DEFAULT(0),
            ADD COLUMN IF NOT EXISTS sahko_sum_tco2 bigint DEFAULT(0),
            ADD COLUMN IF NOT EXISTS liikenne_sum_tco2 bigint DEFAULT(0),
            ADD COLUMN IF NOT EXISTS vesi_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS lammitys_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS jaahdytys_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS kiinteistosahko_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS sahko_kotitaloudet_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS sahko_palv_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS sahko_tv_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS hloliikenne_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS tvliikenne_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS palvliikenne_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS korjaussaneeraus_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS purkaminen_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS uudisrakentaminen_tco2 real DEFAULT(0),
            ADD COLUMN IF NOT EXISTS vaesto_sum bigint DEFAULT(0),
            ADD COLUMN IF NOT EXISTS ruutu_sum bigint DEFAULT(0),
            ALTER COLUMN geom TYPE geometry(MultiPolygon, 3067) USING ST_Transform(geom, 3067);

    ELSE 
        EXECUTE 'INSERT INTO zoning_emissions_temp_table(id, geom, kayttotarkoitus, kaavamaarays_otsikko, kaavamaarays_teksti, nimi, vuosi) SELECT nextval(''zoning_emissions_temp_seq'') AS id, zoning_table.geom, zoning_table.kayttotarkoitus, zoning_table.kaavamaarays_otsikko, zoning_table.kaavamaarays_teksti, zoning_table.nimi, to_date(' || vuosi || '::text, ''YYYY'') AS vuosi FROM ' || quote_ident(zoning_table) || ' zoning_table';
    END IF;
    
END LOOP;

CREATE INDEX ON zoning_emissions_temp_table USING GIST (geom);

UPDATE zoning_emissions_temp_table SET
    uudisrakentaminen_tco2 = uu_tco2,
    purkaminen_tco2 = pu_tco2,
    korjaussaneeraus_tco2 = ko_tco2,
    palvliikenne_tco2 = pa_tco2,
    tvliikenne_tco2 = tv_tco2,
    hloliikenne_tco2 = hl_tco2,
    sahko_tv_tco2 = st_tco2,
    sahko_palv_tco2 = sp_tco2,
    sahko_kotitaloudet_tco2 = sk_tco2,
    kiinteistosahko_tco2 = ki_tco2,
    jaahdytys_tco2 = ja_tco2,
    lammitys_tco2 = la_tco2,
    vesi_tco2 = ve_tco2,
    vaesto_sum = v_sum,
    ruutu_sum = r_sum
FROM (SELECT
    zoning_emissions_table.id AS id,
    COALESCE(SUM(emissions_temp_table.uudisrakentaminen_tco2), 0) AS uu_tco2,
    COALESCE(SUM(emissions_temp_table.purkaminen_tco2), 0) AS pu_tco2,
    COALESCE(SUM(emissions_temp_table.korjaussaneeraus_tco2), 0) AS ko_tco2,
    COALESCE(SUM(emissions_temp_table.palvliikenne_tco2), 0) AS pa_tco2,
    COALESCE(SUM(emissions_temp_table.tvliikenne_tco2), 0) AS tv_tco2,
    COALESCE(SUM(emissions_temp_table.hloliikenne_tco2), 0) AS hl_tco2,
    COALESCE(SUM(emissions_temp_table.sahko_tv_tco2), 0) AS st_tco2,
    COALESCE(SUM(emissions_temp_table.sahko_palv_tco2), 0) AS sp_tco2,
    COALESCE(SUM(emissions_temp_table.sahko_kotitaloudet_tco2), 0) AS sk_tco2,
    COALESCE(SUM(emissions_temp_table.kiinteistosahko_tco2), 0) AS ki_tco2,
    COALESCE(SUM(emissions_temp_table.jaahdytys_tco2), 0) AS ja_tco2,
    COALESCE(SUM(emissions_temp_table.lammitys_tco2), 0) AS la_tco2,
    COALESCE(SUM(emissions_temp_table.vesi_tco2), 0) AS ve_tco2,
    COALESCE(SUM(emissions_temp_table.vaesto_sum), 0) AS v_sum,
    COUNT(*) AS r_sum
    FROM emissions_temp_table, zoning_emissions_temp_table AS zoning_emissions_table
    WHERE ST_Intersects(zoning_emissions_table.geom, emissions_temp_table.geom) AND zoning_emissions_table.vuosi = to_date(emissions_temp_table.vuosi::text, 'YYYY')
    GROUP BY zoning_emissions_table.id
) AS co2_sums WHERE zoning_emissions_temp_table.id = co2_sums.id;

UPDATE zoning_emissions_temp_table AS a SET
    liikenne_sum_tco2 = (t.hloliikenne_tco2 + t.tvliikenne_tco2 + t.palvliikenne_tco2)::bigint FROM zoning_emissions_temp_table AS t WHERE a.id = t.id;
UPDATE zoning_emissions_temp_table AS a SET
    sahko_sum_tco2 = (t.kiinteistosahko_tco2 + t.sahko_kotitaloudet_tco2 + t.sahko_palv_tco2 + t.sahko_tv_tco2)::bigint FROM zoning_emissions_temp_table AS t WHERE a.id = t.id;
UPDATE zoning_emissions_temp_table AS a SET
    sum_tco2 = (t.uudisrakentaminen_tco2 + t.purkaminen_tco2 + t.korjaussaneeraus_tco2 + t.liikenne_sum_tco2 + t.sahko_sum_tco2 + t.jaahdytys_tco2 + t.lammitys_tco2 + t.vesi_tco2)::bigint FROM zoning_emissions_temp_table AS t WHERE a.id = t.id;

RETURN QUERY SELECT * FROM zoning_emissions_temp_table;

END;
$$ LANGUAGE plpgsql;
