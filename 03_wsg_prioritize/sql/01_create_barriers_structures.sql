-- create barrier table for splitting streams
-- 1. dams
-- 2. road / stream xings (CBS)

-- --------------------------------
-- create table
-- --------------------------------
DROP TABLE IF EXISTS cwf.barriers;
CREATE TABLE cwf.barriers
(
    barrier_id serial primary key,
    source_id integer,
    barrier_type text,
    barrier_name text,
    linear_feature_id integer,
    blue_line_key integer,
    downstream_route_measure double precision,
    wscode_ltree ltree,
    localcode_ltree ltree,
    watershed_group_code text,
    geom geometry(Point, 3005),
    -- add a unique constraint so that we don't have equivalent barriers messing up subsequent joins
    UNIQUE (linear_feature_id, downstream_route_measure)
);

-- --------------------------------
-- insert dams first so they over-ride any other barrier at same location
-- --------------------------------
INSERT INTO cwf.barriers
(
    source_id,
    barrier_type,
    barrier_name,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    watershed_group_code,
    geom
)
SELECT
    dam_id,
    'DAM' as barrier_type,
    d.dam_name as barrier_name,
    d.linear_feature_id,
    d.blue_line_key,
    d.downstream_route_measure,
    d.wscode_ltree,
    d.localcode_ltree,
    d.watershed_group_code,
    ST_Force2D((st_Dump(d.geom)).geom)
FROM cwf.dams d
INNER JOIN cwf.segmented_streams s
ON d.linear_feature_id = s.linear_feature_id
-- join on linear_feature_id but make sure the point is matched to
-- a single (correct) segment by comparing measures
AND d.downstream_route_measure + .001 > s.downstream_route_measure
AND d.downstream_route_measure - .001 < s.upstream_route_measure
WHERE d.barrier_ind != 'N'
-- we have to ignore points on side channels for this exercise
AND d.blue_line_key = s.watershed_key
AND (s.downstream_barrier_id_15 IS NULL OR s.downstream_barrier_id_20 IS NULL)
ORDER BY dam_id, s.downstream_route_measure
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert culverts
-- --------------------------------
INSERT INTO cwf.barriers
(
    source_id,
    barrier_type,
    barrier_name,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    watershed_group_code,
    geom
)
SELECT
    crossing_id,
    'CBS' as barrier_type,
    road_name_full as barrier_name,
    b.linear_feature_id,
    b.blue_line_key,
    b.downstream_route_measure,
    b.wscode_ltree,
    b.localcode_ltree,
    b.watershed_group_code,
    ST_Force2D((st_Dump(b.geom)).geom)
FROM fish_passage.road_stream_crossings_culverts b
INNER JOIN cwf.segmented_streams s
ON b.linear_feature_id = s.linear_feature_id
-- join on linear_feature_id but make sure the point is matched to
-- a single (correct) segment by comparing measures
AND b.downstream_route_measure + .001 > s.downstream_route_measure
AND b.downstream_route_measure - .001 < s.upstream_route_measure
AND (s.downstream_barrier_id_15 IS NULL OR s.downstream_barrier_id_20 IS NULL)
WHERE b.blue_line_key = s.watershed_key
-- don't include crossings that have been determined to be open bottom/non-existent
AND crossing_id NOT IN (SELECT source_id FROM cwf.modelled_culverts_qa)
-- don't include crossings on >= 6th order streams, these won't be culverts
-- *EXCEPT* for this one 6th order stream under hwy 97C by Logan Lake
AND (s.stream_order < 6 OR crossing_id = 6201511)
ORDER BY crossing_id, s.downstream_route_measure
ON CONFLICT DO NOTHING;


-- --------------------------------
-- index for speed
-- --------------------------------
CREATE INDEX ON cwf.barriers (linear_feature_id);
CREATE INDEX ON cwf.barriers (blue_line_key);
CREATE INDEX ON cwf.barriers (watershed_group_code);
CREATE INDEX ON cwf.barriers USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers USING GIST (geom);

-- create temp table for loading downstream barrier ids
DROP TABLE IF EXISTS cwf.barriers_temp;
CREATE TABLE cwf.barriers_temp
(
    barrier_id integer primary key,
    source_id integer,
    barrier_type text,
    barrier_name text,
    linear_feature_id integer,
    blue_line_key integer,
    downstream_route_measure double precision,
    wscode_ltree ltree,
    localcode_ltree ltree,
    watershed_group_code text,
    downstream_ids integer[],
    geom geometry(Point, 3005),
    UNIQUE (linear_feature_id, downstream_route_measure)
);