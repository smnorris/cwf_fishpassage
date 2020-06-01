-- Create table holding potential structure barriers
-- 1. dams
-- 2. road / stream xings (CBS)

-- --------------------------------
-- create table
-- --------------------------------
DROP TABLE IF EXISTS cwf.barriers_structures;
CREATE TABLE cwf.barriers_structures
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
INSERT INTO cwf.barriers_structures
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
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON d.linear_feature_id = s.linear_feature_id
WHERE d.barrier_ind != 'N'
-- we have to ignore points on side channels for this exercise
AND d.blue_line_key = s.watershed_key
ORDER BY dam_id
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert culverts
-- --------------------------------
INSERT INTO cwf.barriers_structures
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
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON b.linear_feature_id = s.linear_feature_id
WHERE b.blue_line_key = s.watershed_key
-- don't include crossings that have been determined to be open bottom/non-existent
AND crossing_id NOT IN (SELECT source_id FROM cwf.modelled_culverts_qa)
-- don't include crossings on >= 6th order streams, these won't be culverts
-- *EXCEPT* for this one 6th order stream under hwy 97C by Logan Lake
AND (s.stream_order < 6 OR crossing_id = 6201511)
ORDER BY crossing_id
ON CONFLICT DO NOTHING;


-- --------------------------------
-- index for speed
-- --------------------------------
CREATE INDEX ON cwf.barriers_structures (linear_feature_id);
CREATE INDEX ON cwf.barriers_structures (blue_line_key);
CREATE INDEX ON cwf.barriers_structures (watershed_group_code);
CREATE INDEX ON cwf.barriers_structures USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers_structures USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers_structures USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers_structures USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers_structures USING GIST (geom);

