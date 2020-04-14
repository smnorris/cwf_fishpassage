-- create barrier table for splitting streams
-- 1. gradient barriers >15pct
-- 2. large dams
-- 3. subsurface flow
--    (although the streams don't need to be split at these points, we do want them in the barrier table)

-- --------------------------------
-- create table
-- --------------------------------
DROP TABLE IF EXISTS cwf.barriers;
CREATE TABLE cwf.barriers
(
    barrier_id serial primary key,
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
    'DAM' as barrier_type,
    d.dam_name as barrier_name,
    d.linear_feature_id,
    d.blue_line_key,
    d.downstream_route_measure,
    d.wscode_ltree,
    d.localcode_ltree,
    d.watershed_group_code,
    ST_Force2D((st_Dump(d.geom)).geom)
FROM cwf.large_dams d
WHERE d.barrier_ind = 'Y';

-- --------------------------------
-- insert gradient barriers
-- --------------------------------
INSERT INTO cwf.barriers
(
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
    'GRADIENT' as barrier_type,
    threshold::text as barrier_name,
    b.linear_feature_id,
    b.blue_line_key,
    b.downstream_route_measure,
    b.wscode_ltree,
    b.localcode_ltree,
    b.watershed_group_code,
    ST_Force2D((st_Dump(b.geom)).geom)
FROM cwf.gradient_barriers b
WHERE b.threshold IN (.15,.22,.3)
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert subsurface flow
-- --------------------------------
INSERT INTO cwf.barriers
(
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
    'SUBSURFACE FLOW' as barrier_type,
    NULL as barrier_name,
    s.linear_feature_id,
    s.blue_line_key,
    s.downstream_route_measure,
    s.wscode_ltree,
    s.localcode_ltree,
    s.watershed_group_code,
    ST_LineInterpolatePoint(
        ST_Force2D(
            (ST_Dump(s.geom)).geom
        ),
        0
    ) as geom
FROM whse_basemapping.fwa_stream_networks_sp s
WHERE s.edge_type IN (1410, 1425)
AND s.local_watershed_code IS NOT NULL
AND s.blue_line_key = s.watershed_key
AND s.fwa_watershed_code NOT LIKE '999%%'
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
CREATE TABLE cwf.barriers_temp
(
    barrier_id integer primary key,
    barrier_type text,
    barrier_name text,
    linear_feature_id integer,
    blue_line_key integer,
    downstream_route_measure double precision,
    wscode_ltree ltree,
    localcode_ltree ltree,
    watershed_group_code text,
    downstream_ids text,
    geom geometry(Point, 3005),
    UNIQUE (linear_feature_id, downstream_route_measure)
);