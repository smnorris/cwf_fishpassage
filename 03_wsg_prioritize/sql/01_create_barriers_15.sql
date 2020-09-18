-- create barrier table for splitting streams
-- 1. gradient barriers >15pct
-- 2. hydro dams
-- 3. subsurface flow
--    (although the streams don't need to be split at these points, we do want them in the barrier table)
-- 4. Other known hard barriers (eg falls)

-- --------------------------------
-- create table
-- --------------------------------
DROP TABLE IF EXISTS cwf.barriers_15;
CREATE TABLE cwf.barriers_15
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
-- (TODO - it could be worth rounding the measures to the nearest m or cm to further
-- ensure unique locations, but that would require some logic to make sure the
-- rounding doesn't put the feature on the wrong line...)
-- --------------------------------
INSERT INTO cwf.barriers_15
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
WHERE d.barrier_ind = 'Y'
AND d.hydro_dam_ind = 'Y'
AND d.watershed_group_code IN
  (SELECT watershed_group_CODE
     FROM cwf.target_watershed_groups
    WHERE status = 'In'
  );

-- --------------------------------
-- insert gradient barriers
-- --------------------------------
INSERT INTO cwf.barriers_15
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
-- ensure that points are unique so that when splitting streams,
-- we don't generate zero length lines
SELECT DISTINCT ON (blue_line_key, round(downstream_route_measure::numeric, 2))
    gradient_barrier_id,
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
WHERE b.threshold IN (.15,.20,.30)
AND b.watershed_group_code IN
  (SELECT watershed_group_CODE
     FROM cwf.target_watershed_groups
    WHERE status = 'In'
  )
-- spot manual QA of gradient barriers
AND b.linear_feature_id NOT IN
  (4035444,   -- dam in BABL
   701934669) -- odd point on Salmon River that looks like a data error
ORDER BY blue_line_key, round(downstream_route_measure::numeric, 2)
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert subsurface flow
-- --------------------------------
INSERT INTO cwf.barriers_15
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
    linear_feature_id as source_id,
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
-- Do not include subsurface flows on the Chilcotin at the Clusco.
-- The subsurface flow is a side channel, the Chilcotin merges
-- with the Clusco farther upstream
AND NOT (s.blue_line_key = 356363411 AND s.downstream_route_measure < 213010)
AND s.watershed_group_code IN
  (SELECT watershed_group_CODE
     FROM cwf.target_watershed_groups
    WHERE status = 'In'
  )
ON CONFLICT DO NOTHING;

-- --------------------------------
-- Insert other natural barriers
-- --------------------------------
INSERT INTO cwf.barriers_15
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
    fish_obstacle_point_id,
   'FALLS' as barrier_type,
    NULL as barrier_name,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    watershed_group_code,
    geom
FROM whse_fish.fiss_falls_events_sp
-- these are the only barriers to insert - hard code them for now,
-- at some point we may want to maintain a lookup table
WHERE fish_obstacle_point_id IN (27481, 27482, 19653, 19565)
ON CONFLICT DO NOTHING;

-- --------------------------------
-- index for speed
-- --------------------------------
CREATE INDEX ON cwf.barriers_15 (linear_feature_id);
CREATE INDEX ON cwf.barriers_15 (blue_line_key);
CREATE INDEX ON cwf.barriers_15 (watershed_group_code);
CREATE INDEX ON cwf.barriers_15 USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers_15 USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers_15 USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers_15 USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers_15 USING GIST (geom);
