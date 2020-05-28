-- create barrier table for splitting streams
-- 1. gradient barriers >30pct
-- 2. hydro dams
-- 3. subsurface flow
--    (although the streams don't need to be split at these points, we do want them in the barrier table)

-- --------------------------------
-- create table
-- --------------------------------
DROP TABLE IF EXISTS cwf.barriers_30;
CREATE TABLE cwf.barriers_30
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
INSERT INTO cwf.barriers_30
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
AND d.watershed_group_code IN ('CHWK','FRCN','HARR','LFRA','LILL');
-- --------------------------------
-- insert gradient barriers
-- --------------------------------
INSERT INTO cwf.barriers_30
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
WHERE b.threshold = .30
AND b.watershed_group_code IN ('CHWK','FRCN','HARR','LFRA','LILL')
-- spot manual QA, remove gradients created by dams
AND b.linear_feature_id NOT IN (4035444)
ORDER BY blue_line_key, round(downstream_route_measure::numeric, 2)
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert subsurface flow
-- --------------------------------
INSERT INTO cwf.barriers_30
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
AND s.watershed_group_code IN ('CHWK','FRCN','HARR','LFRA','LILL')
ON CONFLICT DO NOTHING;

-- --------------------------------
-- index for speed
-- --------------------------------
CREATE INDEX ON cwf.barriers_30 (linear_feature_id);
CREATE INDEX ON cwf.barriers_30 (blue_line_key);
CREATE INDEX ON cwf.barriers_30 (watershed_group_code);
CREATE INDEX ON cwf.barriers_30 USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers_30 USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers_30 USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers_30 USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers_30 USING GIST (geom);

