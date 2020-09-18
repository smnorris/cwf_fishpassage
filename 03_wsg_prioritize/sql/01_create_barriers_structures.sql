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
    source text,
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
    source,
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
    d.source_dataset as source,
    d.dam_id,
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
-- insert modelled culverts
-- --------------------------------
INSERT INTO cwf.barriers_structures
(
    source,
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
    'MODELLED_CROSSINGS_CLOSED_BOTTOM' as source,
    b.crossing_id,
    'MODELLED_CULVERT' as barrier_type,
    b.road_name_full as barrier_name,
    b.linear_feature_id,
    b.blue_line_key,
    b.downstream_route_measure,
    b.wscode_ltree,
    b.localcode_ltree,
    b.watershed_group_code,
    ST_Force2D((st_Dump(b.geom)).geom) as geom
FROM fish_passage.road_stream_crossings_culverts b
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON b.linear_feature_id = s.linear_feature_id
LEFT OUTER JOIN whse_fish.pscis_events p
ON b.crossing_id = p.model_crossing_id
WHERE b.blue_line_key = s.watershed_key
-- don't include crossings that have been determined to be open bottom/non-existent
AND crossing_id NOT IN (SELECT source_id FROM cwf.modelled_culverts_qa)
-- don't include crossings on >= 6th order streams, these won't be culverts
-- *EXCEPT* for this one 6th order stream under hwy 97C by Logan Lake
AND (s.stream_order < 6 OR crossing_id = 6201511)
-- don't include PSCIS crossings
AND p.stream_crossing_id IS NULL
-- just work with groups of interest for now.
AND b.watershed_group_code IN ('HORS','LNIC','BULK','ELKR')
ORDER BY crossing_id
ON CONFLICT DO NOTHING;

-- --------------------------------
-- insert PSCIS barriers
-- --------------------------------
INSERT INTO cwf.barriers_structures
(
    source,
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
    'PSCIS_'||e.pscis_status as source,
    e.stream_crossing_id as source_id,
    e.pscis_status as barrier_type,
    a.external_crossing_reference as barrier_name,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    watershed_group_code,
    e.geom
FROM whse_fish.pscis_events_sp e
LEFT OUTER JOIN whse_fish.pscis_points_all p
ON e.stream_crossing_id = p.stream_crossing_id
LEFT OUTER JOIN whse_fish.pscis_assessment_svw a
ON e.stream_crossing_id = a.stream_crossing_id
WHERE (e.current_barrier_result_code IN ('BARRIER', 'POTENTIAL')
-- there are a bunch of designs with no barrier result code
-- include them for now, they should be reviewed.
OR e.current_barrier_result_code IS NULL)
-- actually, only include PSCIS crossings within the watershed groups of interest for now
-- (there are some in the HARR group that fall on the fraser)
AND e.watershed_group_code IN ('HORS','LNIC','BULK','ELKR')
ORDER BY e.stream_crossing_id
ON CONFLICT DO NOTHING;



-- --------------------------------
-- index for speed
-- --------------------------------
CREATE INDEX ON cwf.barriers_structures (source_id);
CREATE INDEX ON cwf.barriers_structures (linear_feature_id);
CREATE INDEX ON cwf.barriers_structures (blue_line_key);
CREATE INDEX ON cwf.barriers_structures (watershed_group_code);
CREATE INDEX ON cwf.barriers_structures USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers_structures USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers_structures USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers_structures USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers_structures USING GIST (geom);

