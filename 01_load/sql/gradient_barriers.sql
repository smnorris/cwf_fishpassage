-- combine all existing gradient barrier tables into a single table
-- Existing tables:
--  - only have unique ids within ws groups
--  - do not include linear_feature_id
-- This should be much easier to work with over short term

DROP TABLE IF EXISTS cwf.gradient_barriers;

CREATE TABLE cwf.gradient_barriers AS

WITH

gb15 AS
(
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_150_id)
        a.gradient_barrier_150_id ,
        b.linear_feature_id,
        a.blue_line_key           ,
        a.fwa_watershed_code      ,
        a.local_watershed_code    ,
        a.downstream_route_measure,
        a.upstream_route_measure  ,
        b.downstream_route_measure as stream_measure,
        a.watershed_group_code    ,
        a.length_metre            ,
        a.from_elevation          ,
        a.to_elevation            ,
        a.threshold               ,
        a.gradient                ,
        a.localcode_ltree         ,
        a.wscode_ltree
    FROM fish_passage_cwf_salmon.gradient_barriers_150 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_150_id, b.downstream_route_measure DESC
),

gb20 AS
    (
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_200_id)
        a.gradient_barrier_200_id,
        b.linear_feature_id,
        a.blue_line_key           ,
        a.fwa_watershed_code      ,
        a.local_watershed_code    ,
        a.downstream_route_measure,
        a.upstream_route_measure  ,
        b.downstream_route_measure as stream_measure,
        a.watershed_group_code    ,
        a.length_metre            ,
        a.from_elevation          ,
        a.to_elevation            ,
        a.threshold               ,
        a.gradient                ,
        a.localcode_ltree         ,
        a.wscode_ltree
    FROM fish_passage_cwf_salmon.gradient_barriers_200 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_200_id, b.downstream_route_measure DESC
),

gb30 AS
    (
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_300_id)
        a.gradient_barrier_300_id,
        b.linear_feature_id,
        a.blue_line_key           ,
        a.fwa_watershed_code      ,
        a.local_watershed_code    ,
        a.downstream_route_measure,
        a.upstream_route_measure  ,
        b.downstream_route_measure as stream_measure,
        a.watershed_group_code    ,
        a.length_metre            ,
        a.from_elevation          ,
        a.to_elevation            ,
        a.threshold               ,
        a.gradient                ,
        a.localcode_ltree         ,
        a.wscode_ltree
    FROM fish_passage_cwf_salmon.gradient_barriers_300 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_300_id, b.downstream_route_measure DESC
),

gb_all AS
(
    SELECT * from gb15
    UNION ALL
    SELECT * FROM gb20
    UNION ALL
    SELECT * FROM gb30
)


SELECT
    row_number() over() as gradient_barrier_id,
    gb.linear_feature_id,
    gb.blue_line_key           ,
    gb.fwa_watershed_code      ,
    gb.local_watershed_code    ,
    gb.downstream_route_measure,
    gb.upstream_route_measure  ,
    gb.watershed_group_code    ,
    gb.length_metre            ,
    gb.from_elevation          ,
    gb.to_elevation            ,
    gb.threshold               ,
    gb.gradient                ,
    gb.localcode_ltree         ,
    gb.wscode_ltree,
    st_locatealong(s.geom, gb.downstream_route_measure) as geom
FROM gb_all gb
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON gb.linear_feature_id = s.linear_feature_id;


CREATE INDEX ON cwf.gradient_barriers (linear_feature_id);
CREATE INDEX ON cwf.gradient_barriers (blue_line_key);
CREATE INDEX ON cwf.gradient_barriers (watershed_group_code);
CREATE INDEX ON cwf.gradient_barriers USING GIST (geom);
CREATE INDEX ON cwf.gradient_barriers USING GIST (wscode_ltree);
CREATE INDEX ON cwf.gradient_barriers USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.gradient_barriers USING GIST (localcode_ltree);
CREATE INDEX ON cwf.gradient_barriers USING BTREE (localcode_ltree);

ALTER TABLE cwf.gradient_barriers ALTER COLUMN geom SET DATA TYPE geometry(MultiPointZM, 3005);