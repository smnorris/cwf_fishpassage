-- combine all existing gradient barrier tables into a single table
-- Existing tables:
--  - only have unique ids within ws groups
--  - do not include linear_feature_id
-- This should be much easier to work with over short term

DROP TABLE IF EXISTS cwf.gradient_barriers;

CREATE TABLE cwf.gradient_barriers AS

WITH

gb03 AS
(
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_030_id)
        a.gradient_barrier_030_id ,
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
    FROM fish_passage.gradient_barriers_030 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_030_id, b.downstream_route_measure DESC
),

gb05 AS
(
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_050_id)
        a.gradient_barrier_050_id ,
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
    FROM fish_passage.gradient_barriers_050 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_050_id, b.downstream_route_measure DESC
),

gb08 AS
(
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_080_id)
        a.gradient_barrier_080_id ,
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
    FROM fish_passage.gradient_barriers_080 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_080_id, b.downstream_route_measure DESC
),

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
    FROM fish_passage.gradient_barriers_150 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_150_id, b.downstream_route_measure DESC
),

gb22 AS
    (
    SELECT DISTINCT ON (a.watershed_group_code, a.gradient_barrier_220_id)
        a.gradient_barrier_220_id,
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
    FROM fish_passage.gradient_barriers_220 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_220_id, b.downstream_route_measure DESC
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
    FROM fish_passage.gradient_barriers_300 a
    INNER JOIN whse_basemapping.fwa_stream_networks_sp b
    ON a.blue_line_key = b.blue_line_key
    AND a.downstream_route_measure >= b.downstream_route_measure
    ORDER BY a.watershed_group_code, a.gradient_barrier_300_id, b.downstream_route_measure DESC
),

gb_all AS
(
    SELECT * from gb03
    UNION ALL
    SELECT * FROM gb05
    UNION ALL
    SELECT * FROM gb08
    UNION ALL
    SELECT * from gb15
    UNION ALL
    SELECT * FROM gb22
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