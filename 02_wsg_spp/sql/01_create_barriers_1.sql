-- create empty table
DROP TABLE IF EXISTS cwf.barriers_1;

CREATE TABLE cwf.barriers_1
(
    barrier_id SERIAL PRIMARY KEY,
    fish_obstacle_point_id integer,
    barrier_name text,
    barrier_type text,
    linear_feature_id integer,
    blue_line_key integer,
    downstream_route_measure double precision,
    wscode_ltree ltree,
    localcode_ltree ltree,
    distance_to_stream double precision,
    watershed_group_code text,
    geom Geometry(Point, 3005)
);

INSERT INTO cwf.barriers_1
(
    fish_obstacle_point_id,
    barrier_name,
    barrier_type,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    distance_to_stream,
    watershed_group_code,
    geom
)

WITH src_pts AS

(
    SELECT
      fish_obstacle_point_id,
      NULL::int as dam_id,
      obstacle_name as barrier_name,
      'FALLS' as barrier_type,
      geom
    FROM whse_fish.fiss_obstacles_pnt_sp
    WHERE obstacle_name = 'Falls'
    AND height >= 5
    AND height <> 999
    AND height <> 9999
    -- do not include these falls which are:
    --  - not barriers to fish passage
    --  - do not get snapped to the correct stream and are insignificant for this analysis
    -- 27273 - Hells Gate, not a barrier
    -- 33254 - Spahats Creek falls that are closer to Clearwater River, safe to ignore
    AND fish_obstacle_point_id NOT IN (27273, 33254)

    UNION ALL

    SELECT
      NULL::int as fish_obstacle_point_id,
      row_number() over() as dam_id,
      dam_name as barrier_name,
      'DAM' as barrier_type,
      geom
    FROM cwf.large_dams
    WHERE barrier = 'Y'

),

nearest AS
(
  SELECT
    pt.fish_obstacle_point_id,
    pt.barrier_name,
    pt.barrier_type,
    str.linear_feature_id,
    str.wscode_ltree,
    str.localcode_ltree,
    str.blue_line_key,
    str.waterbody_key,
    str.length_metre,
    ST_Distance(str.geom, pt.geom) as distance_to_stream,
    str.watershed_group_code,
    str.downstream_route_measure as downstream_route_measure_str,
    (
      ST_LineLocatePoint(
        st_linemerge(str.geom),
        ST_ClosestPoint(str.geom, pt.geom)
      )
      * str.length_metre
  ) + str.downstream_route_measure AS downstream_route_measure,
  st_linemerge(str.geom) as geom_str
  FROM src_pts pt
  CROSS JOIN LATERAL
  (SELECT
     linear_feature_id,
     wscode_ltree,
     localcode_ltree,
     blue_line_key,
     waterbody_key,
     length_metre,
     downstream_route_measure,
     watershed_group_code,
     geom
    FROM whse_basemapping.fwa_stream_networks_sp str
    WHERE str.localcode_ltree IS NOT NULL
    AND NOT str.wscode_ltree <@ '999'
    ORDER BY str.geom <-> pt.geom
    LIMIT 1) as str
    WHERE ST_Distance(str.geom, pt.geom) <= 50
)

SELECT
    fish_obstacle_point_id,
    barrier_name,
    barrier_type,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    distance_to_stream,
    watershed_group_code,
    ST_Force2D(
      ST_LineInterpolatePoint(geom_str,
       ROUND(
         CAST(
            (downstream_route_measure -
               downstream_route_measure_str) / length_metre AS NUMERIC
          ),
         5)
       )
    )::geometry(Point, 3005) AS geom
FROM nearest;

CREATE INDEX ON cwf.barriers_1 (linear_feature_id);
CREATE INDEX ON cwf.barriers_1 (blue_line_key);
CREATE INDEX ON cwf.barriers_1 (watershed_group_code);
CREATE INDEX ON cwf.barriers_1 USING GIST (wscode_ltree);
CREATE INDEX ON cwf.barriers_1 USING BTREE (wscode_ltree);
CREATE INDEX ON cwf.barriers_1 USING GIST (localcode_ltree);
CREATE INDEX ON cwf.barriers_1 USING BTREE (localcode_ltree);
CREATE INDEX ON cwf.barriers_1 USING GIST (geom);
