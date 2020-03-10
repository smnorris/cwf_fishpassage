-- create empty table
DROP TABLE IF EXISTS cwf.barriers;

CREATE TABLE cwf.barriers
(
    barriers_id SERIAL PRIMARY KEY,
    fish_obstacle_point_id integer,
    obstruction_id integer,
    canvec_feature_id text,
    wwd_id integer,
    barrier_name text,
    barrier_type text,
    linear_feature_id integer,
    blue_line_key integer,
    downstream_route_measure double precision,
    wscode_ltree ltree,
    localcode_ltree ltree,
    distance_to_stream double precision,
    geom Geometry(Point, 3005)
);

-- ============================================================
-- insert falls from FWA obstructions, already matched to FWA streams
-- ============================================================
-- do not include FWA obstructions for now, they have no height values
/*INSERT INTO cwf.barriers
(
    obstruction_id,
    barrier_name,
    barrier_type,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    distance_to_stream,
    geom

)

SELECT
    obstruction_id,
    gnis_name as barrier_name,
    'FALLS' as barrier_type,
    linear_feature_id,
    blue_line_key,
    route_measure as downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    0 as distance_to_stream,
    (ST_Dump(geom)).geom
FROM whse_basemapping.fwa_obstructions_sp
WHERE obstruction_type = 'Falls';

*/

-- ============================================================
-- insert remaining falls and dams, matching to nearest stream
-- ============================================================
INSERT INTO cwf.barriers
(
    fish_obstacle_point_id,
    canvec_feature_id,
    wwd_id,
    barrier_name,
    barrier_type,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    distance_to_stream,
    geom
)

WITH src_pts AS

(
    SELECT
      fish_obstacle_point_id,
      NULL as canvec_feature_id,
      NULL::int as wwd_id,
      NULL::int as dam_id,
      obstacle_name as barrier_name,
      'FALLS' as barrier_type,
      geom
    FROM whse_fish.fiss_obstacles_pnt_sp
    WHERE obstacle_name = 'Falls'
    AND height >= 5
    AND height <> 999
    AND height <> 9999

    UNION ALL

/*
    -- do not include canvec or wwd falls for now, they have no height values
    SELECT
      NULL as fish_obstacle_point_id,
      feature_id as canvec_feature_id,
      NULL::int as wwd_id,
      NULL as dam_id,
      name_1_en as barrier_name,
      'FALLS' as barrier_type,
      geom
    FROM cwf.falls
    WHERE dataset_nm = 'CanVec'

    UNION ALL

    SELECT
      NULL as fish_obstacle_point_id,
      NULL as canvec_feature_id,
      row_number() over() as wwd_id,
      NULL as dam_id,
      name as barrier_name,
      'FALLS' as barrier_type,
      geom
    FROM cwf.falls
    WHERE dataset_nm = 'WWD'

    UNION ALL
*/

    SELECT
      NULL as fish_obstacle_point_id,
      NULL as canvec_feature_id,
      NULL as wwd_id,
      row_number() over() as dam_id,
      name as barrier_name,
      'DAM' as barrier_type,
      geom
    FROM cwf.dams

),

nearest AS
(
  SELECT
    pt.fish_obstacle_point_id,
    pt.canvec_feature_id,
    pt.wwd_id,
    pt.barrier_name,
    pt.barrier_type,
    str.linear_feature_id,
    str.wscode_ltree,
    str.localcode_ltree,
    str.blue_line_key,
    str.waterbody_key,
    str.length_metre,
    ST_Distance(str.geom, pt.geom) as distance_to_stream,
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
    canvec_feature_id,
    wwd_id,
    barrier_name,
    barrier_type,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    distance_to_stream,
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


