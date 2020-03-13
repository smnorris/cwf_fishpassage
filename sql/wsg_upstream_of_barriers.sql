-- report on inaccessible watershed groups, upstream of barriers

WITH bottom_barriers AS

(
SELECT
  a.barrier_id,
  a.fish_obstacle_point_id,
  a.barrier_name,
  a.linear_feature_id,
  a.blue_line_key,
  a.localcode_ltree,
  a.wscode_ltree,
  a.downstream_route_measure,
  a.watershed_group_code
FROM cwf.barriers a
LEFT OUTER JOIN cwf.barriers b ON
  -- donwstream criteria 1 - same blue line, lower measure
  (b.blue_line_key = a.blue_line_key AND
   b.downstream_route_measure < a.downstream_route_measure)
  OR
  -- criteria 2 - watershed code a is a child of watershed code b
  (b.wscode_ltree @> a.wscode_ltree
      AND (
           -- AND local code is lower
           b.localcode_ltree < subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree))
           -- OR wscode and localcode are equivalent
           OR b.wscode_ltree = b.localcode_ltree
           -- OR any missed side channels on the same watershed code
           OR (b.wscode_ltree = a.wscode_ltree AND
               b.blue_line_key != a.blue_line_key AND
               b.localcode_ltree < a.localcode_ltree)
           )
  )
WHERE b.barrier_id IS NULL
),

unpassable_upstream_groups AS
(
  SELECT a.barrier_id, a.fish_obstacle_point_id, a.barrier_name, b.watershed_group_code, count(b.linear_feature_id)
  FROM bottom_barriers a
  LEFT OUTER JOIN whse_basemapping.fwa_stream_networks_sp b ON
      -- b is a child of a, always
      b.wscode_ltree <@ a.wscode_ltree
      -- never return the start segment, that is added at the end
    AND b.linear_feature_id != a.linear_feature_id
    AND
        -- conditional upstream join logic, based on whether watershed codes are equivalent
      CASE
        -- first, consider simple case - streams where wscode and localcode are equivalent
        -- this is all segments with equivalent bluelinekey and a larger measure
        -- (plus fudge factor)
         WHEN
            a.wscode_ltree = a.localcode_ltree AND
            (
                (b.blue_line_key <> a.blue_line_key OR
                 b.downstream_route_measure > a.downstream_route_measure + .01)
            )
         THEN TRUE
         -- next, the more complicated case - where wscode and localcode are not equal
         WHEN
            a.wscode_ltree != a.localcode_ltree AND
            (
             -- higher up the blue line (plus fudge factor)
                (b.blue_line_key = a.blue_line_key AND
                 b.downstream_route_measure > a.downstream_route_measure + .01)
                OR
             -- tributaries: b wscode > a localcode and b wscode is not a child of a localcode
                (b.wscode_ltree > a.localcode_ltree AND
                 NOT b.wscode_ltree <@ a.localcode_ltree)
                OR
             -- capture side channels: b is the same watershed code, with larger localcode
                (b.wscode_ltree = a.wscode_ltree
                 AND b.localcode_ltree >= a.localcode_ltree)
            )
          THEN TRUE
      END
  WHERE b.watershed_group_code != a.watershed_group_code
  GROUP BY a.barrier_id, a.fish_obstacle_point_id, a.barrier_name, b.watershed_group_code
  -- to avoid reporting on false positive matches where streams have the wrong watershed code,
  -- simply restrict result to upstream watershed group codes with > 100 matches
  HAVING count(b.linear_feature_id) > 100
)

SELECT
  barrier_id, fish_obstacle_point_id, barrier_name, array_agg(watershed_group_code) as groups
FROM unpassable_upstream_groups
GROUP BY barrier_id, fish_obstacle_point_id, barrier_name
ORDER BY barrier_id, fish_obstacle_point_id, barrier_name
