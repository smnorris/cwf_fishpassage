-- create a table listing watershed groups upstream of barriers
DROP TABLE IF EXISTS cwf.wsg_upstream_of_barriers;

CREATE TABLE cwf.wsg_upstream_of_barriers AS

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
  ORDER BY a.barrier_id, a.fish_obstacle_point_id, a.barrier_name, b.watershed_group_code
),


-- add everything upstream of the Chief Joseph on the Columbia system
-- (upstream of confluence with Okanagan)
columbia AS
(
  SELECT
    NULL::integer as barrier_id,
    NULL::integer as fish_obstacle_point_id,
    'Chief Joseph Dam' as barrier_name,
    watershed_group_code,
    count(linear_feature_id)
  FROM whse_basemapping.fwa_stream_networks_sp
  WHERE
  wscode_ltree <@ '300'::ltree AND
    (wscode_ltree = '300'::ltree OR
      (wscode_ltree > '300.432687'::ltree AND NOT wscode_ltree <@ '300.432687'::ltree)
    OR
      (wscode_ltree = '300'::ltree AND localcode_ltree >= '300.432687'::ltree)
    )
  GROUP BY watershed_group_code
  HAVING count(linear_feature_id) > 100
  ORDER BY watershed_group_code
),

-- combine the two sets, removing everything duplicated
combined AS
(
  SELECT * FROM unpassable_upstream_groups
  WHERE watershed_group_code NOT IN (SELECT watershed_group_code from columbia)
  UNION ALL
  SELECT * FROM columbia
  ORDER BY barrier_id, fish_obstacle_point_id, barrier_name, watershed_group_code
)


SELECT watershed_group_code, barrier_id,
  CASE
    WHEN fish_obstacle_point_id IS NOT NULL
     THEN barrier_name||' '||fish_obstacle_point_id::text
    ELSE barrier_name
  END AS barrier_name
FROM combined
ORDER BY watershed_group_code;