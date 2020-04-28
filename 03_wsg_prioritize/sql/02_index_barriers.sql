-- --------------------------------
-- Populate the downstream_ids column
-- Run group by group to speed things up
-- --------------------------------
INSERT INTO cwf.barriers_temp

SELECT
    barrier_id,
    source_id,
    barrier_type,
    barrier_name,
    linear_feature_id,
    blue_line_key,
    downstream_route_measure,
    wscode_ltree,
    localcode_ltree,
    watershed_group_code,
    string_agg(downstream_id::text, ';') as downstream_ids,
    geom
FROM
(
  SELECT
    a.barrier_id,
    a.source_id,
    a.barrier_type,
    a.barrier_name,
    a.linear_feature_id,
    a.blue_line_key,
    a.downstream_route_measure,
    a.wscode_ltree,
    a.localcode_ltree,
    a.watershed_group_code,
    a.geom,
    b.barrier_id as downstream_id
  FROM
    cwf.barriers a
    LEFT OUTER JOIN cwf.barriers b ON
  -- only consider barriers within the same group.
  a.watershed_group_code = b.watershed_group_code
    AND
  -- b is downstream of a IF :
  -- criteria 1 - b is same blue line, with lower measure
  (a.blue_line_key = b.blue_line_key AND a.downstream_route_measure >
    b.downstream_route_measure) OR
    -- criteria 2 - a watershed code is a descendant of b watershed code
    (a.wscode_ltree <@ b.wscode_ltree AND (
    -- AND localcode of a is bigger than localcode of b at given level
    subltree (a.localcode_ltree,
    0,
    nlevel (b.localcode_ltree)) > b.localcode_ltree
    -- OR, where b's wscode and localcode are equivalent
    -- (ie, at bottom segment of a given watershed code)
    -- but excluding records in a and b on same stream
    OR (b.wscode_ltree = b.localcode_ltree AND a.wscode_ltree != b.wscode_ltree)
    -- OR any missed side channels on the same watershed code
    OR (a.wscode_ltree = b.wscode_ltree AND a.blue_line_key != b.blue_line_key
      AND a.localcode_ltree > b.localcode_ltree)))
  WHERE a.watershed_group_code = %s
  ORDER BY
    a.barrier_id,
    b.wscode_ltree DESC,
    b.localcode_ltree DESC,
    b.downstream_route_measure DESC
) AS z
GROUP BY barrier_id,
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