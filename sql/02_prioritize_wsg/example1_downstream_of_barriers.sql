-- find everything downstream of >=15% grades

-- first, find minimum points

WITH min_pts AS
(
    SELECT
      a.id,
      a.blue_line_key,
      a.downstream_route_measure,
      a.wscode_ltree,
      a.localcode_ltree
    FROM scratch.gradient_barriers a
    LEFT OUTER JOIN scratch.gradient_barriers b ON

    -- b is downstream of a IF :

    -- criteria 1 - b is same blue line, with lower measure
    (
      a.blue_line_key = b.blue_line_key AND
      a.downstream_route_measure > b.downstream_route_measure
    )
    OR

    -- criteria 2 - a watershed code is a descendant of b watershed code
    (
      a.wscode_ltree <@ b.wscode_ltree
    AND
      (
           -- AND localcode of a is bigger than localcode of b at given level
           subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree)) > b.localcode_ltree

           -- OR, where b's wscode and localcode are equivalent
           -- (ie, at bottom segment of a given watershed code)
           -- but excluding records in a and b on same stream
           OR (
              b.wscode_ltree = b.localcode_ltree
              AND a.wscode_ltree != b.wscode_ltree
           )

           -- OR any missed side channels on the same watershed code
           OR (
                a.wscode_ltree = b.wscode_ltree AND
                a.blue_line_key != b.blue_line_key AND
                a.localcode_ltree > b.localcode_ltree
           )
      )
    )
    WHERE b.id IS NULL
    ORDER BY a.id, b.wscode_ltree desc, b.localcode_ltree desc, b.downstream_route_measure desc

)

-- now find everything downstream of the min pts
SELECT DISTINCT
  b.stream_id,
  b.geom
FROM min_pts a
INNER JOIN scratch.streams b
ON
 -- b is downstream of a IF :

    -- criteria 1 - b is same blue line, with lower measure
    (
      a.blue_line_key = b.blue_line_key AND
      a.downstream_route_measure > b.downstream_route_measure
    )
    OR

    -- criteria 2 - a watershed code is a descendant of b watershed code
    (
      a.wscode_ltree <@ b.wscode_ltree
    AND
      (
           -- AND localcode of a is bigger than localcode of b at given level
           subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree)) > b.localcode_ltree

           -- OR, where b's wscode and localcode are equivalent
           -- (ie, at bottom segment of a given watershed code)
           -- but excluding records in a and b on same stream
           OR (
              b.wscode_ltree = b.localcode_ltree
              AND a.wscode_ltree != b.wscode_ltree
           )

           -- OR any missed side channels on the same watershed code
           OR (
                a.wscode_ltree = b.wscode_ltree AND
                a.blue_line_key != b.blue_line_key AND
                a.localcode_ltree > b.localcode_ltree
           )
      )
    )

