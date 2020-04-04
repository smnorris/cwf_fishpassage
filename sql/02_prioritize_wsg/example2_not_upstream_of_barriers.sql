DROP TABLE IF EXISTS scratch.upstream_test;

CREATE TABLE scratch.upstream_test AS

WITH min_pts AS
(
    SELECT
      a.id,
      a.blue_line_key,
      a.downstream_route_measure,
      a.wscode_ltree,
      a.localcode_ltree,
      a.linear_feature_id
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

),

upstream AS
(
  SELECT
    b.stream_id,
    b.geom
  FROM min_pts a
  INNER JOIN scratch.streams b
  ON
      -- b is a child of a, always
    b.wscode_ltree <@ a.wscode_ltree

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
                 a.downstream_route_measure < b.downstream_route_measure + .01)
            )
         THEN TRUE
         -- next, the more complicated case - where wscode and localcode are not equal
         WHEN
            a.wscode_ltree != a.localcode_ltree AND
            (
             -- higher up the blue line (plus fudge factor)
                (b.blue_line_key = a.blue_line_key AND
                 a.downstream_route_measure < b.downstream_route_measure + .01 )
                OR
             -- tributaries: b wscode > a localcode and b wscode is not a child of a localcode
                (b.wscode_ltree > a.localcode_ltree AND
                 NOT b.wscode_ltree <@ a.localcode_ltree)
                OR
             -- capture side channels: b is the same watershed code, with larger localcode
                (b.wscode_ltree = a.wscode_ltree
                 AND b.localcode_ltree > a.localcode_ltree)
            )
          THEN TRUE
      END
)

-- grab everything that is NOT upstream of a barrier
SELECT
  s.*
FROM scratch.streams s
LEFT OUTER JOIN upstream u
ON s.stream_id = u.stream_id
WHERE u.stream_id IS NULL