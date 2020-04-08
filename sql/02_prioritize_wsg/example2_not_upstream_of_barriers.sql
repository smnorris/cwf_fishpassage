DROP TABLE IF EXISTS cwf.upstream_test;

CREATE TABLE cwf.upstream_test AS

WITH upstream AS
(
  SELECT
    b.segmented_stream_id,
    b.geom
  FROM cwf.barriers a
  INNER JOIN cwf.segmented_streams b
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
  AND a.downstream_ids IS NULL
)

-- grab everything that is NOT upstream of a barrier
SELECT
  s.*
FROM cwf.segmented_streams s
LEFT OUTER JOIN upstream u
ON s.segmented_stream_id = u.segmented_stream_id
WHERE u.segmented_stream_id IS NULL