-- this takes ~6min for the ~140 groups of interest
-- leave as single update for now (recreating indexes may take almost as long)
UPDATE cwf.{table}
SET {downstream_id} = b.barrier_id
FROM
(SELECT
  b.segmented_stream_id,
  a.barrier_id
FROM cwf.barriers a
INNER JOIN cwf.{table} b
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
WHERE a.downstream_ids IS NULL) b
WHERE segmented_streams.segmented_stream_id = b.segmented_stream_id;
