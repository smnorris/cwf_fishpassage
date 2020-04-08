---------------------------------------------------------------
-- shorten existing features
---------------------------------------------------------------
WITH min_segs AS (
  SELECT DISTINCT ON (linear_feature_id)
    linear_feature_id,
    downstream_route_measure
  FROM
    temp_streams
  ORDER BY
    linear_feature_id,
    downstream_route_measure ASC)

UPDATE
  cwf.{table} s
SET
  upstream_route_measure = m.downstream_route_measure,
  length_metre = ST_Length (ST_LocateBetween (s.geom,
    s.downstream_route_measure, m.downstream_route_measure)),
  geom = ST_LocateBetween (s.geom, s.downstream_route_measure, m.downstream_route_measure)
FROM
  min_segs m
WHERE
  m.linear_feature_id = s.linear_feature_id;
