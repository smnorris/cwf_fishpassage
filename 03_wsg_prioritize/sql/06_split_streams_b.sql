---------------------------------------------------------------
-- shorten existing features
---------------------------------------------------------------
WITH min_segs AS
(
  SELECT DISTINCT ON (linear_feature_id)
    linear_feature_id,
    downstream_route_measure
  FROM
    temp_streams
  ORDER BY
    linear_feature_id,
    downstream_route_measure ASC
),

shortened AS
(
SELECT
  m.linear_feature_id,
  ST_Length(ST_LocateBetween(s.geom, s.downstream_route_measure, m.downstream_route_measure)) as length_metre,
  (ST_Dump(ST_LocateBetween (s.geom, s.downstream_route_measure, m.downstream_route_measure))).geom as geom
FROM min_segs m
INNER JOIN cwf.{table} s
ON m.linear_feature_id = s.linear_feature_id

)

UPDATE
  cwf.{table} a
SET
  length_metre = b.length_metre,
  geom = b.geom
FROM
  shortened b
WHERE
  b.linear_feature_id = a.linear_feature_id;
