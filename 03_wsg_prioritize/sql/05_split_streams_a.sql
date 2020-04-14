---------------------------------------------------------------
-- create a temp table where we segment streams at barriers
---------------------------------------------------------------
CREATE TEMPORARY TABLE temp_streams AS

-- find streams to break by joining streams to the min pts
WITH to_break AS (
SELECT
  s.linear_feature_id,
  s.downstream_route_measure AS meas_stream_ds,
  s.upstream_route_measure AS meas_stream_us,
  b.downstream_route_measure AS meas_event
FROM
  cwf.{table} s
  INNER JOIN cwf.barriers b ON s.linear_feature_id = b.linear_feature_id
  WHERE (b.downstream_route_measure - s.downstream_route_measure) > 1 AND
        (s.upstream_route_measure - b.downstream_route_measure) > 1 AND
        b.downstream_ids IS NULL
),

-- derive measures of new lines from break points
new_measures AS
(
  SELECT
    linear_feature_id,
    --meas_stream_ds,
    --meas_stream_us,
    meas_event AS downstream_route_measure,
    lead(meas_event, 1, meas_stream_us) OVER (PARTITION BY linear_feature_id
      ORDER BY meas_event) AS upstream_route_measure
  FROM
    to_break
)

-- create new geoms
SELECT
  row_number() OVER () AS id,
  s.linear_feature_id,
  n.downstream_route_measure,
  n.upstream_route_measure,
  (ST_Dump(ST_LocateBetween
    (s.geom, n.downstream_route_measure, n.upstream_route_measure
    ))).geom AS geom
FROM new_measures n
INNER JOIN cwf.{table} s ON n.linear_feature_id = s.linear_feature_id;
