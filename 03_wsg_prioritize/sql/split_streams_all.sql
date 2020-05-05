---------------------------------------------------------------
-- create a temp table where we segment streams at barriers
---------------------------------------------------------------
--DROP TABLE IF EXISTS cwf.temp_streams;
--CREATE TABLE cwf.temp_streams AS

CREATE TEMPORARY TABLE temp_streams AS
-- find streams to break by joining streams to the min pts
WITH to_break AS (
SELECT
  s.segmented_stream_id,
  s.linear_feature_id,
  s.downstream_route_measure AS meas_stream_ds,
  s.upstream_route_measure AS meas_stream_us,
  b.downstream_route_measure AS meas_event
FROM
  cwf.segmented_streams s
  INNER JOIN cwf.barriers b ON s.linear_feature_id = b.linear_feature_id
  -- *Only break stream lines where the barrier pt is >1m from the end*
  -- Also, this restriction ensures we are matching the correct segment
  -- when there is more than 1 equivalent linear_feature_id (rather than
  -- selecting DISTINCT ON and ordering by measure) - because the difference
  -- between barrier and segment dnstr measure is positive and the difference
  -- between the segment upstr measure and the barrier is positive.
  WHERE (b.downstream_route_measure - s.downstream_route_measure) > 1 AND
        (s.upstream_route_measure - b.downstream_route_measure) > 1 AND
        b.downstream_ids IS NULL
),

-- derive measures of new lines from break points
new_measures AS
(
  SELECT
    segmented_stream_id,
    linear_feature_id,
    --meas_stream_ds,
    --meas_stream_us,
    meas_event AS downstream_route_measure,
    lead(meas_event, 1, meas_stream_us) OVER (PARTITION BY segmented_stream_id
      ORDER BY meas_event) AS upstream_route_measure
  FROM
    to_break
)

-- create new geoms
SELECT
  row_number() OVER () AS id,
  n.segmented_stream_id,
  s.linear_feature_id,
  n.downstream_route_measure,
  n.upstream_route_measure,
  (ST_Dump(ST_LocateBetween
    (s.geom, n.downstream_route_measure, n.upstream_route_measure
    ))).geom AS geom
FROM new_measures n
INNER JOIN cwf.segmented_streams s ON n.segmented_stream_id = s.segmented_stream_id;


---------------------------------------------------------------
-- shorten existing features
---------------------------------------------------------------
WITH min_segs AS
(
  SELECT DISTINCT ON (segmented_stream_id)
    segmented_stream_id,
    downstream_route_measure
  FROM
    temp_streams
--    cwf.temp_streams
  ORDER BY
    segmented_stream_id,
    downstream_route_measure ASC
),

shortened AS
(
SELECT
  m.segmented_stream_id,
  ST_Length(ST_LocateBetween(s.geom, s.downstream_route_measure, m.downstream_route_measure)) as length_metre,
  (ST_Dump(ST_LocateBetween (s.geom, s.downstream_route_measure, m.downstream_route_measure))).geom as geom
FROM min_segs m
INNER JOIN cwf.segmented_streams s
ON m.segmented_stream_id = s.segmented_stream_id

)

UPDATE
  cwf.segmented_streams a
SET
  length_metre = b.length_metre,
  geom = b.geom
FROM
  shortened b
WHERE
  b.segmented_stream_id = a.segmented_stream_id;



---------------------------------------------------------------
-- insert new features
---------------------------------------------------------------
INSERT INTO cwf.segmented_streams (linear_feature_id, watershed_group_id,
  edge_type, blue_line_key, watershed_key, fwa_watershed_code,
  local_watershed_code, watershed_group_code, downstream_route_measure,
  length_metre, feature_source, gnis_id, gnis_name, left_right_tributary,
  stream_order, stream_magnitude, waterbody_key, blue_line_key_50k,
  watershed_code_50k, watershed_key_50k, watershed_group_code_50k,
  feature_code, downstream_barrier_id_15, geom)
SELECT
  s.linear_feature_id,
  s.watershed_group_id,
  s.edge_type,
  s.blue_line_key,
  s.watershed_key,
  s.fwa_watershed_code,
  s.local_watershed_code,
  s.watershed_group_code,
  t.downstream_route_measure,
  ST_Length(t.geom) AS length_metre,
  s.feature_source,
  s.gnis_id,
  s.gnis_name,
  s.left_right_tributary,
  s.stream_order,
  s.stream_magnitude,
  s.waterbody_key,
  s.blue_line_key_50k,
  s.watershed_code_50k,
  s.watershed_key_50k,
  s.watershed_group_code_50k,
  s.feature_code,
  s.downstream_barrier_id_15,
  t.geom
FROM
 -- cwf.temp_streams t
  temp_streams t
INNER JOIN cwf.segmented_streams s ON t.segmented_stream_id = s.segmented_stream_id;
