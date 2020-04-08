-- Insert stream data straight from the master table
-- include all streams:
-- - connected to network
-- - in BC
-- - not a side channel of unknown location
-- - in the watershed groups of interest

INSERT INTO cwf.{table}
 (linear_feature_id,
  watershed_group_id,
  edge_type,
  blue_line_key,
  watershed_key,
  fwa_watershed_code,
  local_watershed_code,
  watershed_group_code,
  downstream_route_measure,
  length_metre,
  feature_source,
  gnis_id,
  gnis_name,
  left_right_tributary,
  stream_order,
  stream_magnitude,
  waterbody_key,
  blue_line_key_50k,
  watershed_code_50k,
  watershed_key_50k,
  watershed_group_code_50k,
  gradient,
  feature_code,
  wscode_ltree,
  localcode_ltree,
  upstream_route_measure,
  geom)
SELECT
  linear_feature_id,
  watershed_group_id,
  edge_type,
  blue_line_key,
  watershed_key,
  fwa_watershed_code,
  local_watershed_code,
  watershed_group_code,
  downstream_route_measure,
  length_metre,
  feature_source,
  gnis_id,
  gnis_name,
  left_right_tributary,
  stream_order,
  stream_magnitude,
  waterbody_key,
  blue_line_key_50k,
  watershed_code_50k,
  watershed_key_50k,
  watershed_group_code_50k,
  gradient,
  feature_code,
  wscode_ltree,
  localcode_ltree,
  upstream_route_measure,
  geom
FROM
  whse_basemapping.fwa_stream_networks_sp s
WHERE
  s.fwa_watershed_code NOT LIKE '999%%'
  AND s.edge_type != 6010
  AND s.localcode_ltree IS NOT NULL
  AND s.watershed_group_code = %s
ORDER BY
  linear_feature_id;