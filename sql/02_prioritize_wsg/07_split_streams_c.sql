---------------------------------------------------------------
-- insert new features
---------------------------------------------------------------
INSERT INTO cwf.{table} (linear_feature_id, watershed_group_id,
  edge_type, blue_line_key, watershed_key, fwa_watershed_code,
  local_watershed_code, watershed_group_code, downstream_route_measure,
  length_metre, feature_source, gnis_id, gnis_name, left_right_tributary,
  stream_order, stream_magnitude, waterbody_key, blue_line_key_50k,
  watershed_code_50k, watershed_key_50k, watershed_group_code_50k, gradient,
  feature_code, wscode_ltree, localcode_ltree, upstream_route_measure, geom)
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
  ST_Length (t.geom) AS length_metre,
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
  s.gradient,
  s.feature_code,
  s.wscode_ltree,
  s.localcode_ltree,
  t.upstream_route_measure,
  t.geom
FROM
  temp_streams t
  INNER JOIN cwf.{table} s ON t.linear_feature_id = s.linear_feature_id;
