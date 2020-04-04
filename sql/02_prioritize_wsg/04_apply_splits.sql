-- shorten existing features
WITH min_segs AS
(
SELECT DISTINCT ON (linear_feature_id)
  linear_feature_id,
  downstream_route_measure
 FROM cwf.temp_streams
 ORDER BY linear_feature_id, downstream_route_measure asc
 )

UPDATE cwf.segmented_streams s
SET
  upstream_route_measure = m.downstream_route_measure,
  length_metre = ST_Length(ST_LocateBetween(s.geom, s.downstream_route_measure, m.downstream_route_measure)),
  geom = ST_LocateBetween(s.geom, s.downstream_route_measure, m.downstream_route_measure)
FROM min_segs m
WHERE m.linear_feature_id = s.linear_feature_id;


-- insert new features
INSERT INTO cwf.segmented_streams
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
 s.linear_feature_id        ,
 s.watershed_group_id       ,
 s.edge_type                ,
 s.blue_line_key            ,
 s.watershed_key            ,
 s.fwa_watershed_code       ,
 s.local_watershed_code     ,
 s.watershed_group_code     ,
 t.downstream_route_measure ,
 t.length_metre             ,
 s.feature_source           ,
 s.gnis_id                  ,
 s.gnis_name                ,
 s.left_right_tributary     ,
 s.stream_order             ,
 s.stream_magnitude         ,
 s.waterbody_key            ,
 s.blue_line_key_50k        ,
 s.watershed_code_50k       ,
 s.watershed_key_50k        ,
 s.watershed_group_code_50k ,
 s.gradient                 ,
 s.feature_code             ,
 s.wscode_ltree             ,
 s.localcode_ltree          ,
 t.upstream_route_measure   ,
 t.geom
FROM cwf.temp_streams t
INNER JOIN cwf.segmented_streams s
ON t.linear_feature_id = s.linear_feature_id;