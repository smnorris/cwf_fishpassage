DROP TABLE IF EXISTS cwf.segmented_streams_arcgis;
CREATE TABLE cwf.segmented_streams_arcgis AS
SELECT
  segmented_stream_id,
  linear_feature_id ,
  watershed_group_id ,
  edge_type ,
  blue_line_key ,
  watershed_key ,
  fwa_watershed_code ,
  local_watershed_code ,
  watershed_group_code ,
  downstream_route_measure ,
  length_metre ,
  feature_source ,
  gnis_id ,
  gnis_name ,
  left_right_tributary ,
  stream_order ,
  stream_magnitude ,
  waterbody_key ,
  blue_line_key_50k ,
  watershed_code_50k ,
  watershed_key_50k ,
  watershed_group_code_50k ,
  gradient ,
  feature_code ,
  wscode_ltree::text ,
  localcode_ltree::text ,
  upstream_route_measure ,
  array_to_string(downstream_barrier_id_15, ',', 'NONE') as downstream_barrier_id_15,
  array_to_string(downstream_barrier_id_20, ',', 'NONE') as downstream_barrier_id_20,
  array_to_string(downstream_barrier_id_30, ',', 'NONE') as downstream_barrier_id_30,
  array_to_string(downstream_barrier_id_structure, ',', 'NONE') as downstream_barrier_id_structure,
  array_to_string(upstream_observation_id, ',', 'NONE') as upstream_observation_id,
  array_to_string(downstream_barrier_id_15_sar, ',', 'NONE') as downstream_barrier_id_15_sar,
  array_to_string(downstream_barrier_id_20_sar, ',', 'NONE') as downstream_barrier_id_20_sar,
  array_to_string(downstream_barrier_id_30_sar, ',', 'NONE') as downstream_barrier_id_30_sar,
  geom
FROM cwf.segmented_streams
WHERE watershed_group_code in ('HORS','LNIC','BULK','ELKR');

CREATE INDEX ON cwf.segmented_streams_arcgis USING GIST (geom);
ALTER TABLE cwf.segmented_streams_arcgis ADD PRIMARY KEY (segmented_stream_id);

DROP TABLE IF EXISTS cwf.barriers_15_arcgis;
CREATE TABLE cwf.barriers_15_arcgis AS
SELECT

 barrier_id               ,
 source_id                ,
 barrier_type             ,
 barrier_name             ,
 linear_feature_id        ,
 blue_line_key            ,
 downstream_route_measure ,
 wscode_ltree::text             ,
 localcode_ltree::text          ,
 watershed_group_code     ,
 array_to_string(downstream_ids, ',', 'NONE') as downstream_ids           ,
 array_to_string(upstream_observation_ids, ',', 'NONE') as upstream_observation_ids ,
 geom
FROM cwf.barriers_15
WHERE watershed_group_code in ('HORS','LNIC','BULK','ELKR');

CREATE INDEX ON cwf.barriers_15_arcgis USING GIST (geom);
ALTER TABLE cwf.barriers_15_arcgis ADD PRIMARY KEY (barrier_id);

DROP TABLE IF EXISTS cwf.barriers_20_arcgis;
CREATE TABLE cwf.barriers_20_arcgis AS
SELECT

 barrier_id               ,
 source_id                ,
 barrier_type             ,
 barrier_name             ,
 linear_feature_id        ,
 blue_line_key            ,
 downstream_route_measure ,
 wscode_ltree::text             ,
 localcode_ltree::text          ,
 watershed_group_code     ,
 array_to_string(downstream_ids, ',', 'NONE') as downstream_ids           ,
 array_to_string(upstream_observation_ids, ',', 'NONE') as upstream_observation_ids ,
 geom
FROM cwf.barriers_20
WHERE watershed_group_code in ('HORS','LNIC','BULK','ELKR');

CREATE INDEX ON cwf.barriers_20_arcgis USING GIST (geom);
ALTER TABLE cwf.barriers_20_arcgis ADD PRIMARY KEY (barrier_id);


DROP TABLE IF EXISTS cwf.barriers_report_arcgis;
CREATE TABLE cwf.barriers_report_arcgis (
 barrier_id                    integer             ,
 source_id                     integer             ,
 barrier_type                  text                ,
 barrier_name                  text                ,
 linear_feature_id             integer             ,
 blue_line_key                 integer             ,
 downstream_route_measure      double precision    ,
 wscode_ltree                  text               ,
 localcode_ltree               text               ,
 watershed_group_code          text                ,
 downstream_ids                text           ,
 stream_order                  integer             ,
 upstream_gradient             double precision    ,
 downstream_species            text              ,
 upstream_species              text              ,
 upstream_accessible15_km      numeric             ,
 upstream_accessible20_km      numeric             ,
 upstream_accessible15_sar_km  numeric             ,
 upstream_accessible20_sar_km  numeric             ,
 upstream_accessible30_sar_km  numeric             ,
 geom                          geometry(Point,3005)
 );

INSERT INTO cwf.barriers_report_arcgis
SELECT
 barrier_id,
 source_id,
 barrier_type,
 barrier_name,
 linear_feature_id,
 blue_line_key,
 downstream_route_measure,
 wscode_ltree::text,
 localcode_ltree::text,
 watershed_group_code,
 array_to_string(downstream_ids, ',', 'NONE') as downstream_ids,
 stream_order,
 upstream_gradient,
 array_to_string(downstream_species, ',', 'NONE') as downstream_species,
 array_to_string(upstream_species, ',', 'NONE') as upstream_species,
 upstream_accessible15_km,
 upstream_accessible20_km,
 upstream_accessible15_sar_km,
 upstream_accessible20_sar_km,
 upstream_accessible30_sar_km,
 geom
FROM cwf.barriers_report;

CREATE INDEX ON cwf.barriers_structures_arcgis USING GIST (geom);
ALTER TABLE cwf.barriers_structures_arcgis ADD PRIMARY KEY (barrier_id);

-- running the maps with dashed stream lines for default tables crashes arcmap
DROP TABLE IF EXISTS cwf.streams_carto ;

create table cwf.streams_carto AS
SELECT a.blue_line_key, a.watershed_group_code, st_union(a.geom) as geom
FROM whse_basemapping.fwa_stream_networks_sp a
WHERE watershed_group_code IN ('HORS','BULK','LNIC','ELKR')
GROUP BY blue_line_key, watershed_group_code;

create index on cwf.streams_carto USING GIST (geom);