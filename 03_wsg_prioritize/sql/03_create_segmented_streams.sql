-- create initial stream table based on provincial table
DROP TABLE IF EXISTS cwf.{table};

CREATE TABLE cwf.{table}
(
  segmented_stream_id serial primary key,
  linear_feature_id         bigint                           ,
  watershed_group_id        bigint                           ,
  edge_type                 bigint                           ,
  blue_line_key             bigint                           ,
  watershed_key             bigint                           ,
  fwa_watershed_code        character varying(143)           ,
  local_watershed_code      character varying(143)           ,
  watershed_group_code      character varying(4)             ,
  downstream_route_measure  double precision                 ,
  length_metre              double precision                 ,
  feature_source            character varying(15)            ,
  gnis_id                   bigint                           ,
  gnis_name                 character varying(80)            ,
  left_right_tributary      character varying(7)             ,
  stream_order              bigint                           ,
  stream_magnitude          bigint                           ,
  waterbody_key             bigint                           ,
  blue_line_key_50k         bigint                           ,
  watershed_code_50k        character varying(45)            ,
  watershed_key_50k         bigint                           ,
  watershed_group_code_50k  character varying(4)             ,
  gradient                  double precision                 ,
  feature_code              character varying(10)            ,
  wscode_ltree              ltree                            ,
  localcode_ltree           ltree                            ,
  upstream_route_measure    double precision                 ,
  geom                      geometry(MultiLineStringZM,3005)
);

CREATE INDEX ON cwf.{table} USING btree (linear_feature_id);