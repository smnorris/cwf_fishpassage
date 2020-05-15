DROP TABLE IF EXISTS cwf.dfo_sara_dist_2019;

CREATE TABLE cwf.dfo_sara_dist_2019
(dfo_sara_dist_2019_id serial primary key,
common_name_en  character varying(255)     ,
population_en   character varying(255)     ,
common_name_fr  character varying(255)     ,
population_fr   character varying(255)     ,
scientific_name character varying(255)     ,
taxon           character varying(255)     ,
eco_type        character varying(255)     ,
waterbody       character varying(255)     ,
sara_status     character varying(255)     ,
lead_region     character varying(255)     ,
support_region  character varying(255)     ,
data_source     character varying(255)     ,
species_link    character varying(255)     ,
shape_length    double precision           ,
shape_area      double precision           ,
geom            geometry(MultiPolygon,3005));

INSERT INTO cwf.dfo_sara_dist_2019
(common_name_en,
population_en,
common_name_fr,
population_fr,
scientific_name,
taxon,
eco_type,
waterbody,
sara_status,
lead_region,
support_region,
data_source,
species_link,
geom)
SELECT
common_name_en,
population_en,
common_name_fr,
population_fr,
scientific_name,
taxon,
eco_type,
waterbody,
sara_status,
lead_region,
support_region,
data_source,
species_link,
st_multi(st_subdivide(st_makevalid((ST_Dump(geom)).geom))) as geom
FROM cwf.dfo_sara_dist_2019_src;

