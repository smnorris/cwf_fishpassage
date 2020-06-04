-- create total land cover alteration layer

-- POLYGON SOURCES
-- cut blocks
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP' as source,
  'Harvest' as disturbance_type,
  b.map_tile,
   ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
   ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(a.geom, b.geom)
      END
    , 3)
        )
      )).geom
    ) as geom
FROM whse_forest_vegetation.veg_consolidated_cut_blocks_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE disturbance_start_date >= '1959/01/01';


-- vri
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY' as source,
  CASE
    WHEN bclcs_level_5 IN ('RZ', 'RN', 'UR', 'AP') THEN 'Urban'
    WHEN bclcs_level_5 = 'BU' OR earliest_nonlogging_dist_type = 'B' THEN 'Fire'
    WHEN bclcs_level_5 IN ('GP', 'TZ', 'MI') THEN 'Mining'
  END as disturbance_type,
  b.map_tile,
   ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(a.geom, b.geom)
      END
    , 3)
        )
      )).geom
    ) as geom
FROM whse_forest_vegetation.veg_comp_lyr_r1_poly a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE
  (a.bclcs_level_5 IN ('RZ', 'RN', 'UR', 'AP', 'BU', 'GP', 'TZ', 'MI') OR
    earliest_nonlogging_dist_type = 'B');


-- utility corridors
-- requires some cleaning to go through without topo exceptions (small buffer out/in)
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_TANTALIS.TA_CROWN_TENURES_SVW' as source,
  'Utility Corridor' as disturbance_type,
  b.map_tile,
 ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(st_buffer(st_buffer(a.geom, .1), -.1), b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(st_buffer(st_buffer(a.geom, .1), -.1), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_tantalis.ta_crown_tenures_svw a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(st_buffer(st_buffer(a.geom, .1), -.1), b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE a.tenure_purpose = 'UTILITY';


-- historical fires - topo exceptions to be cleaned, buffer in/out
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP' as source,
  'Fire' as disturbance_type,
  b.map_tile,
   -- make sure the output is valid
   -- the output of st_safe_repair seems to be multipoly, perhaps
   -- there is a bowtie being broken up? re-dump.
    (ST_Dump(ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(st_buffer(st_buffer(a.geom, .1), -.1), b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(st_buffer(st_buffer(a.geom, .1), -.1), b.geom)
      END
    , 3)
          )
      )).geom
    ))).geom as geom
FROM whse_land_and_natural_resource.prot_historical_fire_polys_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(st_buffer(st_buffer(a.geom, .1), -.1), b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE a.fire_year >= 1993;


-- current fires, also needs a clean
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP' as source,
  'Fire' as disturbance_type,
  b.map_tile,
   -- make sure the output is valid
    ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(st_buffer(st_buffer(a.geom, .1), -.1), b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(st_buffer(st_buffer(a.geom, .1), -.1), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_land_and_natural_resource.prot_current_fire_polys_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(st_buffer(st_buffer(a.geom, .1), -.1), b.geom);


-- mining from btm
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_BASEMAPPING.BTM_PRESENT_LAND_USE_V1_SVW' as source,
  'Mining' as disturbance_type,
  b.map_tile,
  -- make sure the output is valid
    ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(a.geom, b.geom)
      END
    , 3)
      )
      )).geom
    ) as geom
FROM whse_basemapping.btm_present_land_use_v1_svw a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE a.present_land_use_label = 'Mining';


-- agriculture
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_ENVIRONMENTAL_MONITORING.NRC_OTHER_LAND_COVER_250K_SP' as source,
  'Agriculture' as disturbance_type,
  b.map_tile,
  -- make sure the output is valid
    ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
        ELSE ST_Safe_Intersection(a.geom, b.geom)
      END
    , 3)
      )
      )).geom
    ) as geom
FROM whse_environmental_monitoring.nrc_other_land_cover_250k_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
-- note year, 60 years ago (relative to 2019 inventory)
WHERE a.land_cover_class_code IN ('Perennial Cropland','Annual Cropland');


-- LINEAR SOURCES to be buffered
-- buffer highways by 8m * n lanes
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_BASEMAPPING.DRA_DGTL_ROAD_ATLAS_MPAR_SP' as source,
  'Road - highway' as disturbance_type,
  b.map_tile,
    ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(ST_Buffer(a.geom, (number_of_lanes * 8)), b.geom) THEN ST_Buffer(a.geom, (number_of_lanes * 8))
        ELSE ST_Safe_Intersection(ST_Buffer(a.geom, (number_of_lanes * 8)), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_basemapping.dra_dgtl_road_atlas_mpar_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
WHERE a.road_class in ('highway','freeway','ramp');


-- buffer other roads by 5m * n lanes, don't include ferry/water and trails
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_BASEMAPPING.DRA_DGTL_ROAD_ATLAS_MPAR_SP' as source,
  'Road - non-highway' as disturbance_type,
  b.map_tile,
     ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(ST_Buffer(a.geom, (number_of_lanes * 5)), b.geom) THEN ST_Buffer(a.geom, (number_of_lanes * 5))
        ELSE ST_Safe_Intersection(ST_Buffer(a.geom, (number_of_lanes * 5)), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_basemapping.dra_dgtl_road_atlas_mpar_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom)
WHERE a.road_class not in ('highway','freeway','proposed','trail','water','ferry');


-- ften roads are all buffered by 5m
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_FOREST_TENURE.FTEN_ROAD_SEGMENT_LINES_SVW' as source,
  'Road - FTEN' as disturbance_type,
  b.map_tile,
    ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(ST_Buffer(a.geom, 5), b.geom) THEN ST_Buffer(a.geom, 5)
        ELSE ST_Safe_Intersection(ST_Buffer(a.geom, 5), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_forest_tenure.ften_road_segment_lines_svw a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom);


-- railways buffered by 5m
INSERT INTO cwf.tlca (source, disturbance_type, map_tile, geom)
SELECT
  'WHSE_BASEMAPPING.GBA_RAILWAY_TRACKS_SP' as source,
  'Railway' as disturbance_type,
  b.map_tile,
     ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- force to multipart just to make sure everthing is the same
  ST_Multi(
   -- include only polygons in cases of geometrycollections
     ST_CollectionExtract(
   -- intersect with tiles
      CASE
        WHEN ST_CoveredBy(ST_Buffer(a.geom, 4), b.geom) THEN ST_Buffer(a.geom, 4)
        ELSE ST_Safe_Intersection(ST_Buffer(a.geom, 4), b.geom)
      END
    , 3)
          )
      )).geom
    ) as geom
FROM whse_basemapping.gba_railway_tracks_sp a
INNER JOIN whse_basemapping.bcgs_5k_grid b
ON ST_Intersects(a.geom, b.geom);