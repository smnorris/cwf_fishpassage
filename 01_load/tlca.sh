#!/bin/bash
set -euxo pipefail

# ==================================
# Total Land Cover Alteration (TCLA)
# ==================================

# ------------------------------------------
# -- get inputs
# ------------------------------------------
# load 250k tiles for parallelization
bcdata bc2pg WHSE_BASEMAPPING.NTS_250K_GRID

# Load complete source layers and run queries locally
bcdata bc2pg WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP
bcdata bc2pg WHSE_BASEMAPPING.DRA_DGTL_ROAD_ATLAS_MPAR_SP
bcdata bc2pg WHSE_FOREST_TENURE.FTEN_ROAD_SEGMENT_LINES_SVW
bcdata bc2pg WHSE_BASEMAPPING.GBA_RAILWAY_TRACKS_SP
bcdata bc2pg WHSE_TANTALIS.TA_CROWN_TENURES_SVW
bcdata bc2pg WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP
bcdata bc2pg WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP
bcdata bc2pg WHSE_BASEMAPPING.BTM_PRESENT_LAND_USE_V1_SVW

veg2pg.sh

# agriculture/urban come from CANVEC https://open.canada.ca/data/en/dataset/97126362-5a85-4fe0-9dc2-915464cfdbb7
# However, this is published by tile. Instead of downloading tile by tile, lets just pull from what looks to be
# the same thing:
# https://catalogue.data.gov.bc.ca/dataset/other-land-cover-1-250-000-geobase-land-cover
# One odd thing though, this table is not published via WFS. Manually download file via
# the Catalogue Download service, unzip and place NRC_OTHER_LAND_COVER_250K_SP.gdb in the /inputs folder
ogr2ogr \
  -t_srs EPSG:3005 \
  -f PostgreSQL PG:"$PGOGR" \
  -lco OVERWRITE=YES \
  -lco SCHEMA=whse_environmental_monitoring \
  -lco GEOMETRY_NAME=geom \
  -nln nrc_other_land_cover_250k_sp \
  NRC_OTHER_LAND_COVER_250K_SP.gdb \
  WHSE_ENVIRONMENTAL_MONITORING_NRC_OTHER_LAND_COVER_250K_SP

# ------------------------------------------
# -- query and clean all inputs, loading to preliminary tlca table
# -- (it includes all overlapping features)
# ------------------------------------------
psql -c "DROP TABLE IF EXISTS cwf.tlca;"
psql -c "CREATE TABLE cwf.tlca
    (tlca_id SERIAL PRIMARY KEY,
     source text,
     disturbance_type text,
     map_tile text,
     geom Geometry(Polygon, 3005));"

time psql -f sql/tlca_load.sql
psql -c "CREATE INDEX ON cwf.tlca USING GIST (geom)"

# ------------------------------------------
# -- once all data are loaded to tlca, we overlay with watershed groups and
# -- remove overlaps (aggregate with st_union)
# ------------------------------------------
psql -c "DROP TABLE IF EXISTS cwf.tlca_union;"
psql -c "CREATE TABLE cwf.tlca_union
    (tlca_union_id SERIAL PRIMARY KEY,
     map_tile text,
     watershed_feature_id integer,
     watershed_group_id integer,
     watershed_group_code text,
     geom Geometry(Polygon, 3005));"

# Get tiles to process (71k) and intersect with watershed groups in parallel.
# https://www.depesz.com/2007/07/05/how-to-insert-data-to-database-as-fast-as-possible/
# https://github.com/dimensionaledge/cf_public/blob/master/tutorials/vector_tiling_and_map_reduce.sh
# https://github.com/dimensionaledge/cf_public/blob/master/tutorials/geoprocessing_alberta_land_cover_data.sh

# process in chunks of ~100 tiles
# Note that this could easily be combined with job above, loading all data to
# the output table without the intermediate table
time psql -t -P border=0,footer=no \
-c "WITH tiles AS
    (   SELECT DISTINCT map_tile
        FROM whse_basemapping.fwa_watershed_groups_subdivided a
        INNER JOIN whse_basemapping.bcgs_5k_grid b
        ON ST_Intersects(a.geom, b.geom)
        WHERE a.watershed_group_code IN
         (SELECT watershed_group_code
          FROM cwf.target_watershed_groups
          WHERE status = 'In')
        ORDER BY map_tile
    ),

    n_tiles AS
    (
        SELECT COUNT(DISTINCT map_tile) / 100 as n
        FROM tiles
    ),

    groups AS
    (
        SELECT
          map_tile,
          ntile(n::integer) over(order by map_tile) as group_id
        FROM tiles, n_tiles
    ),

    ranges AS
    (SELECT DISTINCT
      first_value(map_tile) over(partition by (group_id)) as min_tile,
      last_value(map_tile) over(partition by (group_id)) as max_tile
    FROM groups)

    SELECT ''''||min_tile||'''', ''''||max_tile||''''
    FROM ranges
    ORDER BY min_tile, max_tile;" \
    | sed -e '$d' \
    | parallel --colsep ' ' psql -f sql/tlca_union.sql -v tile1={1} -v tile2={2}
