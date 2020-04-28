#!/bin/bash -x

# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load list of watershed groups
psql -c "CREATE TABLE cwf.target_watershed_groups (watershed_group_code text, status text, notes text)"
psql -c "\copy cwf.target_watershed_groups FROM '../inputs/target_watershed_groups.csv' delimiter ',' csv header"

# convert source .gpkg to geojson for addition to repo
ogr2ogr \
  -f GeoJSON \
  ../inputs/dams_bc.geojson \
  -s_srs EPSG:3005 \
  -t_srs EPSG:4326 \
  -nln dams_bc \
  -lco RFC7946=YES \
  ../inputs/BC_AllDams_wHydro.gpkg \
  BC_AllDams_wHydro

# load dams to cwf schema
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -overwrite \
  -lco OVERWRITE=YES \
  -t_srs EPSG:3005 \
  -lco SCHEMA=cwf \
  -lco GEOMETRY_NAME=geom \
  -nln dams_src \
  ../inputs/dams_bc.geojson

# delete FISS dams that don't exist (preliminary review)
# and update barrier/hydro indicators where needed
psql -f sql/dams_fixes.sql

# match dams to nearest stream
psql -f sql/dams_match2stream.sql

# load FISS obstacles
bcdata bc2pg WHSE_FISH.FISS_OBSTACLES_PNT_SP

# load gradient barriers from source tables
psql -f sql/gradient_barriers.sql

# load and simplify fish ranges
ogr2ogr \
  -t_srs EPSG:3005 \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco OVERWRITE=YES \
  -lco SCHEMA=whse_fish \
  -lco GEOMETRY_NAME=geom \
  -nln fiss_fish_ranges_svw \
  ../inputs/WHSE_FISH.gdb \
  FISS_FISH_RANGES_SVW

psql -f sql/fish_ranges.sql