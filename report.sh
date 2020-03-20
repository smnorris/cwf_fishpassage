#!/bin/bash -x

# load FISS obstacles
bcdata bc2pg WHSE_FISH.FISS_OBSTACLES_PNT_SP

# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load large dams
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -overwrite \
  -lco OVERWRITE=YES \
  -t_srs EPSG:3005 \
  -lco SCHEMA=cwf \
  -lco GEOMETRY_NAME=geom \
  -nln dams \
  inputs/large_dams_bc.geojson

# create barriers table
psql -f sql/01_create_barriers.sql

# qa watershed groups above barriers
psql2csv < sql/02_barrier_qa.sql > outputs/barrier_qa.csv

# create wsg_upstream_of_barriers table
psql -f sql/03_wsg_upstream_of_barriers.sql

# load fish ranges
ogr2ogr \
  -t_srs EPSG:3005 \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco OVERWRITE=YES \
  -lco SCHEMA=whse_fish \
  -lco GEOMETRY_NAME=geom \
  -nln fiss_fish_ranges_svw \
  inputs/WHSE_FISH.gdb \
  FISS_FISH_RANGES_SVW

psql -f sql/04_create_fiss_fish_ranges.sql

# report on watershed groups to include in model
psql2csv < sql/05_wsg_report.sql > outputs/wsg_report.csv