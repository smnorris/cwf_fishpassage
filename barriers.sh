#!/bin/bash -x


# load FISS obstacles
bcdata bc2pg WHSE_FISH.FISS_OBSTACLES_PNT_SP


# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"


# report on observations by watershed group
psql2csv < sql/observations_by_wsg.sql > outputs/observations_by_wsg.csv

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
psql -f sql/barriers.sql

# report on results
psql2csv < sql/wsg_upstream_of_barriers.sql > outputs/wsg_upstream_of_barriers.csv