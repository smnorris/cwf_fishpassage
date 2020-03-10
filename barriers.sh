#!/bin/bash -x


# load FISS obstacles
bcdata bc2pg WHSE_FISH.FISS_OBSTACLES_PNT_SP


# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load CanVec and World Waterfalls data
# (FWA obstructions and FISS obstacles are included in this file but
# we can just grab those from the source data)
# Note - I am not sure why the overwrite behaviour is inconsistent between databases,
# there is probably an active schemas setting in my homebrew installation
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -overwrite \
  -lco OVERWRITE=YES \
  -t_srs EPSG:3005 \
  -lco SCHEMA=cwf \
  -lco GEOMETRY_NAME=geom \
  -nln falls \
  -sql "SELECT dataset_nm, feature_id, name, name_1_en FROM BC_Falls WHERE Dataset_NM IN ('CanVec', 'WWD')" \
  inputs/BC_Falls/BC_Falls.shp

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
  inputs/Large_Dams_Data_BC/Large_Dams_BC_V2.0.shp

# create barriers table
psql -f sql/barriers.sql