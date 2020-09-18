#!/bin/bash
set -euxo pipefail

# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load list of watershed groups
psql -c "DROP TABLE IF EXISTS cwf.target_watershed_groups"
psql -c "CREATE TABLE cwf.target_watershed_groups (watershed_group_code text, status text, notes text, spp_sar text)"
psql -c "\copy cwf.target_watershed_groups FROM '../inputs/target_watershed_groups.csv' delimiter ',' csv header"
psql -c "ALTER TABLE cwf.target_watershed_groups ADD COLUMN spp_array text[]"
psql -c "UPDATE cwf.target_watershed_groups SET spp_array = string_to_array(spp_sar,';')"
psql -c "ALTER TABLE cwf.target_watershed_groups DROP COLUMN spp_sar"
psql -c "ALTER TABLE cwf.target_watershed_groups RENAME COLUMN spp_array TO spp_sar"

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
# (apply these fixes manually as they are provided and re-run the match2streams etc)
#psql -f sql/dams_fixes.sql
#psql -f sql/dams_fixes_2020-05-12.sql
#psql -f sql/dams_fixes_2020-06-11.sql
#psql -f sql/dams_fixes_2020-07-07.sql

# match dams to nearest stream
psql -f sql/dams_match2stream.sql

# load culvert QA table
psql -c "DROP TABLE IF EXISTS cwf.modelled_culverts_qa;"
psql -c "CREATE TABLE cwf.modelled_culverts_qa (watershed_group_code text, reviewer text, source_id integer, structure text, notes text)"
psql -c "\copy cwf.modelled_culverts_qa FROM '../inputs/CWF_culvert_fixes.csv' delimiter ',' csv header"


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

# water licenses
bcdata bc2pg WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SV

# DFO SAR data - critical habitat
tmp="${TEMP:-/tmp}"

wget --trust-server-names -qNP "$tmp" https://dfogis.azureedge.net/CriticalHabitat_HabitatEssentiel.zip
unzip -qun -d "$tmp" "$tmp/CriticalHabitat_HabitatEssentiel.zip"
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco GEOMETRY_NAME=geom \
  -t_srs EPSG:3005 \
  -lco GEOMETRY_NAME=geom \
  -nln cwf.dfo_sara_crithab_2019 \
  -nlt MULTIPOLYGON \
  -where "Common_Name_EN IN ('Salish Sucker', 'Nooksack Dace', 'Westslope Cutthroat Trout', 'Bull Trout')" \
  -overwrite \
  $tmp/CriticalHabitat_FGP.gdb \
  DFO_SARA_CritHab_2019_FGP_EN

# DFO SAR - distribution
wget --trust-server-names -qNP "$tmp" https://pacgis01.dfo-mpo.gc.ca/FGPPublic/DFO_Species_at_Risk_Distribution/Distribution_Repartition.zip
unzip -qun -d "$tmp" "$tmp/Distribution_Repartition.zip"
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco GEOMETRY_NAME=geom \
  -t_srs EPSG:3005 \
  -lco GEOMETRY_NAME=geom \
  -nln cwf.dfo_sara_dist_2019_src \
  -nlt MULTIPOLYGON \
  -where "Common_Name_EN IN ('Salish Sucker', 'Nooksack Dace', 'Westslope Cutthroat Trout', 'Bull Trout') AND lead_region = 'Pacific'" \
  -overwrite \
  Distribution_FGP.gdb \
  DFO_SARA_Dist_2019_FGP_EN

# subdivide and conquor the messy data
psql -f dfo.sql


# DFO conservation units
# provided as shapefiles ¯\_(ツ)_/¯
wget --trust-server-names -qNP "$tmp" https://pacgis01.dfo-mpo.gc.ca/FGPPublic/Pacific_Salmon_CU/Shape_Files/Chinook_Salmon_CU_Shape.zip
unzip -qun -d "$tmp" "$tmp/Chinook_Salmon_CU_Shape.zip"
wget --trust-server-names -qNP "$tmp" https://pacgis01.dfo-mpo.gc.ca/FGPPublic/Pacific_Salmon_CU/Shape_Files/Coho_Salmon_CU_Shape.zip
unzip -qun -d "$tmp" "$tmp/Coho_Salmon_CU_Shape.zip"
wget --trust-server-names -qNP "$tmp" https://pacgis01.dfo-mpo.gc.ca/FGPPublic/Pacific_Salmon_CU/Shape_Files/Lake_Type_Sockeye_Salmon_CU_Shape.zip
unzip -qun -d "$tmp" "$tmp/Lake_Type_Sockeye_Salmon_CU_Shape.zip"

# load to local db, stripping out area/length columns that crash the
# conversion ಠ╭╮ಠ
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco GEOMETRY_NAME=geom \
  -t_srs EPSG:3005 \
  -lco GEOMETRY_NAME=geom \
  -nln cwf.dfo_cu_boundary_chinook \
  -select CU_NAME,FULL_CU_IN,SP_QUAL,CU_TYPE \
  -nlt MULTIPOLYGON \
  -overwrite \
  "$tmp/Chinook_Salmon_CU_Boundary/Chinook_Salmon_CU_Boundary_En.shp"

# CU boundary fields are not standard! ఠ_ఠ
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco GEOMETRY_NAME=geom \
  -t_srs EPSG:3005 \
  -lco GEOMETRY_NAME=geom \
  -nln cwf.dfo_cu_boundary_coho \
  -nlt MULTIPOLYGON \
  -sql "SELECT CU_name AS CU_NAME, Full_CU_IN as FULL_CU_IN, SPECIES_QU AS SP_QUAL, Type AS CU_TYPE from Coho_Salmon_CU_Boundary_En" \
  -overwrite \
  "$tmp/Coho_Salmon_CU_Boundary/Coho_Salmon_CU_Boundary_En.shp"

# file names not standardized / more abbrev field name variations 〴⋋_⋌〵
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -lco GEOMETRY_NAME=geom \
  -t_srs EPSG:3005 \
  -lco GEOMETRY_NAME=geom \
  -nln cwf.dfo_cu_boundary_sockeye \
  -nlt MULTIPOLYGON \
  -sql 'SELECT CU_name AS CU_NAME, CU_INDEX as FULL_CU_IN, SP_QUAL, CU_TYPE from "Lake Type Sockeye Salmon CU Boundary_En"' \
  -overwrite \
  "$tmp/Lake_Type_Sockeye_Salmon_CU_Boundary/Lake Type Sockeye Salmon CU Boundary_En.shp"
