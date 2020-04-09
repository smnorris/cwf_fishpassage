# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load list of watershed groups
psql -c "CREATE TABLE cwf.target_watershed_groups (watershed_group_code text, status text, notes text)"
psql -c "\copy cwf.target_watershed_groups FROM '../inputs/target_watershed_groups.csv' delimiter ',' csv header"

# load large dams to cwf schema
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -overwrite \
  -lco OVERWRITE=YES \
  -t_srs EPSG:3005 \
  -lco SCHEMA=cwf \
  -lco GEOMETRY_NAME=geom \
  -nln large_dams_src \
  ../inputs/large_dams_bc.geojson

# match large dams to nearest stream
psql -f sql/large_dams.sql

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

psql -f sql/fiss_fish_ranges.sql