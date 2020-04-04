# create schema
psql -c "CREATE SCHEMA IF NOT EXISTS cwf"

# load large dams to cwf schema
ogr2ogr \
  -f PostgreSQL \
  PG:"$PGOGR" \
  -overwrite \
  -lco OVERWRITE=YES \
  -t_srs EPSG:3005 \
  -lco SCHEMA=cwf \
  -lco GEOMETRY_NAME=geom \
  -nln large_dams \
  inputs/large_dams_bc.geojson

# load FISS obstacles
bcdata bc2pg WHSE_FISH.FISS_OBSTACLES_PNT_SP