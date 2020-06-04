#!/bin/bash
set -euxo pipefail

# ----------------------------------
# First, create the barrier tables.
# 1. Create the initial, absolute barrier tables 15/20/30 define potentially accessible streams
# 2. Index each barrier table, noting which barriers are upstream / downstream (within the same table)
# 3. We do not care about non-minimal barriers in the 15/20/30 tables, they are considered 'absolute'
#    barriers, so only the record at the bottom is of interest. Just delete the rest
#    so that we don't segment the streams by barriers that we don't care about
# ----------------------------------
# 15pct scenario
psql -f sql/01_create_barriers_15.sql
python model.py add-downstream-ids cwf.barriers_15 barrier_id cwf.barriers_15 barrier_id downstream_ids
psql -c "DELETE FROM cwf.barriers_15 WHERE downstream_ids IS NOT NULL"

# 20pct scenario
psql -f sql/01_create_barriers_20.sql
python model.py add-downstream-ids cwf.barriers_20 barrier_id cwf.barriers_20 barrier_id downstream_ids
psql -c "DELETE FROM cwf.barriers_20 WHERE downstream_ids IS NOT NULL"

# 30pct scenario (for just a few select SAR watersheds with Bull Trout)
psql -f sql/01_create_barriers_30.sql
python model.py add-downstream-ids cwf.barriers_30 barrier_id cwf.barriers_30 barrier_id downstream_ids
psql -c "DELETE FROM cwf.barriers_30 WHERE downstream_ids IS NOT NULL"

# build the structure barriers
psql -f sql/01_create_barriers_structures.sql
python model.py add-downstream-ids cwf.barriers_structures barrier_id cwf.barriers_structures barrier_id downstream_ids
# we could identify & discard structures not on potentially accessible streams from any of above scenarios
# (by running add-downtream-ids on above tables) - but just including everything shouldn't make much difference

# ----------------------------------
# Create output streams table for breaking at barriers
# ----------------------------------
python model.py initialize-output cwf.segmented_streams

# ----------------------------------
# break streams at each barrier
# ----------------------------------
python model.py segment-streams cwf.segmented_streams cwf.barriers_15
python model.py segment-streams cwf.segmented_streams cwf.barriers_20
python model.py segment-streams cwf.segmented_streams cwf.barriers_30
python model.py segment-streams cwf.segmented_streams cwf.barriers_structures

# ----------------------------------
# add columns to the split streams table, noting which barriers are downstream
# ----------------------------------
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_15 barrier_id downstream_barrier_id_15 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_20 barrier_id downstream_barrier_id_20 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_30 barrier_id downstream_barrier_id_30 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_structures barrier_id downstream_barrier_id_structure --include_equivalent_measure

# this depends on existing table "fish_passage.fish_habitat"
#psql2csv < sql/compare_model_results.sql > ../outputs/compare_model_results.csv

# report on results
psql2csv < sql/11_report_structureless.sql > ../outputs/wsg_prioritize.csv
psql2csv < sql/13_dci.sql > ../outputs/dci.csv
# extra query, unrelated to streams
#psql2csv < sql/12_road_density.sql > ../outputs/road_density.csv

# tlca

psql2csv "WITH tlca AS
(SELECT
  watershed_group_code,
  SUM(st_area(geom)) / 10000 as area_tlca_ha
FROM cwf.tlca_union
GROUP BY watershed_group_code
)
SELECT
 a.watershed_group_code,
 ROUND((COALESCE(b.area_tlca_ha, 0))::numeric, 2) as area_tlca_ha,
 ROUND((ST_Area(a.geom) / 10000)::numeric, 2) as area_total_ha,
 ROUND(((COALESCE(b.area_tlca_ha, 0) / (ST_Area(a.geom) / 10000)) * 100)::numeric, 2) as pct_tlca
FROM whse_basemapping.fwa_watershed_groups_poly a
LEFT OUTER JOIN tlca b ON a.watershed_group_code = b.watershed_group_code
ORDER BY a.watershed_group_code" > ../outputs/tlca.csv
