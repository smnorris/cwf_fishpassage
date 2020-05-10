#!/bin/bash
set -euxo pipefail

# - generate barriers
# - index the barriers
# - cleanup barriers
# - split streams at barriers
# - create output table
# - generate, index, clean new barriers
# - run splitting with new barriers
# - label with new barriers
# -etc

# create and index our barrier tables that define the accessible habitat model
psql -f sql/01_create_barriers_15.sql
psql -f sql/01_create_barriers_20.sql
python model.py index-barriers cwf.barriers_15 barrier_id cwf.barriers_15 barrier_id downstream_ids upstream_ids
python model.py index-barriers cwf.barriers_20 barrier_id cwf.barriers_20 barrier_id downstream_ids upstream_ids


psql -f sql/01_create_barriers_structures.sql
python model.py index-barriers cwf.barriers_structures barrier_id

# create output streams table for breaking at barriers
python model.py initialize-output cwf.segmented_streams

# ------------------
# 15 % gradient barriers model
# ------------------
psql -t -P border=0,footer=no \
  -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" \
  | sed -e '$d' \
  | parallel python model.py split-streams

python model.py create-output
python model.py label downstream_barrier_id_15

# rename initial barrier table
psql -c "DROP TABLE IF EXISTS cwf.barriers_15"
psql -c "ALTER TABLE cwf.barriers RENAME TO barriers_15"


# ------------------
# 20% gradient barriers model
# ------------------
psql -f sql/split_streams_all.sql
python model.py label downstream_barrier_id_20

# rename initial barrier table
psql -c "DROP TABLE IF EXISTS cwf.barriers_20"
psql -c "ALTER TABLE cwf.barriers RENAME TO barriers_20"


# ------------------
# add barriers from all dams and potential CBS points
# (there aren't nearly as many points, no need to run in parallel)
# ------------------
psql -f sql/split_streams_all.sql
python model.py label downstream_barrier_id_structure

# rename initial barrier table
psql -c "DROP TABLE IF EXISTS cwf.barriers_structures"
psql -c "ALTER TABLE cwf.barriers RENAME TO barriers_structures"

# this depends on existing table "fish_passage.fish_habitat"
#psql2csv < sql/compare_model_results.sql > ../outputs/compare_model_results.csv

# report on results
psql2csv < sql/11_report_structureless.sql > ../outputs/wsg_prioritize.csv
