#!/bin/bash
set -euxo pipefail

# Run through the steps in model.py
# - generate barriers
# - index the barriers
# - cleanup barriers
# - split streams at barriers
# - create output table

python model.py barriers-create 15
#python model.py barriers-create 20

psql -t -P border=0,footer=no \
  -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" \
  | sed -e '$d' \
  | parallel python model.py barriers-index

python model.py barriers-cleanup

psql -t -P border=0,footer=no \
  -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" \
  | sed -e '$d' \
  | parallel python model.py split-streams

python model.py create-output
python model.py label

# this depends on existing table "fish_passage.fish_habitat"
psql2csv < sql/compare_model_results.sql > ../outputs/compare_model_results.csv

# report on results
psql2csv < sql/10_report.sql > ../outputs/wsg_prioritize.csv

# rename initial barrier table
psql -c "ALTER TABLE cwf.barriers RENAME TO barriers_15"

# create barriers from all dams and potential CBS points
python model.py barriers-create structures