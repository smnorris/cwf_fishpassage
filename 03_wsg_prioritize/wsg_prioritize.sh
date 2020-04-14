#!/bin/bash
set -euxo pipefail

# Run through the steps in model.py
# - generate barriers
# - index the barriers
# - cleanup barriers
# - split streams at barriers
# - create output table

python model.py barriers-create

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