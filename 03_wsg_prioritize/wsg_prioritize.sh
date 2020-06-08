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

# report on results
psql2csv < sql/11_report_structureless.sql > ../outputs/wsg_prioritize.csv