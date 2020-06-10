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

# 20pct scenario
psql -f sql/01_create_barriers_20.sql
python model.py add-downstream-ids cwf.barriers_20 barrier_id cwf.barriers_20 barrier_id downstream_ids

# 30pct scenario (for just a few select SAR watersheds with Bull Trout)
psql -f sql/01_create_barriers_30.sql
python model.py add-downstream-ids cwf.barriers_30 barrier_id cwf.barriers_30 barrier_id downstream_ids

# build the structure barriers
psql -f sql/01_create_barriers_structures.sql
python model.py add-downstream-ids cwf.barriers_structures barrier_id cwf.barriers_structures barrier_id downstream_ids
# we could identify & discard structures not on potentially accessible streams from any of above scenarios
# (by running add-downtream-ids on above tables) - but just including everything shouldn't make much difference

# ----------------------------------
# Create and index the observations table
# ----------------------------------
psql -f sql/create_observations.sql
python model.py add-downstream-ids cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id downstream_ids
python model.py add-upstream-ids cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id upstream_ids
# remove non-maximal observations, for this scenario we only care about points with no other points upstream
psql -c "DELETE FROM cwf.fish_obsrvtn_events_sar WHERE upstream_ids IS NOT NULL"

# -- For SAR scenarios, note which barriers are below observations
python model.py add-upstream-ids cwf.barriers_15 barrier_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id upstream_observation_ids
python model.py add-upstream-ids cwf.barriers_20 barrier_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id upstream_observation_ids
python model.py add-upstream-ids cwf.barriers_30 barrier_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id upstream_observation_ids

# ----------------------------------
# Create output streams table for breaking at barriers and observations
# ----------------------------------
python model.py initialize-output cwf.segmented_streams

# ----------------------------------
# break streams at each barrier, at maximal observations
# ----------------------------------
python model.py segment-streams cwf.segmented_streams cwf.barriers_15
python model.py segment-streams cwf.segmented_streams cwf.barriers_20
python model.py segment-streams cwf.segmented_streams cwf.barriers_30
python model.py segment-streams cwf.segmented_streams cwf.barriers_structures
python model.py segment-streams cwf.segmented_streams cwf.fish_obsrvtn_events_sar

# ----------------------------------
# add columns to the split streams table, noting which barriers are downstream, which observations are upstream
# ----------------------------------
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_15 barrier_id downstream_barrier_id_15 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_20 barrier_id downstream_barrier_id_20 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_30 barrier_id downstream_barrier_id_30 --include_equivalent_measure
python model.py add-downstream-ids cwf.segmented_streams segmented_stream_id cwf.barriers_structures barrier_id downstream_barrier_id_structure --include_equivalent_measure
python model.py add-upstream-ids cwf.segmented_streams segmented_stream_id cwf.fish_obsrvtn_events_sar fish_obsrvtn_pnt_distinct_id upstream_observation_id

# above is enough for salmon, but for resident species in the SAR scenario, we add new
# columns tracking downstream barriers for each scenario - but removing barriers that
# are downstream of an observation
python model.py clear-barriers-below-observations cwf.segmented_streams cwf.barriers_15 downstream_barrier_id_15 downstream_barrier_id_15_sar
python model.py clear-barriers-below-observations cwf.segmented_streams cwf.barriers_20 downstream_barrier_id_20 downstream_barrier_id_20_sar
python model.py clear-barriers-below-observations cwf.segmented_streams cwf.barriers_30 downstream_barrier_id_30 downstream_barrier_id_30_sar

# above queries will result in empty arrays. Turn these into NULL for now for easier reporting
psql -c "UPDATE cwf.segmented_streams SET downstream_barrier_id_15_sar = NULL WHERE downstream_barrier_id_15_sar = '{}'"
psql -c "UPDATE cwf.segmented_streams SET downstream_barrier_id_20_sar = NULL WHERE downstream_barrier_id_20_sar = '{}'"
psql -c "UPDATE cwf.segmented_streams SET downstream_barrier_id_30_sar = NULL WHERE downstream_barrier_id_30_sar = '{}'"

# report on results
psql2csv < sql/11_report_structureless.sql > ../outputs/wsg_prioritize.csv
psql2csv < sql/11_report_structureless_sar.sql > ../outputs/wsg_prioritize_sar.csv


psql2csv < sql/13_dci.sql > ../outputs/dci.csv
psql2csv < sql/13_dci_sar.sql > ../outputs/dci_sar.csv