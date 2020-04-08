#!/bin/bash -x

# create barriers table for this analysis
psql -f sql/01_create_barriers.sql

# list watershed groups above the barriers
psql2csv < sql/02_wsg_upstream_of_barriers.sql > ../outputs/wsg_upstream_of_barriers.csv

