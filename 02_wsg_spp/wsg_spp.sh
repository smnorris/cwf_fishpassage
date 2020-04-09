#!/bin/bash -x

# create barriers table
psql -f sql/01_create_barriers.sql

# qa watershed groups above barriers
psql2csv < sql/02_barrier_qa.sql > outputs/barrier_qa.csv

# create wsg_upstream_of_barriers table
psql -f sql/03_wsg_upstream_of_barriers.sql

# generate report
psql2csv < sql/04_wsg_report.sql > outputs/wsg_report.csv