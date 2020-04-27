#!/bin/bash -x

# create barriers table
psql -f sql/01_create_barriers_1.sql

# qa watershed groups above barriers
psql2csv < sql/02_barrier_qa.sql > ../outputs/wsg_upstream_of_barriers.csv

# create wsg_upstream_of_barriers table
psql -f sql/03_wsg_upstream_of_barriers.sql

# generate report
psql2csv < sql/04_wsg_report.sql > ../outputs/01_wsg_spp.csv

# drop tables created for this report to avoid confusion with other barrier processing
psql -c "DROP TABLE cwf.wsg_upstream_of_barriers"
psql -c "DROP TABLE cwf.barriers_1"