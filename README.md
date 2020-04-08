# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.

## Requirements

- Postgresql/PostGIS and a FWA database loaded via `fwapg`
- bcdata
- psql2csv
- table `gradient_barriers` - potential barriers at various percentage gradient, generated for BC Fish Passage

## Setup

Set an environment variable `$PGOGR` with a value that points to your database. For example:

    export PGOGR='host=localhost user=postgres dbname=mydatabase password=postgres port=5432'

If necessary, load the latest dam data to `/inputs/large_dams_bc.geojson`

## Load required data

Create the `cwf` schema and load required data:

    ./01_load.sh

Load table `gradient_barriers` to schema `cwf`

# Prioritization Steps


## 1. Refine set of watershed groups for modelling/prioritization

From the 256 watershed groups in BC, select groups that are likely to support the species of interest
(CH, CO, SK, ST). This was primarily a manual task based on review of literature and various datasets.

Considerations that removed most watersheds are:
- do not include watersheds that drain into the Peace/Mackenzie
- do not include watersheds above Chief Joseph Dam (USA, Columbia River)
- do not include watersheds above the Ross Dam (USA, Skagit River)

As subsequent modelling of fish passage is conducted on a per watershed group basis, we can also
support the initial watershed selection by generating a report of complete watershed groups upstream of known/likely barriers,
defined as large dams (from CWF) and falls > 5m (from BC Fish Obstacles). To run:

    ./02_wsg_upstream_of_barriers.sh

This script:

- loads latest fiss obstacles data
- loads large dams from file
- creates a barrier table (matching input dams and falls to nearest stream within 50m)
- finds watershed groups upstream of the barriers and writes output to `outputs/wsg_upstream_of_barriers.csv`

Also see queries in `sql/01_choose_wsg/archive` for reporting on fish ranges and number of observations per watershed group.

## 2. Report on length of stream available per watershed group

For prioritization of watershed groups for further work, report on the maximum potential length of stream available to anadramous species per group.

This is simply all streams in the network that are:

- not upstream of a major dam
- not upstream of a >=100m section of stream of >=15% or >=20% grade (depending on the species of interest present within the watershed group)
- not upstream of a subsurface flow line

To generate:

    python model.py barriers-create
    psql -t -P border=0,footer=no -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" | sed -e '$d' | parallel python model.py barriers-index
    python model.py barriers-cleanup
    psql -t -P border=0,footer=no -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" | sed -e '$d' | parallel python model.py split-streams
    python model.py create-output

