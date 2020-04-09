# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.

## Requirements

- Postgresql/PostGIS (tested with v12.2/3.0.1)
- a FWA database loaded via [`fwapg`](https://github.com/smnorris/fwapg)
- Python >=3.7
- [bcdata](https://github.com/smnorris/bcdata)
- [pgdata](https://github.com/smnorris/pgdata)
- [psql2csv](https://github.com/fphilipe/psql2csv)
- [GNU Parallel](https://www.gnu.org/software/parallel/) (optional, for speed)
- BC Fish Passage gradient barrier tables (from BC Fish Passage technical working group):

        fish_passage.gradient_barriers_030
        fish_passage.gradient_barriers_050
        fish_passage.gradient_barriers_080
        fish_passage.gradient_barriers_150
        fish_passage.gradient_barriers_220
        fish_passage.gradient_barriers_300


## Setup

Set an environment variable `$PGOGR` with a value that points to your database. For example:

    export PGOGR='host=localhost user=postgres dbname=mydatabase password=postgres port=5432'

If necessary, load the latest dam data to `/inputs/large_dams_bc.geojson`

## Load required data

Load all required data, downloading where required:

    cd 01_load
    ./load.sh


# Prioritization Steps


## A. Identify watershed groups supporting species of interest

From the 256 watershed groups in BC, select groups that are likely to support the species of interest
(CH, CO, SK, ST). This was primarily a manual task based on review of literature and various datasets.

Considerations that removed most watersheds are:
- do not include watersheds that drain into the Peace/Mackenzie
- do not include watersheds above Chief Joseph Dam (USA, Columbia River)
- do not include watersheds above the Ross Dam (USA, Skagit River)

As subsequent modelling of fish passage is conducted on a per watershed group basis, we can also
support the initial watershed selection by generating a report of complete watershed groups upstream of known/likely barriers, defined as large dams (from CWF) and falls > 5m (from BC Fish Obstacles). To run:

    cd 02_wsg_spp
    ./wsg_upstream_of_barriers.sh

This script:
- creates a barrier table (matching input dams and falls to nearest stream within 50m)
- finds watershed groups upstream of these barriers and writes output to `outputs/wsg_upstream_of_barriers.csv`


## B. Rank watershed groups for further investigation

For prioritization of watershed groups for further work, report on the maximum potential length of stream (and area of waterbodies) available to anadramous species per group.

This is simply all streams/lakes/wetlands in the network that are:

- not upstream of a major dam
- not upstream of a >=100m section of stream of >=15% or >=20% grade (depending on the species of interest present within the watershed group)
- not upstream of a subsurface flow line

To generate:

    cd 03_wsg_prioritize
    python model.py barriers-create
    psql -t -P border=0,footer=no -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" | sed -e '$d' | parallel python model.py barriers-index
    python model.py barriers-cleanup
    psql -t -P border=0,footer=no -c "SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'" | sed -e '$d' | parallel python model.py split-streams
    python model.py create-output

