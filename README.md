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
- BC Fish Ranges (from BC Fish Passage technical working group)
- BC Fish Passage gradient barrier tables (from BC Fish Passage technical working group):

        fish_passage.gradient_barriers_030
        fish_passage.gradient_barriers_050
        fish_passage.gradient_barriers_080
        fish_passage.gradient_barriers_150
        fish_passage.gradient_barriers_220
        fish_passage.gradient_barriers_300


## Setup

### Environment variables

Scripts depend on several environment variables that point your postgres database:

    export PGHOST=localhost
    export PGPORT=5432
    export PGDATABASE=mydb
    export PGUSER=postgres

    # put these together into a sqlalchemy URL
    # http://docs.sqlalchemy.org/en/latest/core/engines.html
    export DATABASE_URL='postgresql://'$PGUSER'@'$PGHOST':'$PGPORT'/'$PGDATABASE
    # and a OGR compatible string
    export PGOGR='host=localhost user=postgres dbname=mydb password=mypwd port=5432'


### Supporting data files

Make any changes required to files in `/inputs` (large dams, watershed groups of interest)

### Load to postgres

Load required data to postgres (this presumes that FWA data is already loaded via `fwapg`)

    cd 01_load
    ./load.sh


## Prioritization


### A. Identify watershed groups supporting species of interest

From the 256 watershed groups in BC, select groups that are likely to support the species of interest
(Chinook - CH, Sockeye - SK, Steelhead - ST, Coho - CO). This is primarily a manual review, but we can support the review by reporting on watershed groups where:

1. Within the watershed group, there are >= 5 observations since Jan. 1, 1990 for at least 1 of the 4 priority species (CH, CO, SK, ST)
2. The watershed group is not a part of the Mackenzie system
3. The (entire) watershed group is not upstream of a major barrier, defined as:
    - BC large dams (CWF, [large_dams_bc.geojson](inputs/large_dams_bc.geojson))
    - Falls > 5m (Province of BC, [FISS Obstacles](https://catalogue.data.gov.bc.ca/dataset/provincial-obstacles-to-fish-passage))
    - the Chief Joseph Dam (modelled as a point at the confluence of the Columbia and the Okanagan)
    - the Ross Dam (simply defined as the SKGT watershed group)

To generate the report:

    cd 02_wsg_spp
    ./report.sh


### B. Rank watershed groups for further investigation

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

