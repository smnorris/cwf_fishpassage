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
- [BC Fish Ranges](https://catalogue.data.gov.bc.ca/dataset/provincial-fish-ranges-watersheds) (available from BC Fish Passage technical working group as automated download is not possible)
- BC Fish Passage gradient barrier tables (from BC Fish Passage technical working group):

        fish_passage.gradient_barriers_150
        fish_passage.gradient_barriers_200
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
(Chinook: `CH`, Sockeye: `SK`, Steelhead: `ST`, Coho:`CO`). This is primarily a manual review, but we can support the review by reporting several indicators for each watershed group:

1. How many priority species observations occur within the watershed group (total)?
2. How many priority species observations occur within the watershed group (since 1990)?
3. What is the most recent observation of priority species within the watershed group?
4. Is the watershed group part of the Mackenzie system?
5. What is the BC Fish Ranges classification for the watershed group?
6. Is the (entire) watershed group upstream of a major barrier? Defined as:
    - BC large dams (CWF, [large_dams_bc.geojson](inputs/large_dams_bc.geojson))
    - Falls > 5m (Province of BC, [FISS Obstacles](https://catalogue.data.gov.bc.ca/dataset/provincial-obstacles-to-fish-passage))
    - the Chief Joseph Dam (modelled as a point at the confluence of the Columbia and the Okanagan)
    - the Ross Dam (simply defined as the SKGT watershed group)

To generate the report:

    cd 02_wsg_spp
    ./wsg_spp.sh

See output: [outputs/01_wsg_spp.csv](outputs/01_wsg_spp.csv)

### B. Rank watershed groups for further investigation

For prioritization of watershed groups for further work, report on the maximum potential length of stream (and area of waterbodies) available to anadramous species per group.

This is simply all streams/lakes/wetlands in the network that are:

- not upstream of a major dam
- not upstream of a >=100m section of stream of >=15% or >=20% grade (depending on the species of interest present within the watershed group)
- not upstream of a subsurface flow line

To generate the report:

    cd 03_wsg_prioritize
    ./wsg_prioritize.sh


