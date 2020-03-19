# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.

## Method

Select all watershed groups where:

1. There are >= 5 observations since Jan. 1, 1990 for at least 1 of the 4 priority species (Chinook, Sockeye, Steelhead, Coho)
2. Remove watershed groups upstream of major barriers, defined as:
    - large dams (CWF)
    - FISS Obstacles:
        - layer: `WHSE_FISH.FISS_OBSTACLES_PNT_SP`
        - query:
        ```
            obstacle_name = 'Falls'
            AND height >= 5
            AND height <> 999
            AND height <> 9999
        ```

## Requirements

- bcdata
- Postgresql/PostGIS and a FWA database loaded via `fwapg`
- BC fish observation data loaded via `bcfishobs`


## Setup

Set an environment variable `$PGOGR` with a value that points to your database. For example:

    export PGOGR='host=localhost user=postgres dbname=mydatabase password=postgres port=5432'

If necessary, load the latest dam data to `/inputs/large_dams_bc.geojson`

## Process

Run the script

    ./report.sh

This script will:

- load latest fiss obstacles data
- load large dams from file
- create barriers table (matching dams and falls to nearest stream within 50m)
- dump QA of barriers to `outputs/wsg_upstream_of_barriers.csv`
- create a table listing watershed groups upstream of large barriers `cwf.wsg_upstream_of_barriers`
- run a query reporting on which watershed groups match the criteria noted in Methods above

## Output

See results in file `outputs/wsg_report.csv`