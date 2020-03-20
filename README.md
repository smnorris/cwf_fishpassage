# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.

## Method

Select all watershed groups meeting these criteria:

1. Within the watershed group, there are >= 5 observations since Jan. 1, 1990 for at least 1 of the 4 priority species (Chinook, Sockeye, Steelhead, Coho)
2. The watershed group is not a part of the Mackenzie system
3. The (entire) watershed group is not upstream of a major barrier, defined as:
    - BC large dams (CWF, [large_dams_bc.geojson](inputs/large_dams_bc.geojson))
    - Falls > 5m (Province of BC, [FISS Obstacles](https://catalogue.data.gov.bc.ca/dataset/provincial-obstacles-to-fish-passage))
    - the Chief Joseph Dam (modelled as a point at the confluence of the Columbia and the Okanagan)
4. Finally, a watershed be included or excluded based on review of additional datasets and literature

## Requirements

- bcdata
- Postgresql/PostGIS and a FWA database loaded via `fwapg`
- BC fish observation data loaded to the postgres db via `bcfishobs`
- a copy of BC Fish Ranges dataset [`WHSE_FISH.FISS_FISH_RANGES_SVW`](https://catalogue.data.gov.bc.ca/dataset/provincial-fish-ranges-watersheds)
  Note that the Fish Ranges layer is larger than the maximum permitted by the DataBC Catalogue download service and not published via WFS. You may need to request this data from the Province. This script presumes the file is downloaded as file geodatabase to `inputs\WHSE_FISH.gdb\FISS_FISH_RANGES_SVW`


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
- load the fish ranges data
- create barriers table (matching dams and falls to nearest stream within 50m)
- dump QA of barriers to `outputs/wsg_upstream_of_barriers.csv`
- create a table listing watershed groups upstream of large barriers `cwf.wsg_upstream_of_barriers`
- run a query reporting on which watershed groups match the criteria noted in Methods above

## Output

See results in file `outputs/wsg_report.csv`.
The selection criteria are noted in the columns:

1. `obs_gt5_ind`
2. `mackenzie_ind`
3. `barrier_ind`
4. `manual_review_ind`

These criteria are combined into the final `consider_wsg` column - consider the watershed group for further analysis if this is column is true.
Note that fish ranges columns for each species of interest are added for QA.