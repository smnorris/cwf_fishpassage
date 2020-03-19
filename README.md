# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.

Logic: select all watershed groups where:

1. There are >= 5 observations since Jan. 1, 1990 for at least 1 of the 4 priority species (Chinook, Sockeye, Steelhead, Coho)
2. Remove watershed groups upstream of major barriers

## Requirements

- bcdata
- Postgresql/PostGIS and a FWA database loaded via `fwapg`
- BC fish observation data loaded via `bcfishobs`

## Find watershed groups with observations of species of interest

See `sql/observations_by_wsg.sql`

## Load and reference barriers (large dams and waterfalls)

Create a `barriers` table by combining falls (>5m) and dams from several datasets:

1. large dams (CWF)
2. FISS Obstacles:
    - layer: `WHSE_FISH.FISS_OBSTACLES_PNT_SP`
    - query:
    ```
        obstacle_name = 'Falls'
        AND height >= 5
        AND height <> 999
        AND height <> 9999
    ```

To create the barriers table and load above features matched to the nearest stream (within 50m):

1. Set an environment variable `$PGOGR` with a value that points to your database. For example:

    `export PGOGR='host=localhost user=postgres dbname=mydatabase password=postgres port=5432'`

2. Load the latest dam data to `/inputs/large_dams_bc.geojson` (or adjust the path in `barriers.sh`).

3. Run the script

    `./barriers.sh`

4. View results in file `outputs/wsg_upstream_of_barriers.csv`