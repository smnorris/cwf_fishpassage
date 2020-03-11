# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.


## Requirements

- bcdata
- Postgresql/PostGIS and a FWA database loaded via `fwapg`


## Barriers (large dams and waterfalls)

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

2. Ensure you have the Falls and Dams shapefiles loaded to `/inputs` folder or adjust the paths in `barriers.sh` accordingly.

3. Run the script

    `./barriers.sh`

4. Report on watershed groups upstream of barriers, dumping to csv:

    `psql2csv < sql/wsg_upstream_of_barriers.sql > outputs/wsg_upstream_of_barriers.csv`