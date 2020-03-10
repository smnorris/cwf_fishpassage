# CWF Fish Passage Watershed Prioritization

Scripts to inform prioritization of watershed groups for CWF Fish Passage work.


## Requirements

- bcdata
- Postgresql/PostGIS and a FWA database loaded via `fwapg`


## Barriers (large dams and waterfalls)

Create a `barriers` table by combining falls (>5m) and dams from several datasets:

1. large dams (CWF)
2. waterfalls (CanVec, WDD via CWF)
3. FWA Obstructions:
    - layer: `WHSE_BASEMAPPING.FWA_OBSTRUCTIONS_SP`
    - query:  `obstruction_type = 'Falls'`
4. FISS Obstacles:
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

2. Ensure you have the Falls and Dams shapefiles loaded on your system and adjust the path in the `PROJECT` variable in the `barriers.sh` script accordingly.

3. Run the script:
`./barriers.sh`
