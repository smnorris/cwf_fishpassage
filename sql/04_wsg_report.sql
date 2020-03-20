-- generate a report on the 3 criteria of interest for each watershed group

-- Primary output columns of interest
-- 1. obs_gt5_ind: >= 5 observations since Jan 01 1990 for at least 1 of 4 priority spp ()
-- 2. mackenzie_ind: is watershed group a part of the mackenzie system?
-- 3. barrier_ind: is the watershed group (entirely) above a point in the barriers table
--    (falls > 5m and large dams)

-- First, select observations directly from the source table
-- Do not include 'Summary' records
-- only include observations since 1990
WITH obs AS
(
    SELECT * FROM
    whse_fish.fiss_fish_obsrvtn_pnt_sp
    WHERE observation_date >= DATE('1990-01-01')
    AND point_type_code = 'Observation'
),

-- generate a count of observations by species,
-- and report on the latest observation
obs_by_wsg AS
(
    SELECT
      wsg.watershed_group_code,
      count(*) FILTER (WHERE obs.species_code = 'CH') as ch_n,
      max(observation_date) FILTER (WHERE obs.species_code = 'CH') as ch_latest,
      count(*) FILTER (WHERE obs.species_code = 'CO') as co_n,
      max(observation_date) FILTER (WHERE obs.species_code = 'CO') as co_latest,
      count(*) FILTER (WHERE obs.species_code = 'SK') as sk_n,
      max(observation_date) FILTER (WHERE obs.species_code = 'SK') as sk_latest,
      count(*) FILTER (WHERE obs.species_code = 'ST') as st_n,
      max(observation_date) FILTER (WHERE obs.species_code = 'ST') as st_latest
    FROM whse_basemapping.fwa_watershed_groups_subdivided wsg
    LEFT OUTER JOIN  obs
    ON ST_Within(obs.geom, wsg.geom)
    GROUP BY wsg.watershed_group_code
    ORDER BY watershed_group_code
),

-- note all watershed gropus in the mackenzie drainage
mackenzie AS
(
    SELECT
        watershed_group_code
    FROM whse_basemapping.fwa_stream_networks_sp
    WHERE wscode_ltree <@ '200'::ltree
    GROUP BY watershed_group_code
    HAVING count(linear_feature_id) > 100
    ORDER BY watershed_group_code
),

-- combine above and join to wsg_upstream_of_barriers to generate
-- the output criteria columns
indicators AS
(
    SELECT
     o.*,
     CASE
       WHEN ch_n >= 5 OR co_n >= 5 OR st_n >= 5 OR st_n >= 5
       THEN True
     END AS obs_gt5_ind,
     CASE
       WHEN mackenzie.watershed_group_code IS NOT NULL
       THEN True
     END AS mackenzie_ind,
    b.barrier_name as barrier_ind
    FROM obs_by_wsg o
    LEFT OUTER JOIN mackenzie
    ON o.watershed_group_code = mackenzie.watershed_group_code
    LEFT OUTER JOIN cwf.wsg_upstream_of_barriers b
    ON o.watershed_group_code = b.watershed_group_code
    ORDER BY watershed_group_code
),

-- put everything together, adding a column noting whether group is in or out of analysis
consider AS
(
  SELECT
    *,
    CASE
    WHEN obs_gt5_ind = 'y' AND mackenzie_ind IS NULL AND barrier_ind IS NULL THEN True
    END as consider_wsg
  FROM indicators
)

-- define the max passable slope to be modelled in the group
-- (20% if Steelhead are present, 15% otherwise)
SELECT *,
CASE
    WHEN consider_wsg = 'y' AND st_n >= 5 THEN 20
    WHEN consider_wsg = 'y' AND st_n < 5 THEN 15
END as model_barrier_gradient
FROM consider
