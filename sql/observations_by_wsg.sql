-- report on n observations and latest observation by watershed group for species of interest

-- Note that we select directly from the source observation data, but only use
-- records of point_type_code='Observation', not 'Summary'

WITH obs AS
(
    SELECT * FROM
    whse_fish.fiss_fish_obsrvtn_pnt_sp
    WHERE observation_date >= DATE('1990-01-01')
    AND point_type_code = 'Observation'
),

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
)

SELECT
 *,
 CASE
   WHEN ch_n >= 5 OR co_n >= 5 OR st_n >= 5 OR st_n >= 5
   THEN True
 END AS obs_gt5_ind
FROM obs_by_wsg