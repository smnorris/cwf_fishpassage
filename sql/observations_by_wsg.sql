-- report on watershed groups with more than 5 observations of spp of interest since 1990
-- select directly from source observation data, but only use 'Observations', not summaries

SELECT
  wsg.watershed_group_code,
  count(*) as n_obs
FROM whse_fish.fiss_fish_obsrvtn_pnt_sp obs
INNER JOIN whse_basemapping.fwa_watershed_groups_subdivided wsg
ON ST_Within(obs.geom, wsg.geom)
WHERE species_code in ('CH', 'CO', 'SK', 'ST')
AND observation_date >= DATE('1990-01-01')
AND point_type_code = 'Observation'
GROUP BY wsg.watershed_group_code
HAVING count(*) >= 5;
