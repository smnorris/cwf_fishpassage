-- compare model results

with a AS
(SELECT
  watershed_group_code,
  sum(st_length(geom)) / 1000 as total_length_km,
  sum(st_length(geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL) / 1000 AS accessible15_length_km
FROM cwf.segmented_streams s
GROUP BY watershed_group_code),

B AS (SELECT
  watershed_group_code,
  sum(st_length(geom)) / 1000 as total_length_km,
  sum(st_length(geom)) FILTER (WHERE s.fish_habitat IN ('FISH HABITAT - INFERRED - 000-030PCT','FISH HABITAT - INFERRED - 030-050PCT','FISH HABITAT - INFERRED - 050-080PCT','FISH HABITAT - INFERRED - 080-150PCT','FISH HABITAT - OBSERVED - 000-030PCT','FISH HABITAT - OBSERVED - 030-050PCT','FISH HABITAT - OBSERVED - 050-080PCT','FISH HABITAT - OBSERVED - 080-150PCT','FISH HABITAT - OBSERVED - 150-220PCT','FISH HABITAT - OBSERVED - 220-300PCT','FISH HABITAT - OBSERVED - GT300PCT')) / 1000 AS accessible15_obs_length_km
FROM fish_passage.fish_habitat_cwf_salmon s
GROUP BY watershed_group_code)

select
a.watershed_group_code,
round(a.total_length_km) as total_length_km,
round(a.accessible15_length_km) as accessible15_length_km,
round((a.accessible15_length_km / a.total_length_km)::numeric, 4) * 100 as pct_accessible_15,
round(b.accessible15_obs_length_km) as accessible15_obs_length_km,
round((b.accessible15_obs_length_km / a.total_length_km)::numeric, 4) * 100 as pct_accessible_15_obs,
round(b.accessible15_obs_length_km - a.accessible15_length_km) as diff_abs,
round(((b.accessible15_obs_length_km / a.total_length_km) - (a.accessible15_length_km / a.total_length_km))::numeric, 4) * 100  as diff_pct_total,
round(((b.accessible15_obs_length_km - a.accessible15_length_km) / b.accessible15_obs_length_km)::numeric, 4) * 100 as diff_pct_accessible

FROM a
LEFT OUTER JOIN b
ON a.watershed_group_code = b.watershed_Group_code
