-- First, find area of each of lakes/wetlands/reservoirs
WITH area_lake_total AS
(
SELECT
  watershed_group_code,
  sum(st_area(geom)) / 10000 as lake_area_ha_total
FROM whse_basemapping.fwa_lakes_poly
GROUP BY watershed_group_code
ORDER BY watershed_group_code
),

area_wetland_total AS
(
SELECT
  watershed_group_code,
  sum(st_area(geom)) / 10000 as wetland_area_ha_total
FROM whse_basemapping.fwa_wetlands_poly
GROUP BY watershed_group_code
ORDER BY watershed_group_code
),

area_reservoir_total AS
(
SELECT
  watershed_group_code,
  sum(st_area(geom)) / 10000 as reservoir_area_ha_total
FROM whse_basemapping.fwa_manmade_waterbodies_poly
GROUP BY watershed_group_code
ORDER BY watershed_group_code
),

-- combine the three totals
area_wb_total AS
(
SELECT
  wsg.watershed_group_code,
  round(l.lake_area_ha_total::numeric, 1) as lake_area_ha_total,
  round(w.wetland_area_ha_total::numeric, 1) as wetland_area_ha_total,
  round(r.reservoir_area_ha_total::numeric, 1) as reservoir_area_ha_total
FROM
-- just in case there are watershed groups with no lakes?
(
  SELECT DISTINCT watershed_group_code
  FROM whse_basemapping.fwa_watershed_groups_poly
) AS wsg
LEFT OUTER JOIN area_lake_total l ON wsg.watershed_group_code = l.watershed_group_code
LEFT OUTER JOIN area_wetland_total w ON wsg.watershed_group_code = w.watershed_group_code
LEFT OUTER JOIN area_reservoir_total r ON wsg.watershed_group_code = r.watershed_group_code
),

-- Now, find minimum stream segment in each waterbody
-- note that this is probably unreliable for wetlands
-- (a single wetland can drain to multiple locations)
min_segments AS
(SELECT DISTINCT ON (watershed_group_code, waterbody_key)
  s.watershed_group_code,
  s.waterbody_key,
  wb.waterbody_type,
  fwa_watershed_code,
  downstream_route_measure
FROM cwf.segmented_streams s
INNER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
WHERE wb.waterbody_type != 'R'
ORDER BY watershed_group_code, waterbody_key, fwa_watershed_code, downstream_route_measure
),

-- get the downstream_ids for the minimum stream segment in a waterbody
-- (this ensures that gradient barriers or structures that happen to be mis-coded as within
-- a waterbody are not included, we just get the downstream_ids at the drainage pt of the wb)
distinct_wb AS
(SELECT DISTINCT
  m.watershed_group_code,
  m.waterbody_key,
  s.downstream_barrier_id_15,
  s.downstream_barrier_id_20,
  s.downstream_barrier_id_30,
  s.downstream_barrier_id_structure,
  m.waterbody_type
FROM min_segments m
INNER JOIN cwf.segmented_streams s
ON m.waterbody_key = s.waterbody_key
AND m.fwa_watershed_code = s.fwa_watershed_code
AND m.downstream_route_measure = s.downstream_route_measure
),

-- sum the areas of lakes above various barrier types
area_lake AS
(SELECT
 a.watershed_group_code,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible15,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible15_structureless,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible20,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible20_structureless,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible30,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible30_structureless
FROM distinct_wb a
LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly lk
ON a.waterbody_key = lk.waterbody_key
AND a.watershed_group_code = lk.watershed_group_code
WHERE a.waterbody_type = 'L'
GROUP BY a.watershed_group_code
ORDER BY a.watershed_group_code
),

-- sum the areas of wetlands above various barrier types
area_wetland AS
(
SELECT
 a.watershed_group_code,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible15,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible15_structureless,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible20,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible20_structureless,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible30,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible30_structureless
FROM distinct_wb a
LEFT OUTER JOIN whse_basemapping.fwa_wetlands_poly wl
ON a.waterbody_key = wl.waterbody_key
AND a.watershed_group_code = wl.watershed_group_code
WHERE a.waterbody_type = 'W'
GROUP BY a.watershed_group_code
ORDER BY a.watershed_group_code
),

-- sum the areas of reservoirs above various barrier types
area_reservoir AS
(
 SELECT
 a.watershed_group_code,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_15,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_15_structureless,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_20,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_20_structureless,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_30,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_30 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_30_structureless
FROM distinct_wb a
LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly res
ON a.waterbody_key = res.waterbody_key
AND a.watershed_group_code = res.watershed_group_code
WHERE a.waterbody_type = 'X'
GROUP BY a.watershed_group_code
ORDER BY a.watershed_group_code
),

linear AS
(
SELECT
  watershed_group_code,
  COALESCE(round((sum(st_length(s.geom)) / 1000)::numeric, 1), 0) as length_km_total,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible15,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND s.downstream_barrier_id_structure IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible15_structureless,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible20,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND s.downstream_barrier_id_structure IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible20_structureless,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_30 IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible30,
  COALESCE(round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_30 IS NULL AND s.downstream_barrier_id_structure IS NULL) / 1000)::numeric, 1), 0) AS length_km_accessible30_structureless,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible15,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND s.downstream_barrier_id_structure IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible15_structureless,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible20,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND s.downstream_barrier_id_structure IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible20_structureless,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_30 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible30,
  COALESCE(round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_30 IS NULL AND s.downstream_barrier_id_structure IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1), 0) AS stream_km_accessible30_structureless

FROM cwf.segmented_streams s
LEFT OUTER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
GROUP BY watershed_group_code
),

-- because the streams are broken at modelled crossings, which are
-- matched to PSCIS points...measures can be almost exactly the same,
-- resulting in imprecise matching, especially given our .001 fudge
-- factor. So, just select on distinct ids.
pscis AS
(
  SELECT DISTINCT ON (e.stream_crossing_id)
   e.stream_crossing_id,
   e.watershed_group_code,
   CASE
    WHEN hc.stream_crossing_id IS NOT NULL
     THEN 'HABITAT CONFIRMATION'
     ELSE p.current_pscis_status
   END AS pscis_status,
   p.current_barrier_result_code,
   ass.assessment_date,
   s.downstream_barrier_id_15,
   s.downstream_barrier_id_20,
   s.downstream_barrier_id_30

  FROM whse_fish.pscis_events e

  INNER JOIN whse_fish.pscis_points_all p
  ON e.stream_crossing_id = p.stream_crossing_id

  LEFT OUTER JOIN whse_fish.pscis_habitat_confirmation_svw hc
  ON e.stream_crossing_id = hc.stream_crossing_id

  LEFT OUTER JOIN whse_fish.pscis_assessment_svw ass
  ON e.stream_crossing_id = ass.stream_crossing_id

  INNER JOIN cwf.segmented_streams s
  ON e.blue_line_key = s.blue_line_key
  AND e.downstream_route_measure > s.downstream_route_measure - .001
  AND e.downstream_route_measure < s.upstream_route_measure + .001
  ORDER BY e.stream_crossing_id, s.downstream_route_measure
),

pscis_summary AS
(
  SELECT
   watershed_group_code,
   count(stream_crossing_id) AS n_pscis_all,
   count(stream_crossing_id) FILTER (WHERE assessment_date >= '2010-01-01' OR assessment_date IS NULL) AS n_pscis_gt2010,
   count(stream_crossing_id) FILTER (WHERE downstream_barrier_id_15 IS NULL) as n_pscis_gt2010_accessible_15,
   count(stream_crossing_id) FILTER (WHERE downstream_barrier_id_20 IS NULL) as n_pscis_gt2010_accessible_20,
   count(stream_crossing_id) FILTER (WHERE downstream_barrier_id_30 IS NULL) as n_pscis_gt2010_accessible_30
  FROM pscis
  GROUP BY watershed_group_code
),

potential_barriers AS
(
  SELECT DISTINCT ON (p.barrier_id)
   p.barrier_id,
   p.watershed_group_code,
   s.downstream_barrier_id_15,
   s.downstream_barrier_id_20,
   s.downstream_barrier_id_30
  FROM cwf.barriers_structures p
  INNER JOIN cwf.segmented_streams s
  ON p.linear_feature_id = s.linear_feature_id
  AND p.downstream_route_measure > s.downstream_route_measure - .001
  AND p.downstream_route_measure < s.upstream_route_measure + .001
  WHERE p.blue_line_key = s.watershed_key
  ORDER BY p.barrier_id, s.downstream_route_measure
 ),

crossing_summary AS
(
  SELECT
  watershed_group_code,
  count(barrier_id) as n_modelled_culverts_total,
  count(barrier_id) FILTER (WHERE downstream_barrier_id_15 IS NULL) as n_modelled_culverts_accessible15,
  count(barrier_id) FILTER (WHERE downstream_barrier_id_20 IS NULL) as n_modelled_culverts_accessible20,
  count(barrier_id) FILTER (WHERE downstream_barrier_id_30 IS NULL) as n_modelled_culverts_accessible30
FROM potential_barriers
GROUP BY watershed_group_code
)

SELECT
  linear.*,
  coalesce(area_wb_total.lake_area_ha_total, 0) + coalesce(area_wb_total.reservoir_area_ha_total, 0) as lake_reservoir_area_ha_total,
  coalesce(area_lake.lake_area_ha_accessible15, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_15, 0) as lake_reservoir_area_ha_accessible15,
  coalesce(area_lake.lake_area_ha_accessible15_structureless, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_15_structureless, 0) as lake_reservoir_area_ha_accessible15_structureless,
  coalesce(area_lake.lake_area_ha_accessible20, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_20, 0) as lake_reservoir_area_ha_accessible20,
  coalesce(area_lake.lake_area_ha_accessible20_structureless, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_20_structureless, 0) as lake_reservoir_area_ha_accessible20_structureless,
  coalesce(area_lake.lake_area_ha_accessible30, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_30, 0) as lake_reservoir_area_ha_accessible30,
  coalesce(area_lake.lake_area_ha_accessible30_structureless, 0) + coalesce(area_reservoir.reservoir_area_ha_accessible_30_structureless, 0) as lake_reservoir_area_ha_accessible30_structureless,
  coalesce(area_wb_total.wetland_area_ha_total, 0) AS wetland_area_ha_total,
  coalesce(area_wetland.wetland_area_ha_accessible15, 0) AS wetland_area_ha_accessible15,
  coalesce(area_wetland.wetland_area_ha_accessible15_structureless, 0) AS wetland_area_ha_accessible15_structureless,
  coalesce(area_wetland.wetland_area_ha_accessible20, 0) AS wetland_area_ha_accessible20,
  coalesce(area_wetland.wetland_area_ha_accessible20_structureless, 0) AS wetland_area_ha_accessible20_structureless,
  coalesce(area_wetland.wetland_area_ha_accessible30, 0) AS wetland_area_ha_accessible30,
  coalesce(area_wetland.wetland_area_ha_accessible30_structureless, 0) AS wetland_area_ha_accessible30_structureless,
  coalesce(p.n_pscis_all, 0) AS n_pscis_all,
  coalesce(p.n_pscis_gt2010, 0) AS n_pscis_gt2010,
  coalesce(p.n_pscis_gt2010_accessible_15, 0) AS n_pscis_gt2010_accessible_15,
  coalesce(p.n_pscis_gt2010_accessible_20, 0) AS n_pscis_gt2010_accessible_20,
  coalesce(p.n_pscis_gt2010_accessible_30, 0) AS n_pscis_gt2010_accessible_30,
  coalesce(x.n_modelled_culverts_total, 0) AS n_modelled_culverts_total,
  coalesce(x.n_modelled_culverts_accessible15, 0) AS n_modelled_culverts_accessible15,
  coalesce(x.n_modelled_culverts_accessible20, 0) AS n_modelled_culverts_accessible20,
  coalesce(x.n_modelled_culverts_accessible30, 0) AS n_modelled_culverts_accessible30
FROM linear
INNER JOIN area_wb_total
ON linear.watershed_group_code = area_wb_total.watershed_group_code
LEFT OUTER JOIN area_lake
ON linear.watershed_group_code = area_lake.watershed_group_code
LEFT OUTER JOIN area_wetland
ON linear.watershed_group_code = area_wetland.watershed_group_code
LEFT OUTER JOIN area_reservoir
ON linear.watershed_group_code = area_reservoir.watershed_group_code
LEFT OUTER JOIN pscis_summary p
ON linear.watershed_group_code = p.watershed_group_code
LEFT OUTER JOIN crossing_summary x
ON linear.watershed_group_code = x.watershed_group_code