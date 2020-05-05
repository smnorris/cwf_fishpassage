WITH distinct_wb AS
(SELECT DISTINCT
  s.watershed_group_code,
  s.waterbody_key,
  s.downstream_barrier_id_15,
  s.downstream_barrier_id_20,
  s.downstream_barrier_id_structure,
  wb.waterbody_type
FROM cwf.segmented_streams s
INNER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
),

areal AS
(SELECT
 a.watershed_group_code,
 round((sum(st_area(lk.geom)) / 10000)::numeric, 1) as lake_area_ha_total,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible15,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible15_structureless,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible20,
 round((sum(st_area(lk.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as lake_area_ha_accessible20_structureless,
 round((sum(st_area(wl.geom)) / 10000)::numeric, 1) as wetland_area_ha_total,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible15,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible15_structureless,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible20,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible20_structureless,
 round((sum(st_area(res.geom)) / 10000)::numeric, 1) as reservoir_area_ha_total,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_15,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_15_structureless,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_20,
 round((sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_20 IS NULL AND a.downstream_barrier_id_structure IS NULL) / 10000)::numeric, 1) as reservoir_area_ha_accessible_20_structureless
FROM distinct_wb a
LEFT OUTER JOIN whse_basemapping.fwa_lakes_poly lk
ON a.waterbody_key = lk.waterbody_key
AND a.watershed_group_code = lk.watershed_group_code
LEFT OUTER JOIN whse_basemapping.fwa_wetlands_poly wl
ON a.waterbody_key = wl.waterbody_key
AND a.watershed_group_code = wl.watershed_group_code
LEFT OUTER JOIN whse_basemapping.fwa_manmade_waterbodies_poly res
ON a.waterbody_key = res.waterbody_key
AND a.watershed_group_code = res.watershed_group_code
GROUP BY a.watershed_group_code
ORDER BY a.watershed_group_code
),

linear AS
(
SELECT
  watershed_group_code,
  round((sum(st_length(s.geom)) / 1000)::numeric, 1) as length_km_total,
  round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL) / 1000)::numeric, 1) AS length_km_accessible15,
  round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND s.downstream_barrier_id_structure IS NULL) / 1000)::numeric, 1) AS length_km_accessible15_structureless,
  round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL) / 1000)::numeric, 1) AS length_km_accessible20,
  round((sum(st_length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND s.downstream_barrier_id_structure IS NULL) / 1000)::numeric, 1) AS length_km_accessible20_structureless,
  round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1) AS stream_km_accessible15,
  round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND s.downstream_barrier_id_structure IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1) AS stream_km_accessible15_structureless,
  round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1) AS stream_km_accessible20,
  round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_20 IS NULL AND s.downstream_barrier_id_structure IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1) AS stream_km_accessible20_structureless
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'L' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS lake_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'W' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS wetland_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'X' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS reservoir_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type IS NULL AND s.edge_type IN (1200, 1400, 1410, 1425, 6010) AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS other_km,
FROM cwf.segmented_streams s
LEFT OUTER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
GROUP BY watershed_group_code
),

-- because the streams are broken and modelled crossings, which are
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
   s.downstream_barrier_id_20

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
   count(stream_crossing_id) FILTER (WHERE downstream_barrier_id_20 IS NULL) as n_pscis_gt2010_accessible_20
  FROM pscis
  GROUP BY watershed_group_code
),

modelled_culverts AS
(
  SELECT DISTINCT ON (p.crossing_id)
   p.crossing_id,
   p.watershed_group_code,
   s.downstream_barrier_id_15,
   s.downstream_barrier_id_20
  FROM fish_passage.road_stream_crossings_culverts p
  INNER JOIN cwf.segmented_streams s
  ON p.linear_feature_id = s.linear_feature_id
  AND p.downstream_route_measure > s.downstream_route_measure - .001
  AND p.downstream_route_measure < s.upstream_route_measure + .001
  WHERE p.blue_line_key = s.watershed_key
  -- don't include crossings that have been determined to be open bottom/non-existent
  AND p.crossing_id NOT IN (SELECT source_id FROM cwf.modelled_culverts_qa)
  ORDER BY p.crossing_id, s.downstream_route_measure
 ),

crossing_summary AS
(
  SELECT
  watershed_group_code,
  count(crossing_id) as n_modelled_culverts_total,
  count(crossing_id) FILTER (WHERE downstream_barrier_id_15 IS NULL) as n_modelled_culverts_accessible15,
  count(crossing_id) FILTER (WHERE downstream_barrier_id_20 IS NULL) as n_modelled_culverts_accessible20
FROM modelled_culverts
GROUP BY watershed_group_code
)

SELECT
linear.*,
areal.lake_area_ha_total + coalesce(areal.reservoir_area_ha_total, 0) as lake_reservoir_area_ha_total,
areal.lake_area_ha_accessible15 + coalesce(areal.reservoir_area_ha_accessible_15, 0) as lake_reservoir_area_ha_accessible15,
areal.lake_area_ha_accessible15_structureless + coalesce(areal.reservoir_area_ha_accessible_15_structureless, 0) as lake_reservoir_area_ha_accessible15_structureless,
areal.lake_area_ha_accessible15 + coalesce(areal.reservoir_area_ha_accessible_20, 0) as lake_reservoir_area_ha_accessible20,
areal.lake_area_ha_accessible15_structureless + coalesce(areal.reservoir_area_ha_accessible_20_structureless, 0) as lake_reservoir_area_ha_accessible20_structureless,
areal.wetland_area_ha_total,
areal.wetland_area_ha_accessible15,
areal.wetland_area_ha_accessible15_structureless,
areal.wetland_area_ha_accessible20,
areal.wetland_area_ha_accessible20_structureless,
p.n_pscis_all,
p.n_pscis_gt2010,
p.n_pscis_gt2010_accessible_15,
p.n_pscis_gt2010_accessible_20,
x.n_modelled_culverts_total,
x.n_modelled_culverts_accessible15,
x.n_modelled_culverts_accessible20
FROM linear
INNER JOIN areal
ON linear.watershed_group_code = areal.watershed_group_code
LEFT OUTER JOIN pscis_summary p
ON linear.watershed_group_code = p.watershed_group_code
LEFT OUTER JOIN crossing_summary x
ON linear.watershed_group_code = x.watershed_group_code