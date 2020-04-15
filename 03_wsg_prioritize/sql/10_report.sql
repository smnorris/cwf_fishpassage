WITH distinct_wb AS
(SELECT DISTINCT
  s.watershed_group_code,
  s.waterbody_key,
  s.downstream_barrier_id_15,
  wb.waterbody_type
FROM cwf.segmented_streams s
INNER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
),

areal AS
(SELECT
 a.watershed_group_code,
 round((sum(st_area(lk.geom)) + sum(st_area(res.geom)) / 10000)::numeric, 1) as lake_reservoir_area_ha_total,
 round((sum(st_area(lk.geom)) + sum(st_area(res.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as lake_reservoir_area_ha_accessible15,
 round((sum(st_area(wl.geom)) / 10000)::numeric, 1) as wetland_area_ha_total,
 round((sum(st_area(wl.geom)) FILTER (WHERE a.downstream_barrier_id_15 IS NULL) / 10000)::numeric, 1) as wetland_area_ha_accessible15
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
  round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND (wb.waterbody_type IS NULL AND s.edge_type IN (1000,1100,2000,2300)) OR wb.waterbody_type = 'R') / 1000)::numeric, 1) AS stream_km_accessible15
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'L' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS lake_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'W' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS wetland_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type = 'X' AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS reservoir_km,
 -- round((SUM(ST_Length(s.geom)) FILTER (WHERE s.downstream_barrier_id_15 IS NULL AND wb.waterbody_type IS NULL AND s.edge_type IN (1200, 1400, 1410, 1425, 6010) AND s.local_watershed_code IS NOT NULL) / 1000)::numeric,1) AS other_km,
FROM cwf.segmented_streams s
LEFT OUTER JOIN whse_basemapping.fwa_waterbodies wb
ON s.waterbody_key = wb.waterbody_key
GROUP BY watershed_group_code
)

SELECT
linear.*,
areal.lake_reservoir_area_ha_total,
areal.lake_reservoir_area_ha_accessible15,
areal.wetland_area_ha_total,
areal.wetland_area_ha_accessible15
from linear
inner join areal
ON linear.watershed_group_code = areal.watershed_group_code