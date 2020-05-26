SELECT
 watershed_group_code,
 Round((SUM(ST_Length(geom)) / 1000)::numeric) as length_km
FROM

(SELECT
 watershed_group_code,
 CASE
   WHEN ST_CoveredBy(p.geom, dra.geom) THEN dra.geom
   ELSE ST_Safe_Intersection(p.geom, dra.geom)
 END AS geom
FROM whse_basemapping.fwa_watershed_groups_subdivided p
INNER JOIN whse_basemapping.dra_dgtl_road_atlas_mpar_sp dra
ON ST_Intersects(p.geom, dra.geom)
AND NOT ST_Touches(p.geom, dra.geom)
AND road_class NOT IN ('trail', 'ferry', 'proposed', 'water')
AND watershed_group_code IN (SELECT watershed_group_code from cwf.target_watershed_groups)
) as f
GROUP BY
  watershed_group_code;
