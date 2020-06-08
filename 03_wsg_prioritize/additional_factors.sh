# extra queries, unrelated to streams

# road density
psql2csv < sql/12_road_density.sql > ../outputs/road_density.csv

# total land cover alteration
psql2csv "WITH tlca AS
(SELECT
  watershed_group_code,
  SUM(st_area(geom)) / 10000 as area_tlca_ha
FROM cwf.tlca_union
GROUP BY watershed_group_code
)
SELECT
 a.watershed_group_code,
 ROUND((COALESCE(b.area_tlca_ha, 0))::numeric, 2) as area_tlca_ha,
 ROUND((ST_Area(a.geom) / 10000)::numeric, 2) as area_total_ha,
 ROUND(((COALESCE(b.area_tlca_ha, 0) / (ST_Area(a.geom) / 10000)) * 100)::numeric, 2) as pct_tlca
FROM whse_basemapping.fwa_watershed_groups_poly a
LEFT OUTER JOIN tlca b ON a.watershed_group_code = b.watershed_group_code
ORDER BY a.watershed_group_code" > ../outputs/tlca.csv

# water licenses
psql2csv "SELECT
  b.watershed_group_code,
  count(*)
FROM whse_water_management.wls_water_rights_licences_sv a
INNER JOIN whse_basemapping.fwa_watershed_groups_subdivided b
ON ST_Intersects(a.geom, b.geom)
WHERE a.licence_status = 'Current'
GROUP BY b.watershed_group_code" > ../outputs/water_license.csv