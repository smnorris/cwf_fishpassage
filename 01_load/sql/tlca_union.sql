INSERT INTO cwf.tlca_union (map_tile, watershed_feature_id, watershed_group_id,watershed_group_code, geom)
SELECT
  a.map_tile,
  b.watershed_feature_id,
  b.watershed_group_id,
  b.watershed_group_code,
  (ST_Dump(
    ST_Union(
      ST_CollectionExtract(
        CASE
          WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
          ELSE ST_Safe_Intersection(a.geom, b.geom)
        END,
        3
      )
    )
  )).geom as geom
FROM cwf.tlca a
INNER JOIN whse_basemapping.fwa_assessment_watersheds_poly b
ON ST_Intersects(a.geom, b.geom)
WHERE a.map_tile >= :tile1 AND a.map_tile <= :tile2
GROUP BY a.map_tile, b.watershed_feature_id, b.watershed_group_id, b.watershed_group_code;