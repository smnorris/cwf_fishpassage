
DROP TABLE IF EXISTS cwf.barriers_report;

CREATE TABLE cwf.barriers_report AS

-- for points not at existing break-points
-- get upstream length of stream segment on which stream lies
WITH extra_bits AS
(SELECT
  a.source_id,
  a.downstream_route_measure as meas_pt,
  s.downstream_route_measure as dmeas_str,
  s.upstream_route_measure as umeas_str,
  s.upstream_route_measure - a.downstream_route_measure as upstream_length
FROM cwf.barriers_structures a
INNER JOIN cwf.segmented_streams s
ON a.linear_feature_id = s.linear_feature_id
-- join to correct segment, on which point lies
AND a.downstream_route_measure + .001 > s.downstream_route_measure
AND a.downstream_route_measure - .001 < s.upstream_route_measure
-- do not include points that are already at break points
AND abs(s.downstream_route_measure - a.downstream_route_measure) > .01
AND abs(s.upstream_route_measure - a.downstream_route_measure) > .01
WHERE s.downstream_barrier_id_15 IS NULL
),

-- downstream spp *on the same stream only*
downstream_spp AS
(
  SELECT
    barrier_id,
    array_agg(species_code) as species_codes
  FROM
    (
      SELECT DISTINCT
        a.barrier_id,
        unnest(species_codes) as species_code
      FROM cwf.barriers_structures a
      LEFT OUTER JOIN whse_fish.fiss_fish_obsrvtn_events fo
      ON a.blue_line_key = fo.blue_line_key
      AND a.downstream_route_measure > fo.downstream_route_measure
    ) AS f
  GROUP BY barrier_id
),

upstream_spp AS
(
SELECT
  barrier_id,
  array_agg(species_code) as species_codes
FROM
  (
    SELECT DISTINCT
      a.barrier_id,
      unnest(species_codes) as species_code
    FROM cwf.barriers_structures a
    LEFT OUTER JOIN whse_fish.fiss_fish_obsrvtn_events fo
    ON FWA_Upstream(
      a.blue_line_key,
      a.downstream_route_measure,
      a.wscode_ltree,
      a.localcode_ltree,
      fo.blue_line_key,
      fo.downstream_route_measure,
      fo.wscode_ltree,
      fo.localcode_ltree
     )
  ) AS f
GROUP BY barrier_id
),

grade AS
(
SELECT
  a.barrier_id,
  s.gradient
FROM cwf.barriers_structures a
INNER JOIN cwf.segmented_streams s
ON a.linear_feature_id = s.linear_feature_id
AND a.downstream_route_measure > s.downstream_route_measure - .001
AND a.downstream_route_measure + .001 < s.upstream_route_measure
ORDER BY a.barrier_id, s.downstream_route_measure
)

SELECT
  a.barrier_id,
  a.source_id,
  a.barrier_type,
  a.barrier_name,
  a.linear_feature_id,
  a.blue_line_key,
  a.downstream_route_measure,
  a.wscode_ltree,
  a.localcode_ltree,
  a.watershed_group_code,
  t.map_tile_display_name,
  a.downstream_ids,
  s.stream_order,
  g.gradient as upstream_gradient,
  spd.species_codes as downstream_species,
  spu.species_codes as upstream_species,
  -- length accessible @15% (salmon)
  round(((coalesce(sum(st_length(b.geom))
    FILTER (WHERE b.downstream_barrier_id_15 IS NULL), 0)
    + coalesce(e.upstream_length, 0)) / 1000 )::numeric, 2) AS upstream_accessible15_km,
  -- length accessible @20% (salmon/steelhead)
  round(((coalesce(sum(st_length(b.geom))
    FILTER (WHERE b.downstream_barrier_id_20 IS NULL), 0)
    + coalesce(e.upstream_length, 0)) / 1000 )::numeric, 2) AS upstream_accessible20_km,
  -- length accessible @15% (westslope cutthroat -- includes observations)
  round(((coalesce(sum(st_length(b.geom))
    FILTER (WHERE b.downstream_barrier_id_15_sar IS NULL AND b.downstream_barrier_id_20_sar IS NULL AND b.downstream_barrier_id_30_sar IS NULL), 0)
    + coalesce(e.upstream_length, 0)) / 1000 )::numeric, 2) AS upstream_accessible15_sar_km,
  -- length accessible @20% (westslope cutthroat -- includes observations)
  round(((coalesce(sum(st_length(b.geom))
    FILTER (WHERE b.downstream_barrier_id_20_sar IS NULL AND b.downstream_barrier_id_20_sar IS NULL), 0)
    + coalesce(e.upstream_length, 0)) / 1000 )::numeric, 2) AS upstream_accessible20_sar_km,
  -- length accessible @30% (westslope cutthroat -- includes observations)
  round(((coalesce(sum(st_length(b.geom))
    FILTER (WHERE b.downstream_barrier_id_30_sar IS NULL), 0)
    + coalesce(e.upstream_length, 0)) / 1000 )::numeric, 2) AS upstream_accessible30_sar_km,
  a.geom
FROM cwf.barriers_structures a
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON a.linear_feature_id = s.linear_feature_id
LEFT OUTER JOIN extra_bits e
ON a.source_id = e.source_id
LEFT OUTER JOIN cwf.segmented_streams b
ON FWA_Upstream(
    a.blue_line_key,
    a.downstream_route_measure,
    a.wscode_ltree,
    a.localcode_ltree,
    b.blue_line_key,
    b.downstream_route_measure,
    b.wscode_ltree,
    b.localcode_ltree
   )
LEFT OUTER JOIN upstream_spp spu
ON a.barrier_id = spu.barrier_id
LEFT OUTER JOIN downstream_spp spd
ON a.barrier_id = spd.barrier_id
LEFT OUTER JOIN grade g
ON a.barrier_id = g.barrier_id
LEFT OUTER JOIN whse_basemapping.dbm_mof_50k_grid t
ON ST_Intersects(a.geom, t.geom)
WHERE a.watershed_group_code IN ('LNIC', 'BULK','ELKR','HORS')
GROUP BY
  a.barrier_id,
  a.source_id,
  a.barrier_type,
  a.barrier_name,
  a.linear_feature_id,
  a.blue_line_key,
  a.downstream_route_measure,
  a.wscode_ltree,
  a.localcode_ltree,
  a.watershed_group_code,
  t.map_tile_display_name,
  a.downstream_ids,
  s.stream_order,
  g.gradient,
  e.upstream_length,
  spd.species_codes,
  spu.species_codes,
  a.geom;

-- add geom index for viewing but that is about it
CREATE INDEX ON cwf.barriers_report USING GIST (geom);