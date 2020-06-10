-- create a table holding all distinct observation points for the species of interest

DROP TABLE IF EXISTS cwf.fish_obsrvtn_events_sar;
CREATE TABLE cwf.fish_obsrvtn_events_sar AS
SELECT
  e.fish_obsrvtn_pnt_distinct_id,
  e.linear_feature_id,
  e.blue_line_key,
  e.wscode_ltree,
  e.localcode_ltree,
  e.downstream_route_measure,
  e.watershed_group_code,
  e.species_ids,
  (ST_Dump(
      ST_LocateAlong(s.geom, e.downstream_route_measure)
      )
   ).geom::geometry(PointZM, 3005) AS geom
FROM whse_fish.fiss_fish_obsrvtn_events e
INNER JOIN whse_basemapping.fwa_stream_networks_sp s
ON e.linear_feature_id = s.linear_feature_id
AND species_ids && (SELECT array_agg(species_id) FROM whse_fish.species_cd WHERE code IN ('SK','CH','CO','ST','WCT','BT','ND','SSU'));

CREATE INDEX ON cwf.fish_obsrvtn_events_sar USING GIST (geom);