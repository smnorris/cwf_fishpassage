-- Remove geometry duplication from the fish ranges table to make something more useful

DROP TABLE IF EXISTS whse_fish.fiss_fish_ranges;

CREATE TABLE whse_fish.fiss_fish_ranges AS
SELECT DISTINCT
  watershed_poly_id,
  wsd_id,
  watershed_code,
  watershed_group_code,
  species_code,
  species_name,
  range_code
FROM whse_fish.fiss_fish_ranges_svw;

CREATE INDEX ON whse_fish.fiss_fish_ranges (watershed_group_code);