-- ========================================================
-- Dam fixes for study area watersheds: BULK, ELKR, HORS, LNIC
-- All fixes sumbitted by OB, 2020-07-07
-- ========================================================

----------------------------
-- REMOVE DUPLICATES
----------------------------
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 33791;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 38820;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 34284;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 34477;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 21857;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 167;
DELETE FROM cwf.dams_src WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 1292;
DELETE FROM cwf.dams_src WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 298;

----------------------------
-- CORRECT LOCATIONS
----------------------------
UPDATE cwf.dams_src
SET geom = ST_Transform(ST_GeomFromText('POINT(-126.991684 54.726886)', 4326), 3005)
WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 129;

UPDATE cwf.dams_src
SET geom = ST_Transform(ST_GeomFromText('POINT(-120.908091 49.910454)', 4326), 3005)
WHERE source_dataset = 'FISS Database' AND source_id = 21056;

----------------------------
-- No visible structure
----------------------------
UPDATE cwf.dams_src SET barrier_ind = 'N' WHERE source_dataset = 'FISS Database' AND source_id = 24706;
UPDATE cwf.dams_src SET barrier_ind = 'N' WHERE source_dataset = 'FISS Database' AND source_id = 283;
UPDATE cwf.dams_src SET barrier_ind = 'N' WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 1041;
UPDATE cwf.dams_src SET barrier_ind = 'N' WHERE source_dataset = 'WHSE_BASEMAPPING.FWA_OBSTRUCTIONS_SP' AND source_id = 21010;

----------------------------
-- Fishway
----------------------------
UPDATE cwf.dams_src SET barrier_ind = 'N' WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 1478;

