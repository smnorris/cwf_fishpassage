--jordan river
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 26156;

--sooke river
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 33212;

--NEQUILTPAALIS CREEK
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 26162;

--stave river
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 19236;

--coquitlam river
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 19251;

--como creek
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 19237;

--brunette river
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 23913;

--NICOMEKL RIVER x2
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 24877;
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 19907;

--serpentine
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 19906;

--Elgin creek
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 28356;

--Davis creek (at Lardeau)
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 24880;

-- seton river dam is not on the fraser
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 33252;

-- These FISS records are taken care of by other sources with more accurate locations
-- (these are along the Okanagan, at the outlet of each lake)
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id IN (24083, 413, 24711, 149);

-- Adams lake has an old bridge, but I don't think it is a barrier
DELETE FROM cwf.dams_src WHERE source_dataset = 'WHSE_BASEMAPPING.FWA_OBSTRUCTIONS_SP' AND source_id = 28987;

-- Ash River is a hydro structure
UPDATE cwf.dams_src SET hydro_dam_ind = 'Y'
WHERE dam_name = 'ELSIE LAKE SPILLWAY DAM';