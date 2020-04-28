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


-- Ash River is a hydro structure
UPDATE cwf.dams_src SET hydro_dam_ind = 'Y'
WHERE dam_name = 'ELSIE LAKE SPILLWAY DAM';