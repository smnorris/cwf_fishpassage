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


-- from accessible stream QA:

-- Babine lake structure is not a barrier
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'WHSE_BASEMAPPING.FWA_OBSTRUCTIONS_SP' AND source_id = 20662;

-- Aitken Creek != Bulkley River, we don't know where this should be so just set to non-barrier for now
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'FISS Database' AND source_id = 33070;

-- VOGHT CREEK != Coldwater River - we don't know where this should be, so just set to non barrier for now
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'FISS Database' AND source_id = 21056;

-- DFO fish counting structure
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 1096;

-- no structure located; remove
DELETE FROM cwf.dams
WHERE source_dataset = 'FISS Database' AND source_id = 25685;

UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' and source_id = 840;

-- change to 'barrier_ind' = N
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'FISS Database' AND source_id = 19870;

-- Comox Dam: fishway (in canfishpass); change to 'barrier_ind'=N
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 704;

-- change to 'barrier_ind' = N
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW' AND source_id = 1478;

-- change to 'barrier_ind' = N
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'FISS Database' AND source_id = 33251;

-- Duplicate; remove
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 24908;

-- Duplicate; remove
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 23094;

-- Duplicate; remove
DELETE FROM cwf.dams_src WHERE source_dataset = 'FISS Database' AND source_id = 404;

-- change to 'barrier_ind'=N
UPDATE cwf.dams_src SET barrier_ind = 'N'
WHERE source_dataset = 'FISS Database' AND source_id = 19249;

-- these were manually moved/created:
-- source_dataset = 'FISS Database' AND source_id = 19249-- relocate upstream ~700 m;
-- DEAD,AL,N/A,N/A,"New structure not in the dam layer, located ~50m upstream of CBS barrier point 3900006"
-- (added as source_dataset = "Imagery", source_id = 1, barrier_ind='Y')
