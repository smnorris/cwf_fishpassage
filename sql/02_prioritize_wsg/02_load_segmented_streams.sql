-- insert stream data straight from the master table

INSERT INTO cwf.segmented_streams
SELECT *
FROM whse_basemapping.fwa_stream_networks_sp
WHERE watershed_group_code = 'VICT'       -- for watershed group of interest only
AND fwa_watershed_code NOT LIKE '999%'    -- connected to network
AND edge_type != 6010                     -- in BC
AND localcode_ltree IS NOT NULL           -- not a side channel of unknown location
ORDER BY linear_feature_id;

