-- create initial stream table based on provincial table
DROP TABLE IF EXISTS cwf.segmented_streams;
CREATE TABLE cwf.segmented_streams (LIKE whse_basemapping.fwa_stream_networks_sp INCLUDING ALL);

-- remove existing primary key and add new one because we'll be splitting the streams
ALTER TABLE cwf.segmented_streams DROP CONSTRAINT segmented_streams_pkey;
ALTER TABLE cwf.segmented_streams ALTER COLUMN linear_feature_id DROP DEFAULT;
ALTER TABLE cwf.segmented_streams ADD COLUMN segmented_stream_id SERIAL PRIMARY KEY;