WITH barriers_to_remove AS
(SELECT array_agg(barrier_id) as removals
FROM {barrier_schema}.{barrier_table}
WHERE upstream_observation_ids IS NOT NULL)

UPDATE {stream_schema}.{stream_table} s
SET {target_column} = ({source_column} - r.removals)
FROM barriers_to_remove r
WHERE s.{source_column} IS NOT NULL
AND s.watershed_group_code = %s;