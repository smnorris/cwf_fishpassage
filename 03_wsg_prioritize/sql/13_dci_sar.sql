-- there is probably a better way to write this.
WITH segments_15 AS
(SELECT
  watershed_group_code,
  downstream_barrier_id_15,
  downstream_barrier_id_20,
  downstream_barrier_id_30,
  downstream_barrier_id_structure,
  st_length(ST_Union(geom)) as length_segment
FROM cwf.segmented_streams
WHERE downstream_barrier_id_15_sar IS NULL
GROUP BY
  watershed_group_code,
  downstream_barrier_id_15,
  downstream_barrier_id_20,
  downstream_barrier_id_30,
  downstream_barrier_id_structure),

totals_15 AS
(SELECT
 watershed_group_code,
 sum(length_segment) as length_total
FROM segments_15
GROUP BY watershed_group_code),

segments_20 AS
(SELECT
  watershed_group_code,
  downstream_barrier_id_20,
  downstream_barrier_id_30,
  downstream_barrier_id_structure,
  st_length(ST_Union(geom)) as length_segment
FROM cwf.segmented_streams
WHERE downstream_barrier_id_20_sar IS NULL
GROUP BY
  watershed_group_code,
  downstream_barrier_id_20,
  downstream_barrier_id_30,
  downstream_barrier_id_structure),

totals_20 AS
(SELECT
 watershed_group_code,
 sum(length_segment) as length_total
FROM segments_20
GROUP BY watershed_group_code),

segments_30 AS
(SELECT
  watershed_group_code,
  downstream_barrier_id_30,
  downstream_barrier_id_structure,
  st_length(ST_Union(geom)) as length_segment
FROM cwf.segmented_streams
WHERE downstream_barrier_id_30_sar IS NULL
GROUP BY
  watershed_group_code,
  downstream_barrier_id_30,
  downstream_barrier_id_structure),

totals_30 AS
(SELECT
 watershed_group_code,
 sum(length_segment) as length_total
FROM segments_30
GROUP BY watershed_group_code),

dci_15 AS
(SELECT
  t.watershed_group_code,
  SUM((s.length_segment * s.length_segment) / (t.length_total * t.length_total)) as dci_15
FROM segments_15 s
INNER JOIN totals_15 t
ON s.watershed_Group_code = t.watershed_group_code
GROUP BY t.watershed_group_code),

dci_20 AS
(SELECT
  t.watershed_group_code,
  SUM((s.length_segment * s.length_segment) / (t.length_total * t.length_total)) as dci_20
FROM segments_20 s
INNER JOIN totals_20 t
ON s.watershed_Group_code = t.watershed_group_code
GROUP BY t.watershed_group_code),

dci_30 AS
(SELECT
  t.watershed_group_code,
  SUM((s.length_segment * s.length_segment) / (t.length_total * t.length_total)) as dci_30
FROM segments_30 s
INNER JOIN totals_30 t
ON s.watershed_Group_code = t.watershed_group_code
GROUP BY t.watershed_group_code)

SELECT
 dci_15.watershed_Group_code,
 dci_15.dci_15,
 dci_20.dci_20,
 dci_30.dci_30
FROM dci_15
INNER JOIN dci_20 ON dci_15.watershed_group_code = dci_20.watershed_group_code
INNER JOIN dci_30 ON dci_15.watershed_group_code = dci_30.watershed_group_code;