INSERT INTO {schema_a}.{temp_table}

WITH src AS
(
  SELECT *
  FROM {schema_a}.{table_a}
  WHERE watershed_group_code = %s
),

downstream AS
(
    SELECT
      {id_a},
      array_agg(downstream_id) FILTER (WHERE downstream_id IS NOT NULL) AS downstream_ids
    FROM
        (SELECT
            a.{id_a},
            b.{id_b} as downstream_id
        FROM
            src a
        INNER JOIN {schema_b}.{table_b} b ON
        fwa_downstream_linear(
            a.blue_line_key,
            a.downstream_route_measure,
            a.wscode_ltree,
            a.localcode_ltree,
            b.blue_line_key,
            b.downstream_route_measure,
            b.wscode_ltree,
            b.localcode_ltree
        )
        ORDER BY
          a.{id_a},
          b.wscode_ltree DESC,
          b.localcode_ltree DESC,
          b.downstream_route_measure DESC
        ) as d
    GROUP BY {id_a}
),

upstream AS
(
    SELECT
      {id_a},
      array_agg(upstream_id) FILTER (WHERE upstream_id IS NOT NULL) AS upstream_ids
    FROM
        (SELECT
            a.{id_a},
            b.{id_b} as upstream_id
        FROM
            src a
        INNER JOIN {schema_b}.{table_b} b ON
        fwa_upstream_linear(
            a.blue_line_key,
            a.downstream_route_measure,
            a.wscode_ltree,
            a.localcode_ltree,
            b.blue_line_key,
            b.downstream_route_measure,
            b.wscode_ltree,
            b.localcode_ltree
        )
        ORDER BY
          a.{id_a},
          b.wscode_ltree DESC,
          b.localcode_ltree DESC,
          b.downstream_route_measure DESC
        ) as d
    GROUP BY {id_a}
),

updown AS
(SELECT
  a.{id_a},
  a.downstream_ids,
  b.upstream_ids
FROM downstream a
FULL OUTER JOIN upstream b
ON a.{id_a} = b.{id_a})


SELECT a.*,
  updown.downstream_ids AS {dnstr_ids_col},
  updown.upstream_ids AS {upstr_ids_col}
FROM src a
LEFT OUTER JOIN updown ON a.{id_a} = updown.{id_a};
