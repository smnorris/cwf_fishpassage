-- create a temp table where we segment streams at barriers

CREATE TABLE scratch.temp_streams AS

-- first, find minimum barriers so we only apply breaks at the bottom
WITH

wsg_pts AS

(
    SELECT *
    FROM cwf.gradient_barriers
    WHERE watershed_group_code = 'VICT'
)

min_pts AS
(
    SELECT
      a.id,
      a.linear_feature_id,
      a.blue_line_key,
      a.downstream_route_measure,
      a.wscode_ltree,
      a.localcode_ltree
    FROM wsg_pts a
    LEFT OUTER JOIN wsg_pts b ON

    -- b is downstream of a IF :
    -- criteria 1 - b is same blue line, with lower measure
    (
      a.blue_line_key = b.blue_line_key AND
      a.downstream_route_measure > b.downstream_route_measure
    )
    OR
    -- criteria 2 - a watershed code is a descendant of b watershed code
    (
      a.wscode_ltree <@ b.wscode_ltree
    AND
      (
           -- AND localcode of a is bigger than localcode of b at given level
           subltree(a.localcode_ltree, 0, nlevel(b.localcode_ltree)) > b.localcode_ltree

           -- OR, where b's wscode and localcode are equivalent
           -- (ie, at bottom segment of a given watershed code)
           -- but excluding records in a and b on same stream
           OR (
              b.wscode_ltree = b.localcode_ltree
              AND a.wscode_ltree != b.wscode_ltree
           )

           -- OR any missed side channels on the same watershed code
           OR (
                a.wscode_ltree = b.wscode_ltree AND
                a.blue_line_key != b.blue_line_key AND
                a.localcode_ltree > b.localcode_ltree
           )
      )
    )
    WHERE b.id IS NULL
    ORDER BY a.id, b.wscode_ltree desc, b.localcode_ltree desc, b.downstream_route_measure desc

)

to_break AS
(
    SELECT
      s.linear_feature_id,
      s.downstream_route_measure as meas_stream_ds,
      s.upstream_route_measure as meas_stream_us,
      g.threshold,
      g.downstream_route_measure as meas_event
    FROM scratch.streams s
    INNER JOIN min_pts g
    ON s.linear_feature_id = g.linear_feature_id
    WHERE (g.downstream_route_measure - s.downstream_route_measure) > 1
    AND (s.upstream_route_measure - g.downstream_route_measure) > 1
),

-- derive measures of new lines from break points
new_measures AS
(SELECT
  linear_feature_id,
  --meas_stream_ds,
  --meas_stream_us,
  meas_event as downstream_route_measure,
  lead(meas_event, 1, meas_stream_us) over(partition by linear_feature_id order by  meas_event) as upstream_route_measure,
  threshold
FROM to_break
)

-- create new geoms
SELECT
  row_number() over() as id,
  s.linear_feature_id,
  n.downstream_route_measure,
  n.upstream_route_measure,
  ST_LocateBetween(s.geom, n.downstream_route_measure, n.upstream_route_measure) as geom
FROM new_measures n
INNER JOIN scratch.streams s
ON n.linear_feature_id = s.linear_feature_id