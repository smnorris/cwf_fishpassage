CREATE INDEX ON cwf.{table} USING btree (blue_line_key);
CREATE INDEX ON cwf.{table} USING btree (edge_type);
CREATE INDEX ON cwf.{table} USING btree (gnis_name);
CREATE INDEX ON cwf.{table} USING btree (waterbody_key);
CREATE INDEX ON cwf.{table} USING btree (watershed_group_code);
CREATE INDEX ON cwf.{table} USING btree (watershed_key);
CREATE INDEX ON cwf.{table} USING btree (wscode_ltree);
CREATE INDEX ON cwf.{table} USING btree (localcode_ltree);

CREATE INDEX ON cwf.{table} USING gist (localcode_ltree);
CREATE INDEX ON cwf.{table} USING gist (wscode_ltree);
CREATE INDEX ON cwf.{table} USING gist (geom);