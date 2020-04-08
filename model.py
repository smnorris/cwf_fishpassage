import click
import pgdata
from psycopg2 import sql


@click.group()
def cli():
    pass


@cli.command()
def barriers_create():
    db = pgdata.connect(sql_path="sql/02_prioritize_wsg")
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    cur.execute(db.queries["01_create_barriers"])
    conn.commit()


@cli.command()
@click.argument("group")
def barriers_index(group):
    db = pgdata.connect(sql_path="sql/02_prioritize_wsg")
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    cur.execute("SET max_parallel_workers_per_gather = 0")
    cur.execute(db.queries["02_index_barriers"], (group,))
    conn.commit()
    cur.close()
    conn.close()


@cli.command()
def barriers_cleanup():
    db = pgdata.connect()
    db.execute("DROP TABLE cwf.barriers;")
    db.execute("ALTER TABLE cwf.barriers_temp RENAME TO barriers;")
    db.execute(
        """
        CREATE INDEX ON cwf.barriers (linear_feature_id);
        CREATE INDEX ON cwf.barriers (blue_line_key);
        CREATE INDEX ON cwf.barriers (watershed_group_code);
        CREATE INDEX ON cwf.barriers USING GIST (wscode_ltree);
        CREATE INDEX ON cwf.barriers USING BTREE (wscode_ltree);
        CREATE INDEX ON cwf.barriers USING GIST (localcode_ltree);
        CREATE INDEX ON cwf.barriers USING BTREE (localcode_ltree);
        CREATE INDEX ON cwf.barriers USING GIST (geom);"""
    )


@cli.command()
@click.argument("group")
def split_streams(group):
    """break streams at barriers per watershed group (for easy parallelization)
    """
    db = pgdata.connect(sql_path="sql/02_prioritize_wsg")
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    cur.execute("SET max_parallel_workers_per_gather = 0")
    q = sql.SQL(db.queries["03_create_segmented_streams"]).format(
        table=sql.Identifier("segmented_streams_" + group.lower())
    )
    cur.execute(q)
    q = sql.SQL(db.queries["04_load_segmented_streams"]).format(
        table=sql.Identifier("segmented_streams_" + group.lower())
    )
    cur.execute(q, (group,))
    q = sql.SQL(db.queries["05_split_streams_a"]).format(
        table=sql.Identifier("segmented_streams_" + group.lower())
    )
    cur.execute(q)
    q = sql.SQL(db.queries["06_split_streams_b"]).format(
        table=sql.Identifier("segmented_streams_" + group.lower())
    )
    cur.execute(q)
    q = sql.SQL(db.queries["07_split_streams_c"]).format(
        table=sql.Identifier("segmented_streams_" + group.lower())
    )
    cur.execute(q)
    conn.commit()
    cur.close()
    conn.close()


@cli.command()
def create_output():
    """merge temp wsg tables into output streams table and label streams
    upstream of barriers
    """
    db = pgdata.connect(sql_path="sql/02_prioritize_wsg")
    db["cwf.segmented_streams"].drop()
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    # create output table
    q = sql.SQL(db.queries["03_create_segmented_streams"]).format(
        table=sql.Identifier("segmented_streams")
    )
    cur.execute(q)
    # load data from intermediate tables
    for table in [t for t in db.tables if t[:22] == "cwf.segmented_streams_"]:
        t = table.split(".")[1]
        q = sql.SQL(db.queries["08_merge"]).format(
            out_table=sql.Identifier("segmented_streams"), in_table=sql.Identifier(t)
        )
        cur.execute(q)
    # add the usual indexes
    q = sql.SQL(db.queries["index"]).format(table=sql.Identifier("segmented_streams"))
    conn.commit()

    # label streams downstream of barriers
    q = sql.SQL(db.queries["09_label"]).format(
        table=sql.Identifier("segmented_streams"),
        downstream_id=sql.Identifier("downstream_barrier_id_15"),
    )
    cur.execute(q)
    conn.commit()
    cur.close()
    conn.close()

    # drop intermediate tables
    for table in [t for t in db.tables if t[:22] == "cwf.segmented_streams_"]:
        db[table].drop()


if __name__ == "__main__":
    cli()
