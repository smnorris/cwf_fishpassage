import click
import multiprocessing
from functools import partial
import pgdata
from psycopg2 import sql


def execute_parallel(sql, wsg):
    """Execute sql for specified wsg using a non-pooled, non-parallel conn
    """
    # specify multiprocessing when creating to disable connection pooling
    db = pgdata.connect(multiprocessing=True)
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    # Turn off parallel execution for this connection, because we are
    # handling the parallelization ourselves
    cur.execute("SET max_parallel_workers_per_gather = 0")
    cur.execute(sql, (wsg,))
    conn.commit()
    cur.close()
    conn.close()


def create_indexes(table):
    """create usual fwa indexes
    """
    db = pgdata.connect()
    schema, table = db.parse_table_name(table)
    db.execute(f"""CREATE INDEX ON {schema}.{table} (linear_feature_id);
    CREATE INDEX ON {schema}.{table} (blue_line_key);
    CREATE INDEX ON {schema}.{table} (watershed_group_code);

    CREATE INDEX ON {schema}.{table} USING GIST (wscode_ltree);
    CREATE INDEX ON {schema}.{table} USING BTREE (wscode_ltree);
    CREATE INDEX ON {schema}.{table} USING GIST (localcode_ltree);
    CREATE INDEX ON {schema}.{table} USING BTREE (localcode_ltree);

    CREATE INDEX ON {schema}.{table} USING GIST (geom);
    """)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("table_a")
@click.argument("id_a")
@click.argument("table_b")
@click.argument("id_b")
@click.argument("downstream_ids_col")
@click.argument("upstream_ids_col")
def add_upstream_downstream(table_a, id_a, table_b, id_b, downstream_ids_col, upstream_ids_col):
    """note upstream and downstream ids
    """
    db = pgdata.connect()
    schema, table = db.parse_table_name(table_a)
    temp_table = table + "_tmp"
    db[f"{schema}.{temp_table}"].drop()
    db.execute(f"CREATE TABLE {schema}.{temp_table} (LIKE {schema}.{table})")
    db.execute(f"ALTER TABLE {schema}.{temp_table} ADD COLUMN IF NOT EXISTS downstream_ids integer[]")
    db.execute(f"ALTER TABLE {schema}.{temp_table} ADD COLUMN IF NOT EXISTS upstream_ids integer[]")
    groups = sorted([g[0] for g in db.query(f"SELECT DISTINCT watershed_group_CODE from {schema}.{table}")])
    query = sql.SQL(db.queries["02_index_barriers"]).format(
        schema_a=sql.Identifier(schema),
        schema_b=sql.Identifier(schema),
        temp_table=sql.Identifier(temp_table),
        table_a=sql.Identifier(table),
        table_b=sql.Identifier(table),
        id_a=sql.Identifier(id_a),
        id_b=sql.Identifier(id_b),
        dnstr_ids_col=sql.Identifier(downstream_ids_col),
        upstr_ids_col=sql.Identifier(upstream_ids_col)
    )
    # run each group in parallel
    func = partial(execute_parallel, query)
    n_processes = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=n_processes)
    pool.map(func, groups)
    pool.close()
    pool.join()
    # drop source table, rename new table, re-create indexes
    db[f"{schema}.{table}"].drop()
    db.execute(f"ALTER TABLE {schema}.{temp_table} RENAME TO {table}")
    create_indexes(f"{schema}.{table}")
    db.execute(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({id_a})")


@cli.command()
@click.argument("table")
def initialize_output(table):
    db = pgdata.connect()
    schema, table = db.parse_table_name(table)
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    query = sql.SQL(db.queries["03_create_segmented_streams"]).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table)
    )
    cur.execute(query)
    conn.commit()
    groups = [g[0] for g in db.query(f"SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In' AND watershed_group_code IN ('VICT','SANJ','COWN','LFRA','SQAM')")]
    query = sql.SQL(db.queries["04_load_segmented_streams"]).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table)
    )
    # running in parallel only cuts time in half
    func = partial(execute_parallel, query)
    n_processes = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=n_processes)
    pool.map(func, groups)
    pool.close()
    pool.join()
    # create the usual indexes
    create_indexes(f"{schema}.{table}")
    # create a few more
    db.execute(f"CREATE INDEX ON {schema}.{table} (waterbody_key);")
    db.execute(f"CREATE INDEX ON {schema}.{table} (edge_type);")
    db.execute(f"CREATE INDEX ON {schema}.{table} (gnis_name);")


@cli.command()
@click.argument("group")
def split_streams(group):
    """break streams at barriers per watershed group (for easy parallelization)
    """
    db = pgdata.connect()
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    cur.execute("SET max_parallel_workers_per_gather = 0")
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
    db = pgdata.connect()
    db["cwf.segmented_streams"].drop()
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    # create output table
    q = sql.SQL(db.queries["03_create_segmented_streams"]).format(
        table=sql.Identifier("segmented_streams")
    )
    cur.execute(q)

    click.echo("loading segmented_streams from temp tables")
    for table in [t for t in db.tables if t[:22] == "cwf.segmented_streams_"]:
        t = table.split(".")[1]
        q = sql.SQL(db.queries["08_merge"]).format(
            out_table=sql.Identifier("segmented_streams"), in_table=sql.Identifier(t)
        )
        cur.execute(q)

    click.echo("indexing segmented_streams")
    q = sql.SQL(db.queries["index_streams"]).format(
        table=sql.Identifier("segmented_streams")
    )
    cur.execute(q)
    conn.commit()
    cur.close()
    conn.close()


@cli.command()
@click.argument("column")
def label(column):
    db = pgdata.connect()
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    click.echo("labelling streams downstream of barriers")
    q = f"ALTER TABLE cwf.segmented_streams ADD COLUMN {column} integer"
    cur.execute(q)
    q = sql.SQL(db.queries["09_label"]).format(
        table=sql.Identifier("segmented_streams"),
        downstream_id=sql.Identifier(column),
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
