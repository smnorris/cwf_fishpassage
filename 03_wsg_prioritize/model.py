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
@click.option("--include_equivalent_measure", default=False, is_flag=True)
def add_downstream_ids(table_a, id_a, table_b, id_b, downstream_ids_col, include_equivalent_measure):
    """note downstream ids
    """
    db = pgdata.connect()
    schema_a, table_a = db.parse_table_name(table_a)
    schema_b, table_b = db.parse_table_name(table_b)
    # ensure that any existing values get removed
    db.execute(f"ALTER TABLE {schema_a}.{table_a} DROP COLUMN IF EXISTS {downstream_ids_col}")
    temp_table = table_a + "_tmp"
    db[f"{schema_a}.{temp_table}"].drop()
    db.execute(f"CREATE TABLE {schema_a}.{temp_table} (LIKE {schema_a}.{table_a})")
    db.execute(f"ALTER TABLE {schema_a}.{temp_table} ADD COLUMN {downstream_ids_col} integer[]")
    groups = sorted([g[0] for g in db.query(f"SELECT DISTINCT watershed_group_CODE from {schema_a}.{table_a}")])
    # todo - is this really the best way to specify which query to use?
    if include_equivalent_measure:
        q = "02_add_downstream_and_equivalent_ids"
    else:
        q = "02_add_downstream_ids"
    query = sql.SQL(db.queries[q]).format(
        schema_a=sql.Identifier(schema_a),
        schema_b=sql.Identifier(schema_b),
        temp_table=sql.Identifier(temp_table),
        table_a=sql.Identifier(table_a),
        table_b=sql.Identifier(table_b),
        id_a=sql.Identifier(id_a),
        id_b=sql.Identifier(id_b),
        dnstr_ids_col=sql.Identifier(downstream_ids_col)
    )
    # run each group in parallel
    func = partial(execute_parallel, query)
    n_processes = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=n_processes)
    pool.map(func, groups)
    pool.close()
    pool.join()
    # drop source table, rename new table, re-create indexes
    db[f"{schema_a}.{table_a}"].drop()
    db.execute(f"ALTER TABLE {schema_a}.{temp_table} RENAME TO {table_a}")
    create_indexes(f"{schema_a}.{table_a}")
    db.execute(f"ALTER TABLE {schema_a}.{table_a} ADD PRIMARY KEY ({id_a})")


@cli.command()
@click.argument("table_a")
@click.argument("id_a")
@click.argument("table_b")
@click.argument("id_b")
@click.argument("upstream_ids_col")
def add_upstream_ids(table_a, id_a, table_b, id_b, upstream_ids_col):
    """note downstream ids
    """
    db = pgdata.connect()
    schema_a, table_a = db.parse_table_name(table_a)
    schema_b, table_b = db.parse_table_name(table_b)
    # ensure that any existing values get removed
    db.execute(f"ALTER TABLE {schema_a}.{table_a} DROP COLUMN IF EXISTS {upstream_ids_col}")
    temp_table = table_a + "_tmp"
    db[f"{schema_a}.{temp_table}"].drop()
    db.execute(f"CREATE TABLE {schema_a}.{temp_table} (LIKE {schema_a}.{table_a})")
    db.execute(f"ALTER TABLE {schema_a}.{temp_table} ADD COLUMN {upstream_ids_col} integer[]")
    groups = sorted([g[0] for g in db.query(f"SELECT DISTINCT watershed_group_code from {schema_a}.{table_a}")])
    query = sql.SQL(db.queries["02_add_upstream_ids"]).format(
        schema_a=sql.Identifier(schema_a),
        schema_b=sql.Identifier(schema_b),
        temp_table=sql.Identifier(temp_table),
        table_a=sql.Identifier(table_a),
        table_b=sql.Identifier(table_b),
        id_a=sql.Identifier(id_a),
        id_b=sql.Identifier(id_b),
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
    db[f"{schema_a}.{table_a}"].drop()
    db.execute(f"ALTER TABLE {schema_a}.{temp_table} RENAME TO {table_a}")
    create_indexes(f"{schema_a}.{table_a}")
    db.execute(f"ALTER TABLE {schema_a}.{table_a} ADD PRIMARY KEY ({id_a})")


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
    groups = [g[0] for g in db.query(f"SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'")]
    #AND watershed_group_code IN ('VICT','SANJ','COWN','LFRA','SQAM')
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
@click.argument("stream_table")
@click.argument("point_table")
def segment_streams(stream_table, point_table):
    """break streams at points
    """
    db = pgdata.connect()
    stream_schema, stream_table = db.parse_table_name(stream_table)
    point_schema, point_table = db.parse_table_name(point_table)
    groups = [g[0] for g in db.query(f"SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'")]
    # AND watershed_group_code IN ('VICT','SANJ','COWN','LFRA','SQAM')
    query = sql.SQL(db.queries["05_segment_streams"]).format(
        stream_schema=sql.Identifier(stream_schema),
        stream_table=sql.Identifier(stream_table),
        point_schema=sql.Identifier(point_schema),
        point_table=sql.Identifier(point_table)
    )
    func = partial(execute_parallel, query)
    n_processes = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=n_processes)
    pool.map(func, groups)
    pool.close()
    pool.join()


@cli.command()
@click.argument("stream_table")
@click.argument("barrier_table")
@click.argument("source_column")
@click.argument("target_column")
def clear_barriers_below_observations(stream_table, barrier_table, source_column, target_column):
    """create a new column in streams table, populating with barrier_id values from source_column, minus
    any barriers that are downstream of observations (upstream_observation_ids must be present)
    """
    db = pgdata.connect()
    conn = db.engine.raw_connection()
    cur = conn.cursor()
    stream_schema, stream_table = db.parse_table_name(stream_table)
    barrier_schema, barrier_table = db.parse_table_name(barrier_table)
    # add output column
    query = sql.SQL("ALTER TABLE {stream_schema}.{stream_table} DROP COLUMN IF EXISTS {target_column}").format(
        stream_schema=sql.Identifier(stream_schema),
        stream_table=sql.Identifier(stream_table),
        target_column=sql.Identifier(target_column)
    )
    cur.execute(query)
    conn.commit()
    query = sql.SQL("ALTER TABLE {stream_schema}.{stream_table} ADD COLUMN IF NOT EXISTS {target_column} integer[];").format(
        stream_schema=sql.Identifier(stream_schema),
        stream_table=sql.Identifier(stream_table),
        target_column=sql.Identifier(target_column)
    )
    cur.execute(query)
    conn.commit()

    # get the groups and process in parallel
    groups = [g[0] for g in db.query(f"SELECT watershed_group_CODE from cwf.target_watershed_groups WHERE status = 'In'")]
    query = sql.SQL(db.queries["clear_barriers_below_observations"]).format(
        stream_schema=sql.Identifier(stream_schema),
        stream_table=sql.Identifier(stream_table),
        barrier_schema=sql.Identifier(barrier_schema),
        barrier_table=sql.Identifier(barrier_table),
        target_column=sql.Identifier(target_column),
        source_column=sql.Identifier(source_column)
    )
    func = partial(execute_parallel, query)
    n_processes = multiprocessing.cpu_count() - 1
    pool = multiprocessing.Pool(processes=n_processes)
    pool.map(func, groups)
    pool.close()
    pool.join()


if __name__ == "__main__":
    cli()
