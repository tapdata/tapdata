import os

from factory import newDB
from suffix import get_suffix
from tapdata_cli.cli import DataSource


def configured_name(environment_variable):
    name = os.getenv(environment_variable)
    if not name:
        raise RuntimeError("Set {} to a datasource key from auto-test/config.yaml".format(environment_variable))
    return name


def table_name(base_name):
    return (base_name + get_suffix()).lower()


def open_database(datasource_key, table):
    database = newDB(datasource_key, table)
    if database is None:
        raise RuntimeError("No auto-test DB utility matches datasource {}".format(datasource_key))
    return database


def refresh_datasource_schema(datasource_key):
    live_name = datasource_key + get_suffix()
    datasource = DataSource.get(connector_name=live_name)
    if datasource is None:
        raise RuntimeError("Tapdata datasource {} was not created".format(live_name))
    datasource = DataSource(id=datasource["id"])
    if not datasource.validate(load_schema=True):
        raise RuntimeError("Tapdata failed to load schema for datasource {}".format(live_name))


def pipeline_tables(pipeline, source_key, source_table, sink_key, sink_table):
    source = source_key + get_suffix() + "." + source_table
    sink = sink_key + get_suffix() + "." + sink_table
    pipeline.readFrom(source).writeTo(sink)


def drop_oracle_table(database, table):
    try:
        database.execute("DROP TABLE {} PURGE".format(table))
    except Exception as error:
        # Oracle reports ORA-00942 when a test table does not exist yet.
        if "ORA-00942" not in str(error):
            raise


def cleanup(*resources):
    for database, drop_sql in resources:
        if database is None:
            continue
        try:
            try:
                database.execute(drop_sql)
            except Exception as error:
                # The tables are unique to this run; missing-table errors are
                # expected when setup failed before CREATE TABLE completed.
                message = str(error)
                if "ORA-00942" not in message and "3701" not in message:
                    raise
        finally:
            database.close()
