import pytest

from . import env, random_str
from lib.cli import DataSource, logger, MongoDB, Mysql, Postgres


def make_data_source_by_uri(name: str = None):
    name = name or f"test_ds_uri_{random_str()}"
    test_ds = DataSource("mongodb", name=name).uri(env['database_1.URI'])
    test_ds.validate()
    test_ds.save()
    return test_ds


def test_mongo_create_datasource_by_uri():
    ds = DataSource("mongodb", name=f"uri_{random_str()}").uri(env['database_1.URI'])
    assert ds.save()
    assert ds.desc() is not None
    assert ds.status() is not None
    DataSource().list()
    ds.delete()


def test_create_datasource_by_form():
    ds = DataSource("mongodb", name=f"form_{random_str()}")
    ds.host(env['database_1.HOST']).db(env['database_1.DB']).username(env['database_1.USERNAME'])\
        .password(env['database_1.PASSWORD']).type("source").props(env['database_1.PROPS'])
    assert ds.save()
    ds.delete()


def test_mongo_create_by_uri():
    mongo_test = MongoDB(f"test_mongo_{random_str()}")
    mongo_test.uri(env['database_1.URI'])
    assert mongo_test.save()
    assert mongo_test.validate()
    mongo_test.delete()


def test_mysql_to_dict():
    test_ds = Mysql(f"test_mongo_{random_str()}")
    test_ds.host(env['mysql.HOST']).db(env['mysql.DB']).username(env['mysql.USERNAME'])\
        .password(env['mysql.PASSWORD']).type("source").props(env['mysql.PROPS'])
    test_ds.to_dict()
    test_ds.delete()


def test_postgres():
    schema = 'admin'
    test_ds = Postgres(f"test_postgres_{random_str()}")
    test_ds.host(env['postgres.HOST']).db(env['postgres.DB']).username(env['postgres.USERNAME']) \
        .password(env['postgres.PASSWORD']).type("source").props(env['postgres.PROPS'])
    assert test_ds.save()
    test_ds.schema(schema)
    assert test_ds.to_dict().get('database_owner') == schema
    plugin = "wal2json_streaming"
    test_ds.log_decorder_plugin(plugin)
    assert test_ds.to_dict().get('pgsql_log_decorder_plugin_name') == plugin
    test_ds.delete()

