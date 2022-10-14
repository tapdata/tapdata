import pytest
import allure

from . import env, random_str
from tapdata_cli.cli import DataSource, main


main()


@allure.feature("datasource")
class TestDataSource:

    @allure.title("create datasource with mongodb uri")
    def test_mongo_create_datasource_by_uri(self):
        ds = DataSource("mongodb", name=f"uri_{random_str()}").uri(env['database_1.URI'])
        assert ds.save()
        assert ds.desc() is not None
        assert ds.status() == "ready"
        DataSource.list()
        ds.delete()

    @allure.title("create mongodb datasource with form")
    def test_create_datasource_by_form(self):
        ds = DataSource("mongodb", name=f"form_{random_str()}")
        ds.host(env['database_1.HOST']).database(env['database_1.DB']).user(env['database_1.USERNAME']) \
            .password(env['database_1.PASSWORD']).additionalString(env['database_1.PROPS'])
        assert ds.save()
        assert ds.status() == "ready"
        ds.delete()

    @allure.title("create mongodb datasource with uri")
    def test_mongo_create_by_uri(self):
        mongo_test = DataSource("mongodb", f"test_mongo_{random_str()}")
        mongo_test.uri(env['database_1.URI'])
        assert mongo_test.save()
        assert mongo_test.validate()
        assert mongo_test.status() == "ready"
        mongo_test.delete()

    @allure.title("test mysql form valid")
    def test_mysql_to_dict(self):
        test_ds = DataSource("mysql", f"test_mysql_{random_str()}")
        host, port = env['mysql.HOST'].split(":")
        test_ds.host(host).port(int(port)).database(env['mysql.DB']).username(env['mysql.USERNAME'])\
            .password(env['mysql.PASSWORD'])
        assert test_ds.save()
        assert test_ds.status() == "ready"
        test_ds.delete()

    @allure.title("create PG datasource with form")
    def test_postgres(self):
        host, port = env['postgres.HOST'].split(":")
        test_ds = DataSource("postgresql", f"test_postgres_{random_str()}")
        test_ds.host(host).database(env['postgres.DB']).user(env['postgres.USERNAME']) \
            .password(env['postgres.PASSWORD']).schema("admin").port(int(port))
        assert test_ds.save()
        assert test_ds.validate()
        assert test_ds.status() == "ready"
        test_ds.delete()

    @allure.title("create Kafka datasource with form")
    def test_kafka(self):
        test_ds = DataSource("kafka", f"test_kafka_{random_str()}")
        test_ds.nameSrvAddr(env['kafka.HOST'])
        assert test_ds.save()
        assert test_ds.status() == "ready"
        test_ds.delete()
