import sys
import time
from typing import List, Iterable

import pytest, allure

from . import random_str, env
from tapdata_cli.cli import DataSource, logger, Pipeline, JobType, get_obj, Source, Sink, Mysql, Postgres


source_name, sink_name, mysql_name, postgres_name = f"{env['database_1.NAME_PRE']}_{random_str()}",\
                                             f"{env['database_2.NAME_PRE']}_{random_str()}", \
                                             f"{env['mysql.NAME_PRE']}_{random_str()}", \
                                             f"{env['postgres.NAME_PRE']}_{random_str()}"
source_db = DataSource("mongodb", name=source_name).uri(env['database_1.URI'])
sink_db = DataSource("mongodb", name=sink_name).uri(env['database_2.URI'])
mysql_db = Mysql(mysql_name).host(env['mysql.HOST']).db(env['mysql.DB']).username(env['mysql.USERNAME'])\
        .password(env['mysql.PASSWORD']).type("source").props(env['mysql.PROPS'])
postgres_db = Postgres(postgres_name).host(env['postgres.HOST']).db(env['postgres.DB']).username(env['postgres.USERNAME']) \
        .password(env['postgres.PASSWORD']).type("source").props(env['postgres.PROPS'])

if source_db.validate() and sink_db.validate() and mysql_db.validate() and postgres_db.validate():
    source_db.save()
    sink_db.save()
    mysql_db.save()
    postgres_db.save()
else:
    logger.error("datasource check failed, please check !")
    sys.exit(1)

source_mongo_1 = Source(source_name)
source_mongo_2 = Source(sink_name)
source_postgres = Source(postgres_name)
sink_mysql = Sink(mysql_name)


def make_new_pipeline(name):
    p = Pipeline(name=name)
    return p

def wait_scheduling(pipeline: Pipeline, count: int=5, except_status: Iterable[str]=()) -> bool:
    if not except_status:
        except_status = ['running']
    for i in range(count):
        status = pipeline.status()
        if status not in except_status:
            time.sleep(1)
            logger.log(f"the status is {pipeline.status()} now, this is {i+1} times, {count - i} times remaining")
            continue
        else:
            return True
    return False


@allure.feature("pipeline")
class TestPipeline():
    @allure.title("real time database migrate between mongodb")
    def test_migrate_job(self):
        p = make_new_pipeline(f"migrate_{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        assert wait_scheduling(p1, except_status=('running', 'wait_run'))


    @allure.title("real time table sync between mongodb")
    def test_sync_job_create(self):
        p = make_new_pipeline(f"sync_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.writeTo(sink_name)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))


    @allure.title("filter row in migrate")
    def test_filter_migrate(self):
        p = make_new_pipeline(f"filter_migrate_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.migrate
        p2 = p1.filter("id > 2 and sex=male")
        assert id(p1) == id(p2)


    @allure.title("filter row in sync")
    def test_filter_sync(self):
        p = make_new_pipeline(f"filter_sync_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.filter("id > 2 and sex=male").writeTo(sink_name)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))


    @allure.title("filter column in migrate")
    def test_filter_column_migrate(self):
        p = make_new_pipeline(f"filter_column_migrate_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.migrate
        p2 = p1.filterColumn(["id", "name"])
        assert id(p1) == id(p2)


    @allure.title("filter column in sync")
    def test_filter_column_sync(self):
        p = Pipeline(name=f"filter_column_sync_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.filterColumn(["id", "name"]).writeTo(sink_name)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))


    @allure.title("rename column in migrate")
    def test_rename_migrate(self):
        ori_name = f"rename_migrate_{random_str()}"
        new_name = f"rename_migrate_{random_str()}"
        p = make_new_pipeline(f"simple_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.migrate
        p2 = p1.rename(ori_name, new_name)
        assert id(p1) == id(p2)


    @allure.title("rename column in sync")
    def test_rename_sync(self):
        ori_name = f"rename_sync_{random_str()}"
        new_name = f"rename_sync_{random_str()}"
        p = make_new_pipeline(ori_name)
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.rename(ori_name, new_name).writeTo(sink_name)
        assert id(p1) != id(p2)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))


    @allure.title("js udf in migrate")
    def test_js_migrate(self):
        p = make_new_pipeline(f"js_migrate_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.migrate
        p2 = p1.js()
        assert id(p1) == id(p2)


    @allure.title("js udf in sync")
    def test_js_sync(self):
        p = make_new_pipeline(f"js_sync_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.js().writeTo(sink_name)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))


    @allure.title("table merge in migrate?")
    def test_merge_migrate(self):
        p = make_new_pipeline(f"merge_migrate_{random_str()}")
        p1 = p.readFrom(source_name)
        p2 = p.readFrom(sink_name)
        p1.dag.jobType = JobType.migrate
        p3 = p1.merge(p2, [('id', 'id')])
        assert p3 is None


    @allure.title("table merge in sync")
    def test_merge_sync(self):
        p = make_new_pipeline(f"merge_sync_{random_str()}")
        p1 = p.readFrom(source_mongo_1)
        p2 = p.readFrom(source_mongo_2)
        p1.dag.jobType = JobType.sync
        p3 = p1.merge(p2, [('id', 'id')]).writeTo(sink_mysql)
        p3.start()
        assert wait_scheduling(p3, except_status=('running', 'wait_run'))


    @allure.title("multi layer table merge in sync")
    def test_merge_sync_multi_nesting(self):
        p = make_new_pipeline(f"merge_sync_nesting_{random_str()}")
        p1 = p.readFrom(source_mongo_1)
        p2 = p.readFrom(source_mongo_2)
        p3 = p.readFrom(source_postgres)
        p1.dag.jobType = JobType.sync
        p2.dag.jobType = JobType.sync
        p4 = p1.merge(p2.merge(p3))
        p5 = p4.writeTo(sink_mysql)
        p5.start()
        assert wait_scheduling(p5, except_status=('running', 'wait_run', 'error'))


    @allure.title("js udf in sync")
    def test_processor_sync(self):
        p = make_new_pipeline(f"processor_sync_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.js()
        assert id(p1) != id(p2)
        p3 = p2.writeTo(sink_name)
        p3.start()
        assert wait_scheduling(p3, except_status=('running', 'wait_run'))


    def test_processor_migrate(self):
        p = make_new_pipeline(f"processor_migrate_{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.migrate
        p2 = p1.js()
        assert id(p1) == id(p2)


    @allure.title("config job")
    def test_config(self):
        p = make_new_pipeline(f"config_{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.config(config={"desc": "test config"}).start()
        pipeline_obj = get_obj('job', p1.name)
        assert pipeline_obj.job["desc"] == "test config"


    @allure.title("test stop job")
    def test_stop(self):
        p = make_new_pipeline(f"stop_{random_str()}")
        p1 = p.readFrom(source_mongo_1).writeTo(sink_mysql)
        p1.start()
        assert wait_scheduling(p1, except_status=('running', 'error'))
        p1.stop()
        assert wait_scheduling(p1, except_status=('stop', 'stopping', 'error'))


    @allure.title("test get job status")
    def test_stats(self):
        p = make_new_pipeline(f"stats_{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        assert wait_scheduling(p1, except_status=('running', ))
        p1.stats()


    @allure.title("test monitor job status")
    def test_monitor(self):
        p = make_new_pipeline(f"monitor_job{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        p1.monitor(t=2)


    def test_check(self):
        p = make_new_pipeline(f"check_job{random_str()}")
        p1 = p.readFrom(source_name)
        p1.dag.jobType = JobType.sync
        p2 = p1.writeTo(sink_name)
        p2.start()
        assert wait_scheduling(p2, except_status=('running',))
        p1.check()
