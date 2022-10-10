import sys
import time
from typing import List, Iterable

import pytest
import allure

from . import random_str, env
from tapdata_cli.cli import DataSource, logger, Pipeline, JobType, get_obj, Source, Sink, main

main()

try:
    # format kafka name
    source_name, sink_name, mysql_name, postgres_name, kafka_name = (
        f"{env['database_1.NAME_PRE']}",
        f"{env['database_2.NAME_PRE']}",
        f"{env['mysql.NAME_PRE']}",
        f"{env['postgres.NAME_PRE']}",
        f"{env['kafka.NAME_PRE']}"
    )
    # init datasource
    source_db = DataSource("mongodb", name=source_name).uri(env['database_1.URI'])
    sink_db = DataSource("mongodb", name=sink_name).uri(env['database_2.URI'])
    host, port = env['mysql.HOST'].split(":")
    mysql_db = DataSource("mysql", mysql_name).host(host).port(int(port)).database(env['mysql.DB']). \
        username(env['mysql.USERNAME']).password(env['mysql.PASSWORD'])
    postgres_db = DataSource("postgresql", postgres_name).host(host).database(env['postgres.DB']). \
        user(env['postgres.USERNAME']).password(env['postgres.PASSWORD']).schema("admin").port(int(port))
    kafka_db = DataSource("kafka").nameSrvAddr(env['kafka.HOST'])

    # save datasource
    source_db.save()
    sink_db.save()
    mysql_db.save()
    postgres_db.save()
    kafka_db.save()

except AttributeError:
    pass


def make_new_pipeline(name):
    p = Pipeline(name=name)
    return p


def wait_scheduling(pipeline: Pipeline, count: int = 5, except_status: Iterable[str] = ()) -> bool:
    if not except_status:
        except_status = ['running']
    for i in range(count):
        status = pipeline.status()
        if status not in except_status and status != "error":
            time.sleep(1)
            logger.log(f"the status is {pipeline.status()} now, this is {i + 1} times, {count - i} times remaining")
            continue
        elif status == "error":
            return False
        else:
            return True
    return False


def stop(p: Pipeline):
    # TODO: wait and check job status then stop job, finally delete job
    return True


@allure.feature("pipeline")
class TestPipeline:
    @allure.title("real time database migrate between mongodb")
    def test_migrate_job(self):
        p = make_new_pipeline(f"migrate_{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        assert wait_scheduling(p1, except_status=('running', 'wait_run'))
        assert stop(p1)

    @allure.title("real time table sync between mongodb")
    def test_sync_job_create(self):
        p = make_new_pipeline(f"sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.writeTo(f"{sink_name}.test")
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))
        assert stop(p2)

    @allure.title("filter row in sync")
    def test_filter_sync(self):
        p = make_new_pipeline(f"filter_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.filter("id > 2 && sex == male").writeTo(f"{sink_name}.test")
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))
        assert stop(p2)

    @allure.title("filter column in sync")
    def test_filter_column_sync(self):
        p = Pipeline(name=f"filter_column_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.filterColumn(["id", "name"]).writeTo(f"{sink_name}.test")
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))
        assert stop(p2)

    @allure.title("rename column in sync")
    def test_rename_sync(self):
        ori_name = "test"
        new_name = "test_rename"
        p = make_new_pipeline(f"rename_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.rename(ori_name, new_name).writeTo(f"{sink_name}.test")
        assert id(p1) != id(p2)
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))
        assert stop(p2)

    @allure.title("js udf in sync")
    def test_js_sync(self):
        p = make_new_pipeline(f"js_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.js().writeTo(f"{sink_name}.test")
        p2.start()
        assert wait_scheduling(p2, except_status=('running', 'wait_run'))
        assert stop(p2)

    @allure.title("table merge in sync")
    def test_merge_sync(self):
        p = make_new_pipeline(f"merge_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p.readFrom(f"{sink_name}.test")
        p3 = p1.merge(p2, [('id', 'id')]).writeTo(f"{mysql_name}.test")
        p3.start()
        assert wait_scheduling(p3, except_status=('running', 'wait_run'))
        assert stop(p3)

    @allure.title("js udf in sync")
    def test_processor_sync(self):
        p = make_new_pipeline(f"processor_sync_{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(f"{source_name}.test")
        p2 = p1.js()
        p3 = p2.writeTo(f"{sink_name}.test")
        p3.start()
        assert wait_scheduling(p3, except_status=('running', 'wait_run'))
        assert stop(p3)

    @allure.title("config job")
    def test_config(self):
        p = make_new_pipeline(f"config_{random_str()}")
        p1 = p.readFrom(f"{source_name}").writeTo(f"{sink_name}")
        p1.config(config={"desc": "test config"}).start()
        pipeline_obj = get_obj('job', p1.name)
        assert pipeline_obj.job["desc"] == "test config"

    @allure.title("test stop job")
    def test_stop(self):
        p = make_new_pipeline(f"stop_{random_str()}")
        p1 = p.readFrom(f"{source_name}").writeTo(f"{sink_name}")
        p1.start()
        assert wait_scheduling(p1, except_status=('running',), count=60)
        p1.stop()
        assert wait_scheduling(p1, except_status=('stop',), count=60)

    @allure.title("test get job status")
    def test_stats(self):
        p = make_new_pipeline(f"stats_{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        assert wait_scheduling(p1, except_status=('running',))
        p1.stats()
        assert stop(p1)

    @allure.title("test monitor job status")
    def test_monitor(self):
        p = make_new_pipeline(f"monitor_job{random_str()}")
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.start()
        assert wait_scheduling(p1, except_status=('running',))
        p1.monitor(t=2)

    def test_check(self):
        p = make_new_pipeline(f"check_job{random_str()}")
        p.dag.jobType = JobType.sync
        p1 = p.readFrom(source_name).writeTo(sink_name)
        p1.config({'isAutoInspect': True})
        p1.start()
        assert wait_scheduling(p1, except_status=('running',), count=10)
        p1.check()
        assert stop(p1)
