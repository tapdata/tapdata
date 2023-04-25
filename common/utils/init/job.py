from typing import Union
import os, sys, copy, argparse, inspect, time, random
from enum import Enum
from types import FunctionType
from importlib import import_module


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../lib")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../common")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../data/init_data")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../test_cases")

init_data_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../data/init_data'
test_cases_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../test_cases'
cache_file_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../common/helper/.table_suffix_cache_file'

from helper.suffix import get_test_table, get_suffix
from tapdata_cli import cli
from create_temp_file import ManageTempFile
from create_datasource import *
from config_parser import config
from tapdata_cli.cli import (
    Pipeline,
    Sink,
    Source,
    init
)

init()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', help="test case file, choose it from test_.* from this path",
                        default="test_sync")
    parser.add_argument('--source', help="datasource name, if None, will run dummy", default="dummy")
    parser.add_argument('--target', help="datasource sink name, if None, will run dummy", default="dummy")
    parser.add_argument('--processor', help="processing name, if None, No processing nodes will be used", default=None)
    parser.add_argument('--clean', help="clean cache file", default=False, action='store_true')
    args = parser.parse_args()
    return args


def clean_cache_file():
    if parse_args().clean:
        if os.path.isfile(cache_file_path):
            os.remove(cache_file_path)
            return
        else:
            logger.info("There is no cached file, a cached file will be created 【.table_suffix_cache_file】", "")
            return


class Processor(Enum):
    """
    the following is an enumeration of all supported processor types:
    1 -> sync
    2 -> merge
    3 -> join
    4 -> union
    5 -> js
    6 -> rowFilter
    7 -> fieldCalc
    8 -> fieldModType
    9 -> fieldRename
    10 -> fieldAddDel
    """
    sync = "sync"
    merge = "merge"
    join = "join"
    union = "union"
    js = "js"
    rowFilter = "rowFilter"
    fieldCalc = "fieldCalc"
    fieldModType = "fieldModType"
    fieldRename = "fieldRename"
    fieldAddDel = "fieldAddDel"


class TestCase:

    _reality_test_case = []
    _name_l = []

    def __new__(cls, test_case: Union[list, str], *args: str):
        if isinstance(test_case, list):
            for pt in test_case:
                if pt in cls._test_cases_module():
                    cls._reality_test_case.append(pt)
        elif isinstance(test_case, str):
            if test_case in cls._test_cases_module():
                cls._reality_test_case.append(test_case)
        cls._reality_test_case = set(cls._reality_test_case)
        cls._reality_test_case = list(cls._reality_test_case)
        return super().__new__(cls, *args)

    def __get__(self, instance, owner) -> dict:
        if self._reality_test_case:
            for tc in self._reality_test_case:
                inst = owner()
                inst.test_case = import_module(tc).test
                form_parameter = inst.test_case.__code__.co_varnames[0:inst.test_case.__code__.co_argcount]
                _params_template = dict([(import_module(tc).__name__, form_parameter)])
                inst.name = import_module(tc).__name__ + get_suffix()
                cli.job_name = inst.name
                inst.job_infos["job_name"] = inst.name
                inst._params_template = _params_template
                if tc[5:] in Processor.__dict__["_member_map_"]:
                    inst.job_infos["dag"]["node"]["processor"] = tc[5:]
                else:
                    logger.warn("{} in the name of the test case does not "
                                "conform to the specification, please modify,{}", tc[5:], Processor.__doc__)
                for res in inst:
                    pass
                return res

    def __set__(self, instance, value) -> None:
        mods_params = value.values()
        instance.gen_params = {}
        instance.source_params = []  # Reserved multi-table list
        instance.target_params = []
        instance.dummy_source_table = "%s_%s%s" % (instance.dummy_source_name,
                                                   list(value)[0], get_suffix())
        for param in list(mods_params)[0]:
            if param == "Pipeline":
                instance.gen_params.update(Pipeline=Pipeline)
                continue
            if param.startswith("source"):
                instance.source_table_name = "%s_%s_%s_%s" % \
                                             (instance.datasource_name_l[0],
                                              list(value)[0], param,
                                              f"{''.join(random.sample('zyxwvutsrqponmlkjihgfedcba',2))}")
                source_name = instance.dummy_source if \
                    instance._args.source == "dummy" else instance.datasource_name_l[0]
                exec_param = "%s.%s" % (source_name, instance.source_table_name)
                instance.source_params.append(exec_param)
                continue
            if param.startswith("target"):
                instance.target_table_name = "%s_%s_%s_%s" % \
                                             (instance.datasource_name_l[-1],
                                              list(value)[0], param,
                                              f"{''.join(random.sample('zyxwvutsrqponmlkjihgfedcba',2))}")
                exec_param = "%s.%s" % (instance.datasource_name_l[-1], instance.target_table_name)
                instance.target_params.append(exec_param)
                continue
        instance.gen_params.update(source=instance.source_params, target=instance.target_params)

    @classmethod
    def _test_cases_module(cls):
        test_cases_mod = []
        for f in os.listdir(test_cases_path):
            if not f.endswith(".py"):
                continue
            test_cases_mod.append(f.split(".")[0])
        return test_cases_mod


class Job:

    _args = parse_args()
    run_test_case = TestCase(_args.case)
    exec_parameter = []

    def __init__(self):

        self.source = self._args.source
        self.target = self._args.target
        self.dummy_source_name = ''
        self.init_config = self._parse_init_config
        self.field_mod = self._field_module
        self.datasource_name_l = self._datasource_name
        self.index = 0
        self.datasource_cfg = {
            "name": [name["name"] for name in config],
            "connection_type": [connection_type["connection_type"] for connection_type in config]
        }
        self.dummy_source = ''
        self.dummy_source_table = ''
        self.pipeline_ins = None
        self.cfg = {}
        self.jobs_infos = []
        self.job_test_res = []
        self.job_infos = {
            "job_name": "",
            "dag": {
                "node": {
                    "source": self.source,
                    "target": self.target,
                    "processor": None
                }
            },
            "table_fields": 1,
            "job_test_res": self.job_test_res
        }
        self.test_res = {}
        self.result = {
            "jobs_infos": self.jobs_infos
        }

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == len(self.field_mod):
            raise StopIteration
        field_mod = import_module(self.field_mod[self.index])
        columns = field_mod.columns
        self.dummy_source_name = self.datasource_cfg["name"][self.index]
        self.dummy_source = self.dummy_source_name + get_suffix()
        self.run_test_case = self._params_template
        self.job_infos = copy.copy(self.job_infos)
        self.job_infos["table_fields"] = str(len(columns))
        self.job_infos["job_test_res"] = self.job_test_res
        self.cfg.update({
            "table_name": self.dummy_source_table,
            "table_fields": columns,
            "write_interval_totals": 0
        })
        for sync_tp, step_size in self.init_config().items():
            if sync_tp == "initial_sync":
                last_time_step = 0
            for step in step_size:
                self.Metrics = self._Metrics()
                if self._args.source == "dummy" and sync_tp == "cdc":
                    self.cfg.update({
                        "incremental_interval": 1000,
                        "incremental_interval_totals": int(int(step)/len(self.source_params))
                    })
                else:
                    if self._args.source == "dummy":
                        actual_step = int(int(step)/len(self.source_params))
                    else:
                        if sync_tp == "initial_sync":
                            actual_step = int(int(step)/len(self.source_params)) - last_time_step
                            last_time_step = actual_step
                    self.cfg.update({
                        "initial_totals": actual_step,
                        "incremental_interval": 10000000,
                        "incremental_interval_totals": 1
                    })
                self.test_res = copy.copy(self.test_res)
                self.row_num = step
                self._dummy_source(self.index, self.cfg)
                self.pipeline_ins = self._gen_job()
                self.continuous_testing(self.pipeline_ins, sync_tp)
                self.test_res.update(row_num=step, sync_type=sync_tp, metrics=self.Metrics.__dict__)
                self.job_test_res.append(self.test_res)
        self.jobs_infos.append(self.job_infos)
        self.job_test_res = []
        self.index += 1
        return self.result

    class _Metrics:
        pass

    def continuous_testing(self, p, sync_type: str) -> bool:
        if sync_type == "initial_sync":
            try:
                p.job.reset()
            except AttributeError as e:
                pass
            s = time.time()
            if p.wait_status(["stop", "complete", "wait_start"]):
                p.start()
            else:
                logger.error("Task {} current status is {}, cannot start, skip this scenario...",
                             p.job.name, p.job.stats())
                return False
            logger.info("Job {} starts with {} test ...", p.job.name, sync_type)
            if p.wait_status("running"):
                logger.info("wait job start running cost: {} seconds", int(time.time() - s))
            else:
                logger.error("wait job running timeout: {} seconds, will skip it", int(time.time() - s))

            qps_metrics = []
            while True:
                # status = p.job.status()
                stats = p.job.stats()
                if stats.snapshot_start_at:
                    qps_metrics.append(stats.qps)
                if stats.snapshot_done_at:
                    sync_time = stats.snapshot_done_at - stats.snapshot_start_at
                    p.stop()
                    break
            qps = int(sum(qps_metrics) / len(qps_metrics))
            self._metrics_collect(qps=qps,
                                  sync_time=sync_time
                                  )  # Pass the metrics you want to add into the method

        logger.info(
            "job {} finish, status: {},\n" + \
            "input_stats: insert: {}, update: {}, delete: {}\n" + \
            "output_stats: insert: {}, update: {}, delete: {}",
            p.job.name, p.job.status(),
            stats.input_insert, stats.input_update, stats.input_delete,
            stats.output_insert, stats.output_update, stats.output_Delete,
            "notice", "notice", "notice", "info", "info", "info", "info", "info", "info"
        )

    def _metrics_collect(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self.Metrics, k, v)

    @property
    def _field_module(self) -> list:
        field_mod = []
        for f in os.listdir(init_data_path):
            if not f.endswith(".py"):
                continue
            field_mod.append(f.split(".")[0])
        return field_mod

    def _dummy_source(self, index: int, config: dict) -> bool:
        dum_source = DataSource("dummy", self.datasource_cfg["name"][index] + get_suffix(),
                                self.datasource_cfg["connection_type"][index])
        info = dum_source.get(connector_name=self.datasource_cfg["name"][index] + get_suffix())
        dum_source.pdk_setting.update(config)
        if info is not None:
            dum_source.update_save(info.get('id'))
            logger.info("Updated dummy data source {} succeeded, with data volume of {}",
                        self.datasource_cfg["name"][index] + get_suffix(),
                        self.row_num)
        else:
            dum_source.save()
        self._dummy_init_job()
        return True

    # TODO:
    #  1、源是dummy，目标为包括dummy在内的任意一个数据源：运行以"初始dummy源"为源头的该任务;
    #  2、源不是dummy，目标为包括dummy在内的任意一个数据源：运行以"初始dummy源"为源头，到任意"非dummy"为目标源的该任务来造数据
    def _dummy_init_job(self):  # source is not an initialization task that runs on dummy to make data
        init_dummy_source_t = "%s.%s" % (self.dummy_source, self.dummy_source_table)
        job_name = "Data_creation_from_dummy_to_%s%s" % (self._args.target, get_suffix())
        for s_param in self.source_params:
            p = Pipeline(job_name, mode="sync")
            p.config({"type": "initial_sync"})
            p.readFrom(init_dummy_source_t).writeTo(s_param)
            p.start()
            logger.info("Start running task {} to create data for {}...", job_name, s_param)
            while True:
                if p.wait_status("complete"):
                    p.job.reset()
                    break
            logger.info("Job {} data creation is complete", job_name)

    @property
    def _parse_init_config(self):
        index = 0

        def inner() -> dict:
            init_config = {}
            step_size = []
            nonlocal index
            cfg = config[index]
            init_config.update(initial_sync=step_size)
            for k, v in cfg.items():
                if k.startswith("initial_sync"):
                    step_size.append(v)
                    continue
                if step_size and step_size is init_config["initial_sync"]:
                    step_size = []
                    init_config.update(cdc=step_size)
                if k.startswith("cdc"):
                    step_size.append(v)
            index += 1
            return init_config
        return inner

    def _gen_job(self):
        pipline = self.gen_params.get("Pipeline", Pipeline)
        source = self.gen_params.get("source", "")
        target = self.gen_params.get("target", "")
        func_code = inspect.getsource(self.test_case)
        func_code = func_code + '    return p\n'
        with ManageTempFile(content=func_code) as t:
            return t(pipline, *source, *target)

    @property
    def _datasource_name(self):
        try:
            create_datasource(self._args.source, self._args.target)
        except Exception as e:
            logger.error("{}", e)
            return False
        else:
            _datasource_name = [datasource + get_suffix() for datasource in
                                get_sources(self._args.source, self._args.target)]
            return _datasource_name


if __name__ == '__main__':
    clean_cache_file()
    j = Job()
    print(j.run_test_case, '\n########################################################################################')

