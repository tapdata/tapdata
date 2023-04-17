from typing import Union
import os, sys, copy
from importlib import import_module

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../lib")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../common")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../data/init_data")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")

init_data_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../data/init_data'

from factory import newDB
from helper.suffix import get_test_table, get_suffix
from create_datasource import *
from config_parser import config
from tapdata_cli.cli import (
    Pipeline,
    Sink,
    Source,
    init
)

init()


def gen_config():
    return get_sources()


class Job:

    _datasource_name = [datasource + get_suffix() for datasource in get_sources()]

    def __init__(self, **kwargs: Union[str, list]):
        self.name = ''
        self.source = kwargs.get("source", "dummy")
        self.target = kwargs.get("target", "dummy")
        self.processing = kwargs.get("processing", None)
        self.init_config = self._parse_init_config
        self.field_mod = self._field_module
        self.index = 0
        self.datasource_cfg = {
            "name": [name["name"] for name in config],
            "connection_type": [connection_type["connection_type"] for connection_type in config]
        }
        self.cfg = {}
        self.jobs_infos = []
        self.job_test_res = []
        self.metrics = {}
        self.job_infos = {
            "job_name": self.name,
            "dag": {
                "node": {
                    "source_db_type": self.source,
                    "target": self.target,
                    "processing": self.processing
                }
            },
            "table_fields": 1,
            "job_test_res": self.job_test_res
        }
        self.test_res = {
            "sync_type": "",
            "row_num": 1,
            "metrics": self.metrics
        }
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
        self.job_infos = copy.copy(self.job_infos)
        self.job_infos["table_fields"] = str(len(columns))
        for sync_tp, step_size in self.init_config().items():
            for step in step_size:
                self.cfg.update({
                    "initial_totals": step,
                    "table_fields": columns
                })
                self.row_num = step
                self.test_res = copy.copy(self.test_res)
                self.test_res.update(row_num=step, sync_type=sync_tp)
                # self._dummy_source(self.index, self.cfg)

                self.job_test_res.append(self.test_res)
        self.job_test_res = []
        self.jobs_infos.append(self.job_infos)
        self.index += 1
        return self.result

    @property
    def _field_module(self) -> list:
        field_mod = []
        for f in os.listdir(init_data_path):
            if not f.endswith(".py"):
                continue
            field_mod.append(f.split(".")[0])
        return field_mod

    def _dummy_source(self, index: int, config: dict):
        dummy_source = DataSource("dummy", self.datasource_cfg["name"][index],
                                  self.datasource_cfg["connection_type"][index])
        info = dummy_source.get(connector_name=self.datasource_cfg["name"][index])
        if info is not None:
            dummy_source.pdk_setting.update(config)
            dummy_source.update_save(info.get('id'))
            logger.info("Updated dummy data source {} succeeded, with data volume of {}",
                        self.datasource_cfg["name"][index],
                        self.row_num)
        else:
            dummy_source.save()

    @property
    def _create_datasource(self):
        try:
            create_datasource()
        except Exception as e:
            logger.error("{}", e)
            return False
        return True

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

    def _create_job(self):
        if isinstance(self.source, str):
            pass


if __name__ == '__main__':
    j = Job()
    # for i in j:
    #     print(i)
    # print(j._datasource_name)
