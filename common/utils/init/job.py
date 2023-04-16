from typing import Union
import os, sys, yaml
from importlib import import_module

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../lib")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../common")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../data/init_data")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")
# sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../data")

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

    def __init__(self, *args: int, **kwargs: Union[str, list]):
        self.name = ''
        self.source = kwargs.get("source", "dummy")
        self.target = kwargs.get("target", "dummy")
        self.processing = kwargs.get("processing", None)
        self.step = 0
        self.step_size = args
        self.field_mod = self._field_module
        self.index = 0
        self.datasource_cfg = {
            "name": [name["name"] for name in config],
            "connection_type": [connection_type["connection_type"] for connection_type in config]
        }
        self.cfg = {}
        self.result = {
            "job_info": [
                {
                    "name": self.name,
                    "dag": {
                        "node": {
                            "source_db_type": self.source,
                            "target": self.target,
                            "processing": self.processing
                        }
                    },
                    "table_fields": str,
                    "sync_type": str,
                    "test_res": []
                }
            ]
        }

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == len(self.field_mod):
            raise StopIteration()
        for step in self.step_size:
            field_mod = import_module(self.field_mod[self.index])
            self.cfg.update({
                "initial_totals": step,
                "table_fields": field_mod.columns
            })
            self.step = step
            self._dummy_source(self.index, self.cfg)
        self.index += 1
        return self.cfg

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
                        self.step)
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

    def _create_job(self):
        if isinstance(self.source, str):
            pass


if __name__ == '__main__':

    # j = Job(1, 2, 3)
    # for i in j:
    #     pass
    pass
