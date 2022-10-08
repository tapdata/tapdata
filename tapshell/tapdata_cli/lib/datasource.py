"""
DataSource you can use it as database

Date: 2022.10.08
Author: Jerry Gao
"""
import copy
import json
import sys
import time

import asyncio
import websockets

from tapdata_cli.common.check import ConfigCheck
from tapdata_cli.common.decorate import help_decorate
from tapdata_cli.common.log import logger
from tapdata_cli.common.request import DataSourceApi
from tapdata_cli.params.datasource import pdk_config, DATASOURCE_CONFIG
from tapdata_cli.common.client_cache import client_cache


@help_decorate("Data Source, you can see it as database",
               'ds = DataSource("mysql", "mysql-datasource").host("127.0.0.1").port(3306).username().password().db()')
class DataSource:

    def __init__(self, connector, connector_name=None, connector_type="source_and_target", connector_id=None):
        """
        @param connector: pdkType name
        @param connector_name: datasource name
        @param connector_type: datasource can be used as source and target at the same time
        @param connector_id: datasource id, datasource will get config from backend by api if pass
        """
        # get datasource config
        if connector_id is not None:
            self._id = connector_id
            self._setting = self.get(connector_id=connector_id)
            return
        # name is not provide
        name = connector if connector != "" and connector_name is None else connector_name
        # if name is already exists
        obj = client_cache.get_obj("datasource", name)
        if obj is not None:
            self.id = obj.id
            self.setting = obj.c
            return
        self.id = connector_id
        self.connection_type = connector_type
        self.setting = {
            "id": self.id,
            "name": name,
            "database_type": connector,
            "connection_type": self.connection_type
        }
        self.pdk_setting = {}

    def _config_setting(self, value):
        key = sys._getframe(1).f_code.co_name
        self.setting.update({key: value})

    def _config_pdk_setting(self, value):
        key = sys._getframe(1).f_code.co_name
        if key == "uri" and value:
            self.pdk_setting.update({"isUri": True, key: value})
        else:
            self.pdk_setting.update({key: value})

    def __getattr__(self, item):
        if item in self.setting:
            return self._config_setting
        elif item in self.pdk_setting:
            return self._config_pdk_setting
        else:
            try:
                value = getattr(self, item)
                return value
            except AttributeError:
                return None

    @staticmethod
    @help_decorate("static method, used to list all datasources", res="datasource list, list")
    def list():
        return DataSourceApi().list()["data"]

    @help_decorate("desc a datasource, display readable struct", res="datasource struct")
    def desc(self, quiet=True):
        c = copy.deepcopy(self.setting)
        remove_keys = [
            "response_body",
            "user_id",
            "id",
            "transformed",
            "schemaVersion",
            "schema",
            "username",
            "everLoadSchema",
            "isUrl",
        ]
        # remove field hard to understand
        for k, v in c.items():
            if not v:
                remove_keys.append(k)

        for k in remove_keys:
            if k in c:
                del (c[k])

        if not quiet:
            print(json.dumps(c, indent=4))

        return c

    @help_decorate("get a datasource status", "")
    def status(self, quiet=True):
        if self.id is None:
            logger.warn("datasource is not save, please save first")
            return
        info = self.get(self.id)
        status = info.get("status")
        tableCount = info.get("tableCount", "unknown")
        loadCount = info.get("loadCount", 0)
        loadFieldsStatus = info.get("loadFieldsStatus", False)
        loadSchemaDate = info.get("loadSchemaDate", "unknown")
        if not quiet:
            logger.info("datasource {} status is: {}, it has {} tables, loaded {}, last load time is: {}",
                        info.get("name"), status, tableCount, loadCount, loadSchemaDate)
        return status

    def to_dict(self):
        """
        1. get datasource config by connector
        2. check the settings by ConfigCheck
        3. add pdk_setting
        """
        # get database_type check rules
        res = ConfigCheck(self.setting, DATASOURCE_CONFIG, keep_extra=True).checked_config
        self.setting.update(res)
        self.setting.update({"config": self.to_pdk_dict()})
        return self.setting

    def to_pdk_dict(self):
        """
        1. get datasource config by connector
        2. check the settings by ConfigCheck
        """
        mode = "uri" if self.setting.get("isUri") else "form"
        res = ConfigCheck(self.pdk_setting, pdk_config[self.database_type.lower()][mode], keep_extra=True).checked_config
        self.pdk_setting.update(res)
        return self.pdk_setting

    @staticmethod
    @help_decorate("get a datasource by it's id or name", args="id or name, using kargs", res="a DataSource Object")
    def get(connector_id=None, connector_name=None):
        if connector_id is not None:
            f = {
                "where": {
                    "id": connector_id,
                }
            }
        else:
            f = {
                "where": {
                    "name": connector_name,
                }
            }
        params = {
            "filter": json.dumps(f)
        }
        data = DataSourceApi().list(params=params)["data"]
        if len(data["items"]) == 0:
            return None
        return data["items"][0]

    def save(self):
        data = self.to_dict()
        if data is None:
            return
        data = DataSourceApi().post(data)
        show_connections(quiet=True)
        if data["code"] == "ok":
            self.id = data["data"]["id"]
            self.c = DataSource.get(self.id)
            self.validate(quiet=False)
            return True
        else:
            logger.warn("save Connection fail, err is: {}", data["message"])
        return False

    def delete(self):
        if self.id is None:
            return
        data = DataSourceApi().delete(self.id, self.c)
        if data["code"] == "ok":
            logger.info("delete {} Connection success", self.id)
            return True
        else:
            logger.warn("delete Connection fail, err is: {}", data)
        return False

    @help_decorate("validate this datasource")
    def validate(self, quiet=False):
        res = True

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                data = self.to_dict()
                data["updateSchema"] = True
                if isinstance(self.id, str):
                    data.update({
                        "id": self.id,
                    })
                data.update({
                    "accessNodeType": "AUTOMATIC_PLATFORM_ALLOCATION"
                })
                payload = {
                    "type": "testConnection",
                    "data": data
                }
                logger.info("start validate datasource config, please wait for a while ...")
                await websocket.send(json.dumps(payload))

                while True:
                    recv = await websocket.recv()
                    loadResult = json.loads(recv)
                    if loadResult["type"] != "pipe":
                        continue
                    if loadResult["data"]["type"] != "testConnectionResult":
                        continue
                    if loadResult["data"]["result"]["status"] is None:
                        continue

                    if loadResult["data"]["result"]["status"] != "ready":
                        res = False
                    else:
                        res = True

                    if not quiet:
                        if loadResult["data"]["result"] is None:
                            continue
                        for detail in loadResult["data"]["result"]["response_body"]["validate_details"]:
                            if detail["fail_message"] is not None:
                                logger.log("{}: {}, message: {}", detail["show_msg"], detail["status"],
                                           detail["fail_message"], "debug", "info",
                                           "info" if detail["status"] == "passed" else "warn")
                            else:
                                logger.log("{}: {}", detail["show_msg"], detail["status"], "debug", "info")
                    await websocket.close()
                    return res

        try:
            asyncio.run(l())
        except Exception as e:
            logger.warn("load schema exception, err is: {}", e)
        logger.info("datasource valid finished, will check table schema now, please wait for a while ...")
        start_time = time.time()
        while True:
            try:
                time.sleep(5)
                res = DataSourceApi().get(self.id)
                if res["data"] is None:
                    break
                if "loadFieldsStatus" not in res["data"]:
                    continue
                if res["data"]["loadFieldsStatus"] == "finished":
                    break
                loadCount = res["data"].get("loadCount", 0)
                tableCount = res["data"].get("tableCount", 1)
                logger.info("table schema check percent is: {}%", int(loadCount / tableCount * 100), wrap=False)
            except Exception as e:
                break
        logger.info("datasource table schema check finished, cost time: {} seconds", int(time.time() - start_time))
        return res
