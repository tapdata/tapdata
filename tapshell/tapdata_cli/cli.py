import argparse
import copy
import functools
import json
import shlex
import sys
import time
import traceback
import uuid
import re
from logging import *
from platform import python_version
from typing import Iterable, Tuple, Sequence

import asyncio
import requests
import websockets
from IPython.core.magic import Magics, magics_class, line_magic
from IPython.terminal.interactiveshell import TerminalInteractiveShell

if not python_version().startswith("3"):
    print("python version must be 3.x, please install python3 before using tapdata cli")
    sys.exit(-1)
import os

os.environ['PYTHONSTARTUP'] = '>>>'
os.environ["PROJECT_PATH"] = os.sep.join([os.path.dirname(os.path.abspath(__file__)), ".."])

from tapdata_cli.graph import Node, Graph
from tapdata_cli.check import ConfigCheck
from tapdata_cli.log import logger, get_log_level
from tapdata_cli.config_parse import config
from tapdata_cli.request import DataSourceApi, InspectApi, TaskApi, set_req
from tapdata_cli.params.datasource import pdk_config, DATASOURCE_CONFIG
from tapdata_cli.params.job import job_config, node_config, node_config_sync

server = config["backend.server"]
req = set_req(server)
access_code = config["backend.access_code"]


# Helper class, used to provide operation tips
class Help:
    def __init__(self, name, desc, args=None, res=None):
        self.name = name
        self.desc = desc
        self.args = args
        self.res = res


command_help_list = {}
lib_help_list = []
lib_methods_list = {}


def help_decorate(h, args=None, res=None):
    def decorator(obj):
        h2 = h
        if 'Enum' in h2:
            h2 = pad(h2 + ", valid value is: ", 45)
            h2 = h2 + '`'
            for i in list(obj.__dict__.keys()):
                if not i.startswith("__"):
                    h2 = h2 + i + ", "
            h2 = h2.rstrip(", ")
            h2 = h2 + "`"
        context = []
        if h2.startswith("[") and "]" in h2:
            context = h2[1:h2.index("]")].split(",")
            h2 = h2[h2.index("]") + 1:]
        help_obj = Help(
            name=obj.__name__,
            desc=h2,
            args=args,
            res=res,
        )
        obj_print = " ".join(map(str, (obj,)))
        source = obj_print.split(" ")[0][1:]
        if source == "function":
            try:
                full_method = obj_print.split(" ")[1]
                class_name = full_method.split(".")[0]
                method_name = full_method.split(".")[1]
                if class_name[0].isupper():
                    if lib_methods_list.get(class_name) is None:
                        lib_methods_list[class_name] = []
                    lib_methods_list[class_name].append(help_obj)
                else:
                    if not context:
                        if command_help_list.get("default", None) is None:
                            command_help_list["default"] = []
                        command_help_list["Default"].append(help_obj)
                    else:
                        for c in context:
                            if command_help_list.get(c, None) is None:
                                command_help_list[c] = []
                            desc = help_obj.desc.replace("object", c.lower())
                            args2 = help_obj.args.replace("object", c.lower())
                            help_obj_copy = copy.deepcopy(help_obj)
                            help_obj_copy.desc = desc
                            help_obj_copy.args = args2
                            command_help_list[c].append(help_obj_copy)
            except Exception as e:
                pass
        else:
            lib_help_list.append(help_obj)

        @functools.wraps(obj)
        def func_wrapper(*args, **kargs):
            return obj(*args, **kargs)

        if source == "function":
            return func_wrapper
        else:
            return obj

    return decorator


def class_help_decorate(h):
    def decorator(cls):
        return cls

    return decorator


# system server conf, will become readonly after login
system_server_conf = {
    "api": "",
    "access_code": "",
    "token": "",
    "user_id": "",
    "username": "",
    "cookies": {},
    "ws_uri": "",
    "auth_param": ""
}

# language config
i18n = {
    "en": {
        "login": "login tapdata opensource server",
        "logout": "logout tapdata opensource server",
        "server": "init tapdata opensource server address",
        "topic_help": "show all support command namespace",
        "system_help": "show system command, priority 0",
        "login_success_with_access_code": "login tapdata with access_code success!",
        "logout_success": "tapdata client logout success",
        "ready": "ready",
        "invalid": "invalid",
        "testing": "testing",
        "command_help": "tapdata opensource client support command mode usage, type `h command` to list all commands and it's usage",
        "lib_help": "tapdata opensource client support lib mode usage, type `h lib` to list all Basic Class and it's usage",
        "unknown": "unknown"
    },
    "zh": {
        "login": "登录 tapdata 实时数据服务平台",
        "logout": "登出 tapdata 实时数据服务平台",
        "server": "设置 tapdata 实时数据服务平台地址",
        "topic_help": "显示所有支持的命令空间",
        "system_help": "显示 系统 级别命令, 优先级 0",
        "login_success_with_access_code": "iDaas 客户端使用 access_code 登录成功",
        "logout_success": "iDaas 客户端登出成功",
        "ready": "可用",
        "invalid": "不可用",
        "testing": "测试中",
        "command_help": "tapdata开源客户端支持command模式用法，键入`h command`列出所有命令及其用法",
        "lib_help": "tapdata开源客户端支持lib模式用法，键入`h lib`列出所有基类及其用法",
        "unknown": "未知的"
    }
}

# set default display language
_lang = "en"
_l = i18n[_lang]

help_args = {
    "command": "command_help",
    "lib": "lib_help",
}


# some static utils, simple and no direct relation with this tool


# pad a string to a certain length
def pad(string, length):
    string = str(string)

    def len_zh(data):
        temp = re.findall('[^_\-a-zA-Z$0-9. #()\',\\\\/]+', data)
        count = 0
        for i in temp:
            count += len(i)
        return count

    zh = len_zh(string)
    if len(string) >= length:
        return string
    return string + " " * (length - len(string) - zh)


# operation tips when type h
def show_help(t):
    if t == "signature":
        logger.info(
            "signature is used to name a object, it can be: {}, object type includes; {}",
            "id, short id, name",
            "datasource, job, api"
        )
        return

    s = None
    if "." in t:
        s = "."
    if " " in t:
        s = " "
    if s is not None:
        t = t.split(s)[-1]

    l = None
    if t == "command":
        logger.notice(
            "{} is used to name a object, it can be: {}, object type includes; {}\n",
            "signature",
            "id, short id, name",
            "datasource, job, api"
        )
        l = command_help_list
    if t == "lib":
        logger.notice("type {} get detail help, for example: h lib Pipeline\n", "h lib $name")
        l = lib_help_list
    if l is None:
        if lib_methods_list.get(t) is not None:
            l = lib_methods_list.get(t)

    if l is None:
        logger.warn("no help info for {}", t)
        return

    logger.log("{} {} {}", pad("{} name".format(t), 25), pad("desc", 70), "example usage", "info", "info", "info")
    logger.notice("{}", "-" * 120)
    enums = []
    relations = []
    if type(l) == type({}):
        for context, commands in l.items():
            if not commands:
                continue
            logger.notice("{} commands:\n", context)
            for command in commands:
                logger.log(
                    "{} {} {}",
                    pad(command.name, 15), pad(command.desc, 50), command.args,
                    "notice", "debug", "info"
                )
            logger.notice("{}", "-" * 120)
        return

    for i in range(len(l)):
        h = l[i]
        if "Enum" in h.desc:
            enums.append(h)
            continue
        if "Relation" in h.desc:
            relations.append(h)
            continue

        if h.args is None:
            logger.log("{} {}", pad(h.name, 25), pad(h.desc, 70), "notice", "debug")
        else:
            logger.log("{} {} {}", pad(h.name, 25), pad(h.desc, 70), h.args, "notice", "debug", "info")

    if len(relations) > 0:
        logger.info("")
        logger.info("{}", "below is relation object, it used to describe how source and sink linked")
        for h in relations:
            logger.log("{}: {}", pad(h.name, 15), h.desc, "notice", "debug")
    if len(enums) > 0:
        logger.info("")
        logger.info("{}", "below is enum object")
        for h in enums:
            logger.log("{} {}", pad(h.name, 15), pad(h.desc, 50), "notice", "debug")


@magics_class
class global_help(Magics):
    @line_magic
    @help_decorate("show global help", "h command")
    # h line_magic
    def h(self, t=None):
        if not t:
            for k, v in help_args.items():
                logger.log("{}: {}", k, _l[v], "info", "debug")
            return
        try:
            show_help(t)
        except Exception as e:
            logger.warn("no help commands for {} found, please use below command for help, e is: {}", t, e)
            self.h()


# global client cache
client_cache = {
    "tables": {
    },
    "apis": {
        "name_index": {}
    },
    "apiserver": {
        "name_index": {}
    },
    "connectors": {}
}


######################################################################################################################
# some global utils, direct relation with this tool
# get signature index type
def get_index_type(s):
    try:
        number_index = int(s)
        return "number_index"
    except Exception as e:
        pass
    from bson.objectid import ObjectId
    try:
        id_index = ObjectId(s)
        return "id_index"
    except Exception as e:
        pass
    if len(s) == 6:
        for i in s:
            if ("0" <= i <= "9") or ("a" <= i <= "f"):
                continue
            return "name_index"
    else:
        return "name_index"
    return "short_id_index"


def match_line(m, line):
    for i in m:
        if i.endswith(line):
            return i
    return line


def get_signature_v(object_type, signature):
    cache_map_index = op_object_command_class[object_type]["cache"]
    if client_cache.get(cache_map_index) is None or object_type == "api":
        exec("show_" + cache_map_index + "(quiet=True)")
    index_type = get_index_type(signature)
    if index_type == "short_id_index":
        signature = match_line(client_cache[cache_map_index]["id_index"], signature)
        index_type = "id_index"
    return client_cache[cache_map_index][index_type].get(signature)


# get a object with signature
def get_obj(object_type, signature):
    obj = get_signature_v(object_type, signature)
    if obj is None:
        return None
    obj_id = obj["id"]
    obj_name = obj.get("name")
    obj = op_object_command_class[object_type]["obj"](id=obj_id)
    if object_type == "api":
        obj = op_object_command_class[object_type]["obj"](name=obj_name)
    return obj


# get fields for a certain table
def get_table_fields(t, whole=False, source=None, cache=True):
    global client_cache
    if source is None and client_cache.get("connection") is not None:
        source = client_cache.get("connection")
    if source is None:
        return None

    table_id = ""
    index_type = get_index_type(t)
    if index_type == "short_id_index":
        t = match_line(client_cache["tables"]["id_index"], t)
        index_type = "id_index"
    if index_type == "id_index":
        table_id = t
    if client_cache["tables"].get(t) is None:
        show_tables(quiet=True, source=source)

    table = client_cache["tables"][source][index_type].get(t, None)
    if table is None:
        show_tables(quiet=True, source=source)
    table = client_cache["tables"][source][index_type].get(t, None)
    if table is None:
        logger.warn("table {} not find in system", t)
        return

    table_id = table["id"]
    table_name = table["original_name"]
    res = req.get("/MetadataInstances/" + table_id)

    data = res.json()["data"]
    fields = data["fields"]
    if whole:
        return fields
    display_fields = {}
    for f in fields:
        node = display_fields
        field_names = f["field_name"].split(".")
        for i in range(len(field_names)):
            field_name = field_names[i]
            if node.get(field_name) is None:
                node[field_name] = {}
            if i < len(field_names) - 1:
                node = node[field_name]
                continue
            if f["data_type"] == "DOCUMENT":
                continue
            node[field_name] = f["data_type"]
    return display_fields


# generate dag stage, used by dag object, stage is used to describe a dag in server
def gen_dag_stage(obj):
    objType = type(obj)

    if objType == Source:
        return obj.to_dict()

    if objType == Sink:
        return obj.to_dict()

    if objType == Merge:
        return obj.to_dict()

    if obj.func_header:
        return {
            "attrs": {
                "accessNodeProcessId": "",
                "connectionType": "source_and_target",
                "position": [0, 0]
            },
            "id": obj.id,
            "name": objType.__name__,
            "type": "js_processor",
            "script": "function process(record){\n\n\t// Enter you code at here\n%s}" % obj.to_js(),
        }
    else:
        return {
            "attrs": {
                "accessNodeProcessId": "",
                "connectionType": "source_and_target",
                "position": [0, 0]
            },
            "id": obj.id,
            "name": objType.__name__,
            "type": "js_processor",
            "script": obj.to_js(),
        }


################################################################################################################


# show all connectors
def show_connectors(quiet=False):
    res = req.get("/DatabaseTypes")
    data = res.json()["data"]
    global client_cache
    for i in range(len(data)):
        client_cache["connectors"][data[i]["name"].lower()] = {
            "pdkHash": data[i]["pdkHash"],
            "pdkId": data[i]["pdkId"],
            "pdkType": "pdk",
            "name": data[i]["name"]
        }
        if not quiet:
            logger.info("{} " + data[i]["name"], data[i]["id"][-6:])


# show all jobs
def show_pipelines(quiet=False):
    show_jobs(quiet)


# show all jobs
def show_jobs(quiet=False):
    f = {
        "limit": 10000,
        "fields": {
            "syncType": True,
            "id": True,
            "name": True,
            "status": True,
            "last_updated": True,
            "createTime": True,
            "user_id": True,
            "startTime": True,
            "agentId": True,
            "statuses": True,
            "type": True,
            "desc": True
        }
    }
    res = req.get("/Task", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    global client_cache
    jobs = {"name_index": {}, "id_index": {}, "number_index": {}}
    logger.info("system has {} jobs", len(data))
    for i in range(len(data)):
        if "name" not in data[i]:
            continue
        if not quiet:
            logger.log(
                "{}: " + pad(data[i]["name"], 42) + " {} {}", data[i]["id"][-6:],
                pad(data[i].get("status", "unkownn"), 12),
                data[i].get("syncType", "unknown") + "/" + data[i].get("type", "unknown"),
                "debug", "info" if data[i].get("status", "unkownn") != "error" else "error", "notice"
            )
        jobs["name_index"][data[i]["name"]] = data[i]
        jobs["id_index"][data[i]["id"]] = data[i]
        jobs["number_index"][str(i)] = data[i]
    client_cache["jobs"] = jobs


def show_apiserver(quite=False):
    global client_cache
    items = ApiServer.list()
    if not quite:
        logger.log(
            "{} {} {}",
            pad("id", 20),
            pad("name", 20),
            pad("uri", 40),
            "debug", "debug", "debug"
        )
    for i, v in enumerate(items):
        client_cache["apiserver"]["name_index"][v["clientName"]] = {
            "id": v["id"],
            "name": v["clientName"],
            "uri": v["clientURI"],
        }
        if not quite:
            logger.log(
                "{} {} {}",
                pad(v["id"][:6], 20),
                pad(v["clientName"], 20),
                pad(v["clientURI"], 40),
                "notice", "info", "notice"
            )


# show all apis
def show_apis(quiet=False):
    global client_cache
    res = req.get("/Modules", params={"order":"createAt DESC","limit":20,"skip":0,"where":{}})
    data = res.json()["data"]["items"]
    client_cache["apis"]["name_index"] = {}
    if not quiet:
        logger.log(
            "{} {} {} {} {}",
            pad("api_name", 20),
            pad("tablename", 20),
            pad("basePath", 20),
            pad("status", 10),
            "test url", "debug", "debug", "debug", "debug", "debug"
        )
    for i in range(len(data)):
        client_cache["connections"]["id_index"][data[i]["datasource"]]["name"]
        client_cache["apis"]["name_index"][data[i]["name"]] = {
            "id": data[i]["id"],
            "table": data[i]["tableName"],
            "name": data[i]["name"],
            "tableName": data[i]["tableName"],
            "database": client_cache["connections"]["id_index"][data[i]["datasource"]]["name"],
        }
        if not quiet:
            logger.log(
                "{} {} {} {} {}",
                pad(data[i]["name"], 20),
                pad(data[i]["tableName"], 20),
                pad(data[i]["basePath"], 20),
                pad(data[i]["status"], 10),
                "http://" + server + "#/apiDocAndTest?id=" + data[i]["basePath"] + "_v1",
                "notice", "info", "info", "info" if data[i]["status"] == "active" else "warn", "notice"
            )


# show datasources
def show_dbs():
    show_connections()


# show datasources
def show_datasources():
    show_connections()


# show connections
def show_connections(f=None, quiet=False):
    global client_cache
    f = {"limit": 10000}
    res = req.get("/Connections", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    client_cache["connections"] = {"name_index": {}, "id_index": {}, "number_index": {}}
    if not quiet:
        logger.log(
            "{} {} {} {}",
            pad("id", 10),
            pad("status", 10),
            pad("database_type", 20),
            pad("name", 35),
            "debug", "debug", "debug", "debug"
        )
    for i in range(len(data)):
        try:
            if "name" not in data[i]:
                continue
            client_cache["connections"]["name_index"][data[i]["name"]] = data[i]
            client_cache["connections"]["id_index"][data[i]["id"]] = data[i]
            client_cache["connections"]["number_index"][str(i)] = data[i]
        except Exception as e:
            continue

        try:
            exec(data[i]["name"] + " = QuickDataSourceMigrateJob()", globals())
            exec(data[i]["name"] + ".__db__ = " + '"' + data[i]["name"] + '"', globals())
        except Exception as e:
            pass

        if not quiet:
            status = data[i].get("status", "unknown")
            name = data[i].get("name", "unknown")
            logger.log(
                "{} {} {} {}",
                pad(data[i]["id"][-6:], 10),
                pad(_l[status], 10),
                pad(data[i]["database_type"], 20),
                pad(name, 35),
                "debug", "info" if status == "ready" else "warn", "notice", "debug"
            )


# show tables, must be used after use command
def show_tables(source=None, quiet=False):
    global client_cache
    if source is None:
        source = client_cache.get("connection")
    if source is None:
        logger.log(
            "{} before show tables, please use connection first, you can {}, OR {}, OR {}",
            "NO connection USE,",
            "use connection_id",
            "use connection_number",
            "use 'connection_name'",
            "warn", "notice", "notice", "notice"
        )
        return
    source_name = client_cache["connections"]["id_index"][source]["name"]
    f = {"where": {"source.id": source}, "limit": 999999}
    res = req.get("/MetadataInstances", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    client_cache["tables"][source] = {"name_index": {}, "id_index": {}, "number_index": {}}
    tables = []
    each_line_table_count = 5
    each_line_tables = []
    max_table_name_len = 0
    for i in range(len(data)):
        if len(data[i]["original_name"]) > max_table_name_len:
            max_table_name_len = len(data[i]["original_name"])

    for i in range(len(data)):
        if data[i]["meta_type"] == "database":
            continue
        tables.append(data[i])
        client_cache["tables"][source]["name_index"][data[i]["original_name"]] = data[i]
        client_cache["tables"][source]["id_index"][data[i]["id"]] = data[i]
        client_cache["tables"][source]["number_index"][str(i)] = data[i]
        try:
            exec(source_name + "." + data[i]["original_name"] + "=" + '"' + source_name + "." + data[i][
                "original_name"] + '"', globals())
        except Exception as e:
            pass
        if not quiet:
            if len(each_line_tables) == each_line_table_count:
                logger.log("{} " * each_line_table_count, *each_line_tables,
                           *["notice" for i in range(each_line_table_count)])
                each_line_tables = []
            each_line_tables.append(pad(data[i]["original_name"], max_table_name_len))
    return tables


# a quick datasource migrate job create direct use db name
# you can use A.syncTo(B) create a migrate job very fast
class QuickDataSourceMigrateJob:
    def __init__(self):
        self.__db__ = ""
        self.__p__ = None

    def __getattr__(self, key):
        if key in dir(self):
            return getattr(self, key)
        return self.__db__ + "." + key

    def syncTo(self, target, table=["_"], prefix="", suffix=""):
        p = Pipeline(self.__db__ + "_sync_to_" + target.__db__)
        source = Source(self.__db__, table=table)
        p.readFrom(source).writeTo(target.__db__, prefix=prefix, suffix=suffix)
        self.__p__ = p
        return self.__p__

    def start(self):
        if self.__p__ is None:
            logger.warn("no sync job create, can not start...")
            return self.__db__ + "." + "start"
        self.__p__.start()
        return self.__p__

    def status(self):
        if self.__p__ is None:
            logger.warn("no sync job create, can not status...")
            return self.__db__ + "." + "status"
        self.__p__.status()
        return self.__p__

    def monitor(self):
        if self.__p__ is None:
            logger.warn("no sync job create, can not monitor...")
            return self.__db__ + "." + "monitor"
        self.__p__.monitor()
        return self.__p__

    def stop(self):
        if self.__p__ is None:
            logger.warn("no sync job create, can not stop...")
            return self.__db__ + "." + "stop"
        self.__p__.stop()
        return self.__p__

    def delete(self):
        ds = get_obj("datasource", self.__db__)
        if ds is not None:
            if ds.delete():
                logger.info("delete datasource {} success", self.__db__)
            else:
                logger.warn("delete datasource {} fail, maybe some job is still use it", self.__db__)
            return
        logger.warn("datasource {} not found", self.__db__)


@magics_class
# global command for object
class op_object_command(Magics):
    def __common_op(self, op, line):
        object_type, signature = line.split(" ")[0], line.split(" ")[1]
        args = []
        kwargs = {}
        if len(line.split(" ")) > 2:
            for kv in line.split(" ")[2:]:
                if "=" not in kv:
                    args.append(kv)
                else:
                    v = kv.split("=")[1]
                    try:
                        v = int(v)
                        kwargs[kv.split("=")[0]] = v
                    except Exception as e:
                        if v.lower() == "true":
                            kwargs[kv.split("=")[0]] = True
                            continue
                        if v.lower() == "false":
                            kwargs[kv.split("=")[0]] = False
                            continue
                        kwargs[kv.split("=")[0]] = v
        obj = get_obj(object_type, signature)
        if obj is None:
            return
        if op in dir(obj):
            import inspect
            method_args = inspect.getfullargspec(getattr(obj, op)).args
            if "quiet" in method_args:
                kwargs["quiet"] = False
            getattr(obj, op)(*args, **kwargs)

    @line_magic
    @help_decorate("[Job] stop a running job", "stop job $job_name")
    def stop(self, line):
        return self.__common_op("stop", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api] display a object status", "status datasource $datasource_name")
    def status(self, line):
        return self.__common_op("status", line)

    @line_magic
    @help_decorate("[Job] keep monitor a object status", "monitor job $job_name t=30")
    def monitor(self, line):
        return self.__common_op("monitor", line)

    @line_magic
    @help_decorate("[Job] start a job", "start job $job_name")
    def start(self, line):
        return self.__common_op("start", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api] delete a object", "delete object $object_name")
    def delete(self, line):
        return self.__common_op("delete", line)

    @line_magic
    @help_decorate("[Datasource] validate a datasource, and load it's schema", "validate datasource $datasource_id")
    def validate(self, line):
        return self.__common_op("validate", line)

    @line_magic
    @help_decorate("[Job] display job logs", "logs job $job_name limit=100 tail=True")
    def logs(self, line):
        return self.__common_op("logs", line)

    @line_magic
    @help_decorate("[Job] display a job stats", "stats job $job_name")
    def stats(self, line):
        return self.__common_op("stats", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api,Table] desc a object", "desc object $object_name")
    def desc(self, line):
        if line == "":
            logger.warn("no desc datasource found")
            return
        if " " not in line or line.split(" ")[0] == "table":
            if " " in line:
                line = line.split(" ")[1]
            return desc_table(line)
        return self.__common_op("desc", line)


def show_db(line):
    if line == "":
        logger.warn("no show object found")
        return
    connection = get_signature_v("connection", line)
    display = {}
    for k, v in connection.items():
        if v is None or v == "":
            continue
        display[k] = v


@magics_class
class ApiCommand(Magics):
    @line_magic
    def unpublish(self, line):
        if len(client_cache["apis"]["name_index"]) == 0:
            show_apis()
        payload = {
            "id": client_cache["apis"]["name_index"][line]["id"],
            "tablename": client_cache["apis"]["name_index"][line]["table"],
            "status": "pending"
        }
        res = req.patch("/Modules", json=payload)
        res = res.json()
        if res["code"] == "ok":
            logger.info("unpublish {} success", line)
        else:
            logger.warn("unpublish {} fail, err is: {}", line, res)

    @line_magic
    def publish(self, line):
        if " " not in line:
            return

        base_path = line.split(" ")[0]
        line = line.split(" ")[1]

        global client_cache
        if client_cache.get("connections") is None and "." not in line:
            logger.warn("no DataSource set, only table is not enough")
            return
        db = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            if client_cache.get("connections") is None:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                logger.warn("no Datasource {} found in system", db)
            db = client_cache["connections"]["name_index"][db]["id"]

        fields = get_table_fields(table, whole=True, source=db)
        payload = {
            "apiType": "defaultApi",
            "apiVersion": "v1",
            "basePath": base_path,
            "createType": "",
            "datasource": db,
            "describtion": "",
            "name": base_path,
            "path": "/api/v1/" + base_path,
            "readConcern": "majority",
            "readPreference": "primary",
            "status": "active",
            "tablename": table,
            "fields": fields,
            "paths": [
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Create a new record",
                    "method": "POST",
                    "name": "create",
                    "path": "/api/v1/" + base_path,
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records based on id",
                    "method": "GET",
                    "name": "findById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "/{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Update record according to id",
                    "method": "PATCH",
                    "name": "updateById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Delete records based on id",
                    "method": "DELETE",
                    "name": "deleteById",
                    "params": [
                        {
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records by page",
                    "method": "GET",
                    "name": "findPage",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "page number",
                            "name": "page",
                            "type": "int"
                        },
                        {
                            "defaultvalue": 20,
                            "description": "max records per page",
                            "name": "limit",
                            "type": "int"
                        },
                        {
                            "description": "sort setting,Array ,format like [{'propertyName':'ASC'}]",
                            "name": "sort",
                            "type": "object"
                        },
                        {
                            "description": "search filter object,Array",
                            "name": "filter",
                            "type": "object"
                        }
                    ],
                    "path": "/api/v1/" + base_path,
                    "result": "Page<Document>",
                    "type": "preset"
                }
            ]
        }
        res = req.post("/Modules", json=payload).json()
        if res["code"] == "ok":
            logger.info(
                "publish api {} success, you can test it by: {}",
                base_path,
                "http://" + server + "#/apiDocAndTest?id=" + base_path + "_v1"
            )
        else:
            logger.warn("publish api {} fail, err is: {}", base_path, res["message"])


@magics_class
class show_command(Magics):
    @line_magic
    @help_decorate("[Job,Datasource,Api,Table] show objects", "show objects")
    def show(self, line):
        if not line:
            pass
        try:
            eval("show_" + line + "()")
        except Exception as e:
            print(traceback.format_exc())
            eval("show_db('" + line + "')")

    @line_magic
    # load a python file, and exec it
    @help_decorate("[System] load a script file, and exec it", "load script.py")
    def load(self, line):
        exec(open(line).read())

    @line_magic
    @help_decorate("[Datasource] switch datasource context", "use $object_name")
    def use(self, line):
        if line == "":
            logger.warn("no use datasource found")
            return
        global client_cache
        connection = get_signature_v("datasource", line)
        connection_id = connection["id"]
        connection_name = connection["name"]
        client_cache["connection"] = connection_id

        logger.info("datasource switch to: {}", connection_name)

    @line_magic
    @help_decorate("[Table] peek 5 table content for preview", "peek $table_name")
    def peek(self, line):
        if line == "":
            logger.warn("no peek datasource found")
            return
        global client_cache
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        connection_id = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            connection = get_signature_v("datasource", db)
            connection_id = connection["id"]

        table_id = ""
        index_type = get_index_type(line)
        if index_type == "short_id_index":
            line = match_line(client_cache["tables"]["id_index"], line)
            index_type = "id_index"
        if index_type == "id_index":
            table_id = line
        if client_cache["tables"].get(connection_id) is None:
            show_tables(source=connection_id, quiet=True)
        table = client_cache["tables"][connection_id][index_type][table]

        table_id = table["id"]
        table_name = table["original_name"]

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                payload = {
                    "type": "data_preview",
                    "data": {
                        "connectionId": connection_id,
                        "limit": 5,
                        "tableName": table_name
                    }
                }
                await websocket.send(json.dumps(payload))

                while True:
                    loadResult = json.loads(await websocket.recv())
                    if loadResult["type"] != "pipe":
                        continue
                    if loadResult["data"]["type"] != "dataPreviewResult":
                        continue
                    res = loadResult["data"]["result"][table_name]
                    data = res["data"]
                    count = res["count"]
                    for row in data:
                        print(row)
                    await websocket.close()
                    return res

        try:
            asyncio.get_event_loop().run_until_complete(l())
        except Exception as e:
            logger.warn("peek table exception, err is: {}", e)

    @line_magic
    @help_decorate("[Table] count table rows", "count $table_name")
    def count(self, line):
        if line == "":
            logger.warn("no count datasource found")
            return
        global client_cache
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        connection_id = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            connection = get_signature_v("datasource", db)
            connection_id = connection["id"]
        table_id = ""
        index_type = get_index_type(line)
        if index_type == "short_id_index":
            line = match_line(client_cache["tables"]["id_index"], line)
            index_type = "id_index"
        if index_type == "id_index":
            table_id = line
        if client_cache["tables"].get(connection_id) is None:
            show_tables(source=connection_id, quiet=True)
        table = client_cache["tables"][connection_id][index_type][table]
        table_id = table["id"]
        table_name = table["original_name"]

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                payload = {
                    "type": "data_preview",
                    "data": {
                        "connectionId": connection_id,
                        "limit": 10,
                        "tableName": table_name
                    }
                }
                await websocket.send(json.dumps(payload))

                while True:
                    loadResult = json.loads(await websocket.recv())
                    if loadResult["type"] != "pipe":
                        continue
                    if loadResult["data"]["type"] != "dataPreviewResult":
                        continue
                    res = loadResult["data"]["result"][table_name]
                    data = res["data"]
                    count = res["count"]
                    print(count)
                    await websocket.close()
                    return res

        try:
            asyncio.get_event_loop().run_until_complete(l())
        except Exception as e:
            logger.warn("count table exception, err is: {}", e)


@help_decorate("display table struct", "table signature")
def desc_table(line):
    global client_cache
    connection_id = client_cache.get("connection")
    db = connection_id
    if "." in line:
        db = line.split(".")[0]
        line = line.split(".")[1]
    index_type = get_index_type(db)
    if index_type == "short_id_index":
        db = match_line(client_cache["connections"]["id_index"], db)
        index_type = "id_index"
    if index_type == "id_index":
        client_cache["connection"] = db
    if client_cache.get("connections") is None:
        show_connections(quiet=True)
    if db is None:
        logger.warn("please {} before desc table, or {} to get a valid result", "use db", "use db.table")
        return

    connection = client_cache["connections"][index_type][db]
    connection_id = connection["id"]

    if connection_id is None:
        return

    display_fields = get_table_fields(line, source=connection_id)


def login_with_access_code(server, access_code):
    global system_server_conf, req
    req = set_req(server)
    api = "http://" + server + "/api"
    res = req.post("/users/generatetoken", json={"accesscode": access_code})
    if res.status_code != 200:
        logger.warn("init get token request fail, err is: {}", res.json())
        return False
    data = res.json()["data"]
    token = data["id"]
    user_id = data["userId"]
    req.params = {"access_token": token}
    res = req.get("/users")
    if res.status_code != 200:
        logger.warn("get user info by token fail, err is: {}", res.json())
        return False
    username = None
    users = res.json()["data"]["items"]
    for user in users:
        if user["id"] == user_id:
            username = user.get("username", "")
            break
    if token is None:
        return False
    cookies = {"user_id": user_id}
    req.cookies = requests.cookies.cookiejar_from_dict(cookies)
    ws_uri = "ws://" + server + "/ws/agent?access_token=" + token
    logger.info("{}", _l["login_success_with_access_code"])
    system_server_conf = {
        "api": api,
        "access_code": access_code,
        "token": token,
        "user_id": user_id,
        "username": username,
        "cookies": cookies,
        "ws_uri": ws_uri,
        "auth_param": "?access_token=" + token
    }
    logger.notice("please type {} get global help", "h")
    return True


def login_with_password(server, username, password):
    pass


@magics_class
# system magics_class
class system_command(Magics):
    @line_magic
    @help_decorate("[System] login system", "login -s server_address -u username -p password `OR` login -a access_code")
    def login(self, line):
        if not line:
            logger.warn("args can not be empty for login")
            return
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--server", type=str)
        parser.add_argument("-u", "--username", type=str)
        parser.add_argument("-p", "--password", type=str)
        parser.add_argument("-a", "--access_code", type=str)
        args = parser.parse_args(shlex.split(line))
        if not args.server:
            args.server = "127.0.0.1:3030"
        if args.access_code:
            login_with_access_code(args.server, args.access_code)
            return
        login_with_password(args.server.args.username, args.password)

    @line_magic
    @help_decorate("[System] logout system", "logout")
    def logout(self, line):
        global system_server_conf
        system_server_conf["access_code"] = ""
        system_server_conf["token"] = ""
        system_server_conf["cookies"] = {}
        logger.info(_l["logout_success"])

    @line_magic
    @help_decorate("[System] change system lang", "lang zh")
    def lang(self, l="en"):
        global _lang, _l
        if not l:
            return
        if i18n.get(l) is None:
            logger.warn("lang {} not support, will use lang {}", l, _lang, "warn", "notice")
            return
        _lang = l
        _l = i18n[_lang]


@help_decorate("Enum, used to describe a job status")
class JobStatus():
    edit = "edit"
    running = "running"
    scheduled = "scheduled"
    paused = "paused"
    stop = 'stop'
    stopping = 'stopping'
    complete = "complete"
    wait_run = "wait_run"
    error = "error"


@help_decorate("Enum, used to describe a connection readable or writeable")
class ConnectionType:
    source = "source"
    target = "target"
    both = "source_and_target"


@help_decorate("Enum, used to describe write mode for a row")
class WriteMode:
    updateOrInsert = "updateOrInsert"
    appendWrite = "appendWrite"


upsert = "updateOrInsert"
update = "updateWrite"


@help_decorate("Enum, used to config job type")
class SyncType:
    initial = "initial_sync"
    cdc = "cdc"
    both = "initial_sync+cdc"


@help_decorate("Enum, used to config action before sync data")
class DropType:
    drop_table = "dropTable"
    remove_data = "removeData"
    keep_data = "keepData"


no_drop = "no_drop"
drop_data = "drop_data"
drop_schema = "drop_schema"


class BaseObj:
    def __init__(self):
        self.source = None
        self.id = str(uuid.uuid4())
        self.func_header = True


class MergeNode(BaseObj):

    def __init__(self,
                 node_id: str,
                 table_name: str,
                 association: Iterable[Sequence[Tuple[str, str]]],
                 mergeType=WriteMode.updateOrInsert,
                 targetPath=""
                 ):
        self.node_id = node_id
        self.table_name = table_name
        self.mergeType = mergeType
        self.targetPath = targetPath
        self.association = association
        self.father = None
        self.child = []
        super(MergeNode, self).__init__()

    def to_dict(self):
        return {
            "id": self.node_id,
            "isArray": False,
            "joinKeys": [{"source": i[0], "target": i[1]} for i in self.association],
            "mergeType": self.mergeType,
            "targetPath": self.targetPath,
            "children": [i.to_dict() for i in self.child],
            "tableName": self.table_name
        }

    def add(self, node):
        if not hasattr(node, 'father'):
            logger.warn("{}", "the node must be the instance of class MergeNode")
            return
        node.father = self
        self.child.append(node)


class Merge(MergeNode):

    def __init__(self,
                 node_id: str,
                 table_name: str,
                 association: Iterable[Sequence[Tuple[str, str]]],
                 mergeType=WriteMode.updateOrInsert,
                 targetPath=""
                 ):
        super(Merge, self).__init__(
            node_id,
            table_name,
            association,
            mergeType,
            targetPath
        )

    def to_dict(self, is_head=False):
        # if the node is head node
        if self.father is None or is_head:
            d = {
                "type": "merge_table_processor",
                "processorThreadNum": 1,
                "name": "主从合并",
                "mergeProperties": [{
                    "children": [i.to_dict() for i in self.child],
                    "id": self.node_id,
                    "isArray": False,
                    "tableName": self.table_name,
                    "mergeType": "updateOrInsert"
                }],
                "id": self.id,
                "elementType": "Node",
                "catalog": "processor",
                "attrs": {
                    "position": [0, 0]
                }
            }
        else:
            d = {
                "id": self.node_id,
                "isArray": False,
                "joinKeys": [{"source": i[0], "target": i[1]} for i in self.association],
                "mergeType": self.mergeType,
                "targetPath": self.targetPath,
                "children": [i.to_dict() for i in self.child],
                "tableName": self.table_name
            }
        return d


class Js(BaseObj):
    def __init__(self, script, func_header=True):
        super().__init__()
        self.language = "js"
        self.script = script
        self.func_header = func_header

    def to_js(self):
        return self.script


class ValueMap(BaseObj):
    def __init__(self, field, value):
        super().__init__()
        self.field = field
        self.value = value

    def to_js(self):
        return '''
    record["%s"] = %s;
        ''' % (self.field, self.value)


class Rename(BaseObj):
    def __init__(self, ori, new):
        super().__init__()
        self.ori = ori
        self.new = new

    def to_js(self):
        return '''
    record["%s"] = record["%s"];
    delete(record["%s"]);
    return record;
        ''' % (self.new, self.ori, self.ori)


class FilterType:
    keep = "keep"
    delete = "delete"


class Filter(BaseObj):
    def __init__(self, f, filterType=FilterType.keep):
        super().__init__()
        self.f = {filterType: f}

    def _add_record_for_str(self, s):
        import re
        s1 = ""
        m1 = re.finditer('\"([^\"]*)\"', s)
        f = 0
        for i in m1:
            s1 = s1 + s[f:i.start()] + '""'
            f = i.end()
        s1 = s1 + s[f:]

        s2 = ""
        m2 = re.finditer(r'[a-zA-Z_][\.a-zA-Z0-9_]*', s1)
        f = 0
        for i in m2:
            s2 = s2 + s1[f:i.start()] + "record." + i.group()
            f = i.end()
        s2 = s2 + s1[f:]

        s3 = ""
        m3l = list(re.finditer('\"([^\"]*)\"', s2))
        f = 0
        m1l = list(re.finditer('\"([^\"]*)\"', s))
        for i in range(len(m3l)):
            s3 = s3 + s2[f:m3l[i].start()] + m1l[i].group()
            f = m3l[i].end()
        s3 = s3 + s2[f:]

        return s3

    def to_js(self):
        keep = self.f.get("keep")
        delete = self.f.get("delete")
        if keep:
            return '''
    if (%s) {
        return record;
    }
    return null;
        ''' % (self._add_record_for_str(keep))
        return '''
    if (%s) {
        return null;
    }
    return record;
        ''' % (self._add_record_for_str(delete))


class ColumnFilter(Filter):
    def __init__(self, f, filterType=FilterType.keep):
        super().__init__(f, filterType)
        self.id = str(uuid.uuid4())
        self.f = {filterType: f}

    def to_js(self):
        keep = self.f.get(FilterType.keep)
        delete = self.f.get(FilterType.delete)
        if keep:
            return '''
        keepFields = %s;
        newRecord = {};
        for (i in keepFields) {
            newRecord[keepFields[i]] = record[keepFields[i]];
        }
        return newRecord;
        ''' % (str(keep))

        return '''
    deleteFields = %s;
    newRecord = record;
    for (i in deleteFields) {
        delete(newRecord[deleteFields[i]]);
    }
    return newRecord;
}
    ''' % (str(delete))


class JobType:
    migrate = "migrate"
    sync = "sync"


class JobStats:

    qps = 0
    total = 0
    input_insert = 0
    input_update = 0
    input_delete = 0
    output_insert = 0
    output_update = 0
    output_Delete = 0


class LogMinerMode:
    manually = "manually"
    automatically = "automatically"


@help_decorate("use to define a stream pipeline", "p = new Pipeline($name).readFrom($source).writeTo($sink)")
class Pipeline:
    @help_decorate("__init__ method", args="p = Pipeline($name)")
    def __init__(self, name=None, mode="migrate"):
        if name is None:
            name = str(uuid.uuid4())
        self.dag = Dag(name="name")
        self.dag.config({})
        self.dag.jobType = mode
        self.stage = None
        self.job = None
        self.check_job = None
        self.name = name
        self.mergeNode = None
        self.sources = []
        self.sinks = []
        self.validateConfig = None
        self.get()
        self.cache_sinks = {}

    def mode(self, value):
        self.dag.jobType = value

    @help_decorate("read data from source", args="p.readFrom($source)")
    def readFrom(self, source):
        if isinstance(source, QuickDataSourceMigrateJob):
            source = source.__db__
            source = Source(source)
        elif isinstance(source, str):
            if "." in source:
                db, table = source.split(".")
                source = Source(db, table, mode=self.dag.jobType)
            else:
                source = Source(source, mode=self.dag.jobType)
        self.sources.append(source)
        return self._clone(source)

    @help_decorate("write data to sink", args="p.writeTo($sink, $relation)")
    def writeTo(self, sink):
        if isinstance(sink, QuickDataSourceMigrateJob):
            sink = sink.__db__
            sink = Sink(sink)
        elif isinstance(sink, str):
            if "." in sink:
                db, table = sink.split(".")
                sink = Sink(db, table, mode=self.dag.jobType)
            else:
                sink = Sink(sink, mode=self.dag.jobType)

        if self.dag.jobType == JobType.sync:
            primary_key = self.sources[-1].primary_key
            sink.config({
                "updateConditionFields": primary_key,
            })

        self.dag.edge(self.stage, sink)
        self.sinks.append({"sink": sink})
        return self._clone(sink)

    def _common_stage(self, f):
        self.dag.edge(self.stage, f)
        return self._clone(f)

    def _common_stage2(self, p, f):
        if isinstance(p.stage, MergeNode):
            # delete the p.stage from p.dag
            nodes = []
            for i in self.dag.dag["nodes"]:
                if i["id"] != p.stage.id:
                    nodes.append(i)
            self.dag.dag["nodes"] = nodes
            for i in self.dag.dag["edges"]:
                if i["target"] == p.stage.id:
                    i["target"] = f.id
            self.dag.edge(self.stage, f)
        else:
            self.dag.edge(self.stage, f)
            self.dag.edge(p.stage, f)
        return self._clone(f)

    @help_decorate("using simple query filter data", args='p.filter("id > 2 and sex=male")')
    def filter(self, query="", filterType=FilterType.keep):
        if self.dag.jobType == JobType.migrate:
            logger.warn("{}", "migrate job not support filter processor")
            return self
        f = Filter(query, filterType)
        return self._common_stage(f)

    @help_decorate("filter column", args='p.filterColumn(["id", "name"], FilterType.keep)')
    def filterColumn(self, query=[], filterType=FilterType.keep):
        if self.dag.jobType == JobType.migrate:
            logger.warn("{}", "migrate job not support filterColumn processor")
            return self
        f = ColumnFilter(query, filterType)
        return self._common_stage(f)

    def typeMap(self, field, t):
        return self

    def valueMap(self, field, value):
        f = ValueMap(field, value)
        return self._common_stage(f)

    @help_decorate("rename a record key", args="p.rename($old_key, $new_key)")
    def rename(self, ori, new):
        if self.dag.jobType == JobType.migrate:
            logger.warn("{}", "migrate job not support rename processor")
            return self
        f = Rename(ori, new)
        return self._common_stage(f)

    @help_decorate("use a function(js text/python function) transform data", args="p.js()")
    def js(self, script=""):
        if self.dag.jobType == JobType.migrate:
            logger.warn("{}", "migrate job not support js processor")
            return self
        import types
        if type(script) == types.FunctionType:
            from metapensiero.pj.api import translates
            import inspect
            source_code = inspect.getsource(script)
            source_code = "def process(" + source_code.split("(", 2)[1]
            js_script = translates(source_code)[0]
            f = Js(js_script, False)
        else:
            if script.endswith(".js"):
                js_script = open(script, "r").read()
                script = js_script
            f = Js(script)
        return self._common_stage(f)

    @help_decorate("merge another pipeline", args="p.merge($pipeline)")
    def merge(self, pipeline, association: Iterable[Sequence[Tuple[str, str]]] = None, mergeType="updateWrite",
              targetPath=""):
        if not isinstance(pipeline, Pipeline):
            logger.warn("{}", "pipeline must be the instance of class Pipeline")
            return
        if not isinstance(association, Iterable) and association is not None:
            logger.warn("{}", "association error, it can be like this: [('id', 'id')]")
            return
        if self.dag.jobType == JobType.migrate:
            logger.warn("{}", "migrate job not support merge")
            return
        parent_id = self.sources[len(self.sources) - 1].id
        parent_table_name = self.sources[len(self.sources) - 1].tableName
        if self.mergeNode is None:
            self.mergeNode = Merge(
                parent_id, parent_table_name, association=[], mergeType=mergeType, targetPath=targetPath
            )
        if pipeline.mergeNode is None:
            child_id = pipeline.sources[len(pipeline.sources) - 1].id
            child_table_name = pipeline.sources[len(pipeline.sources) - 1].tableName
            pipeline.mergeNode = Merge(
                child_id,
                child_table_name,
                association=[] if association is None else association,
                mergeType=mergeType,
                targetPath=targetPath
            )
        self.mergeNode.add(pipeline.mergeNode)
        return self._common_stage2(pipeline, self.mergeNode)

    @help_decorate("use a function(js text/python function) transform data", args="p.processor()")
    def processor(self, script=""):
        return self.js(script)

    def agg(self, name="agg", method="", key="", groupKeys=[], pk=[], ttl=3600):
        f = Agg(name, method, key, groupKeys, pk, ttl)

        return self._common_stage(f)

    def get(self):
        job = Job(name=self.name)
        if job.id is not None:
            self.job = job

    def _get_source_connection_id(self):
        source_connection_ids = []
        if self.job is None:
            stages = self.dag.stages
        else:
            stages = self.job["stages"]
        for stage in stages:
            if len(stage.get("inputLanes", [])) == 0 and len(stage.get("outputLanes", [])) > 0:
                source_connection_ids.append(stage["connectionId"])
        return source_connection_ids

    def enableLatencyMeasure(self):
        return self.accurateDelay()

    def accurateDelay(self):
        source = self.sources[0]
        sink = self.sinks[0]
        fields = get_table_fields(source.tableName, whole=True, source=source.connectionId)
        self.validateConfig = {
            "flowId": "",
            "name": "",
            "mode": "cron",
            "inspectMethod": "",
            "enabled": True,
            "status": "",
            "limit": {"keep": 100},
            "platformInfo": {"agentType": "private"},
            "timing": {
                "start": int(time.time()) * 1000,
                "end": int(time.time()) * 1000 + 86400000 * 365 * 10,
                "intervals": 1440,
                "intervalsUnit": "minute"
            },
            "tasks": [{
                "fullMatch": True,
                "jsEngineName": "graal.js",
                "script": "",
                "showAdvancedVerification": False,
                "source": {
                    "connectionId": source.connectionId,
                    "databaseType": source.databaseType,
                    "fields": fields,
                    "sortColumn": sink["relation"].association[0][0],
                    "table": source.tableName
                },
                "target": {
                    "connectionId": sink["sink"].connectionId,
                    "databaseType": sink["sink"].databaseType,
                    "fields": fields,
                    "sortColumn": sink["relation"].association[0][0],
                    "table": sink["sink"].tableName
                }
            }]
        }
        return self.config({"accurate_delay": True})

    @help_decorate("config pipeline", args="config map, please h pipeline_config get all config key and it's meaning")
    def config(self, config: dict = None, keep_extra=True):

        if not isinstance(config, dict):
            logger.warn("type {} must be {}", config, "dict", "notice", "notice")
            return
        mode = self.dag.jobType
        self.dag.config(config)
        resp = ConfigCheck(self.dag.setting, job_config[mode], keep_extra=keep_extra).checked_config
        self.dag.config(resp)
        return self

    def readLogFrom(self, logMiner):
        return self

    def _clone(self, stage):
        p = Pipeline()
        p.dag = self.dag
        self.stage = stage
        p.stage = self.stage
        p.job = self.job
        p.check_job = self.check_job
        p.sources = copy.copy(self.sources)
        p.sinks = copy.copy(self.sinks)
        p.name = self.name
        p.cache_sinks = self.cache_sinks
        p.mergeNode = self.mergeNode
        return p

    def cache(self, ttl):
        return self

    @help_decorate("config cdc time", args='p.config_cdc_start_time()')
    def config_cdc_start_time(self, start_time, tz="+8"):
        source_connection_ids = self._get_source_connection_id()
        config = self.dag.config()
        syncPoints = []
        tz = "+08:00"
        t = "localTZ"
        if start_time is None or start_time == "":
            t = "current"
        for i in range(len(source_connection_ids)):
            syncPoints.append({
                "dateTime": start_time,
                "timezone": tz,
                "pointType": t,
                "connectionId": source_connection_ids[i]
            })
        config["syncPoints"] = [syncPoints]
        config["sync_type"] = "cdc"
        self.config(config)

    @help_decorate("start this pipeline as a running job", args="p.start()")
    def start(self):
        if self.job is not None:
            self.job.config(self.dag.setting)
            self.job.start()
            return self
        job = Job(name=self.name, pipeline=self)
        job.validateConfig = self.validateConfig
        self.job = job
        self.config({})
        job.config(self.dag.setting)
        if job.start():
            logger.info("job {} start running ...", self.name)
        else:
            logger.warn("job {} start failed!", self.name)
        return self

    @help_decorate("stop this pipeline job", args="p.stop()")
    def stop(self):
        if self.job is None:
            logger.warn("pipeline {} not start, can not stop", self.name)
            return self
        self.job.stop()
        return self

    @help_decorate("delete this pipeline job", args="p.delete()")
    def delete(self):
        if self.job is None:
            logger.warn("pipeline {} not exists, can not delete", self.name)
            return self
        self.job.delete()
        return self

    @help_decorate("get pipeline job status", args="p.status()")
    def status(self):
        if self.job is None:
            logger.warn("pipeline not start, no status can show")
            return self
        status = self.job.status()
        logger.info("job {} status is: {}", self.name, status)
        return status

    def wait_status(self, status, t=30):
        if self.job is None:
            logger.warn("pipeline not start, no status can show")
            return self
        s = time.time()
        while True:
            if self.job.status() == status:
                time.sleep(10)
                return True
            time.sleep(1)
            if time.time() - s > t:
                break
        return False

    @help_decorate("get pipeline job stats", args="p.stats()")
    def stats(self, quiet=True):
        self.monitor(t=2)
        return self

    @help_decorate("monitor pipeline job until it stoppped or timeout", args="p.monitor(10)")
    def monitor(self, t=30):
        if self.job is None:
            logger.warn("pipeline not start, no monitor can show")
            return
        self.job.monitor(t)
        return self

    def check(self, count=10):
        if self.status() not in [JobStatus.running, JobStatus.stop, JobStatus.complete]:
            logger.warn(
                "{}", "The status of this task is not in [running, stop, complete], unable to check data."
            )
            return
        if not self.dag.setting.get("isAutoInspect"):
            logger.warn("please set {} to enable auto inspect", "$pipeline.config({'isAutoInspect': True})", "info")
            return
        for _ in range(count):
            time.sleep(1)

            data = InspectApi().post({"id": self.job.id})
            if not data:
                pass
            data = data["data"]
            diff_record = data.get("diffRecords", 0)
            diff_tables = data.get("diffTables", 0)
            totals = data.get("totals", 0)
            ignore = data.get("ignore", 0)

            logger.log(
                "data check start, total is {}, ignore row number is {}, diff row number is {}, diff table number is {}",
                totals, ignore, diff_record, diff_tables,
                "info", "info", "warn", "warn",
            )
            if self.status() in [JobStatus.stop, JobStatus.complete, JobStatus.error]:
                break


class Agg(BaseObj):
    def __init__(self, name, method, key, groupKeys, pk=[], ttl=3600):
        super().__init__()
        self.name = name
        self.method = method
        self.key = key
        self.groupKeys = groupKeys
        self.pk = pk
        self.ttl = ttl


class BaseNode:

    @help_decorate("__init__ method", args="connection, table, sql")
    def __init__(self, connection, table=None, table_re=None, mode=JobType.migrate):
        self.mode = mode
        self.id = str(uuid.uuid4())
        self.connection, self.table = self._get_connection_and_table(connection, table, table_re)
        self.tableName = self.table
        self.connectionId = str(self.connection.c["id"])
        self.databaseType = self.connection.c["database_type"]
        self.config_type = node_config if mode == JobType.migrate else node_config_sync
        client_cache["connection"] = self.connectionId

        self.source = None
        self.setting = {
            "connectionId": self.connectionId,
            "databaseType": self.databaseType,
            "id": self.id,
            "name": self.connection.c["name"],
            "attrs": {
                "connectionType": self.connection.c["connection_type"],
                "position": [0, 0],
                "pdkType": "pdk",
                "pdkHash": self.connection.c["pdkHash"],
                "capabilities": self.connection.c["capabilities"],
                "connectionName": self.connection.c["name"]
            },
            "type": "database" if self.mode == "migrate" else "table"
        }

    def _get_connection_and_table(self, connection, table, table_re):
        global client_cache
        if isinstance(connection, QuickDataSourceMigrateJob):
            connection = connection.__db__
        if client_cache.get("connections") is None:
            show_connections(quiet=True)

        # get connection
        if not isinstance(connection, Connection):
            index_type = get_index_type(connection)
            if index_type == "short_id_index":
                connection = match_line(client_cache["connections"]["id_index"], connection)
                index_type = "id_index"
            if index_type == "name_index" and "." in connection:
                connection_and_table = connection.split(".")
                connection = connection_and_table[0]
                table = connection_and_table[1]
            c = client_cache["connections"][index_type][connection]
            connection = Connection(id=c["id"])

        # select all tables default if table not provide (only migrate mode)
        if table is None and self.mode == JobType.migrate:
            if c["id"] not in client_cache["tables"]:
                show_tables(source=connection.id, quiet=True)
            table = list(client_cache["tables"][c["id"]]["name_index"].keys())
        # select first table if table not provide (only sync mode)
        if table is None and self.mode == JobType.sync:
            if c["id"] not in client_cache["tables"]:
                show_tables(source=connection.id, quiet=True)
            try:
                table = list(client_cache["tables"][c["id"]]["name_index"].keys())[0]
            except IndexError:
                logger.error("Source {} no table", c["name"])
                return None, None

        # filter table_re if table_re provide
        if table_re is not None and self.mode == JobType.migrate:
            tables = []
            all_tables = show_tables(source=connection.id, quiet=True)
            import re
            for t in all_tables:
                if "original_name" not in t:
                    continue
                if re.match(table_re, t["original_name"]):
                    tables.append(t["original_name"])
            table = tables
        return connection, table

    def to_dict(self):
        self.config({})
        return self.setting

    def config(self, config: dict = None, keep_extra=True):
        if not isinstance(config, dict):
            logger.warn("type {} must be {}", config, "dict", "notice", "notice")
            return False
        self.setting.update(config)
        resp = ConfigCheck(self.setting, self.config_type[type(self).__name__.lower()],
                           keep_extra=keep_extra).checked_config
        self.setting.update(resp)
        return True

    def test(self):
        self.connection.test()

    def _getTableId(self, tableName):
        payload = {
            "where": {
                "source.id": self.connectionId,
                "meta_type": {"in": ["collection", "table", "view"]},
                "is_deleted": False,
                "original_name": tableName
            },
            "fields": {"id": True, "original_name": True, "fields": True},
            "limit": 1
        }
        res = req.get("/MetadataInstances", params={"filter": json.dumps(payload)}).json()
        table_id = None
        for s in res["data"]["items"]:
            if s["original_name"] == tableName:
                table_id = s["id"]
                break
        if table_id is not None:
            self.primary_key = []
            res = req.get("/MetadataInstances/" + table_id).json()
            for field in res["data"]["fields"]:
                if field.get("primaryKey", False):
                    self.primary_key.append(field["field_name"])
        return table_id

    @help_decorate("get cache job status")
    def cache_status(self):
        self.cache_p.status()


@help_decorate("source is start of a pipeline", "source = Source($Datasource, $table)")
class Source(BaseNode):

    def __init__(self, connection, table=None, mode="migrate"):
        super().__init__(connection, table, mode=mode)
        if self.mode == "migrate":
            self.config_type = node_config
            self.setting.update({
                "tableNames": self.table
            })
        else:
            self.config_type = node_config_sync
            _ = self._getTableId(table)  # to set self.primary_key, don't delete this line
            self.setting.update({
                "tableName": table,
            })


@help_decorate("sink is end of a pipeline", "sink = Sink($Datasource, $table)")
class Sink(Source):
    def __init__(self, connection, table=None, mode="migrate"):
        super().__init__(connection, table, mode=mode)
        if self.mode == JobType.sync:
            self.config_type = node_config_sync
            _ = self._getTableId(table)  # to set self.primary_key, don't delete this line
            self.setting.update({
                "tableName": table,
            })


class Api:
    def __init__(self, id=None, name=None, table=None):
        if id is not None and name is None:
            name = id
        self.id = None
        self.name = name

        if name is None:
            return
        else:
            if self.get(name):
                table = f"{self.db}.{self.tablename}"

        if table is None:
            return

        base_path = name

        global client_cache
        client_cache["apis"]["name_index"] = {}

        db = table.split(".")[0]
        table2 = table.split(".")[1]
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        if db not in client_cache["connections"]["name_index"]:
            show_connections(quiet=True)
        if db not in client_cache["connections"]["name_index"]:
            logger.warn("no Datasource {} found in system", db)
            return
        db = client_cache["connections"]["name_index"][db]

        fields = get_table_fields(table2, whole=True, source=db["id"])
        for index, field in enumerate(fields):
            field["comment"] = ""
            fields[index] = field
        self.base_path = base_path
        self.tablename = table2
        self.payload = {
            "apiType": "defaultApi",
            "apiVersion": "",
            "basePath": base_path,
            "connectionId": db["id"],
            "connectionName": db["name"],
            "connectionType": db["database_type"],
            "datasource": db["id"],
            "fields": fields,
            "listtags": [],
            "name": name,
            "operationType": "GET",
            "prefix": "",
            "readConcern": "",
            "readPreference": "",
            "readPreferenceTag": "",
            "tableName": table2,
            "tablename": table2,
            "status": "generating",
            "paths": [{
                "acl": [
                    "admin"
                ],
                "fields": fields,
                "description": "Get records by page",
                "method": "POST",
                "name": "findPage",
                "params": [
                    {
                        "defaultvalue": 1,
                        "description": "page number",
                        "name": "page",
                        "type": "number",
                        "require": True,
                    },
                    {
                        "defaultvalue": 20,
                        "description": "max records per page",
                        "name": "limit",
                        "type": "number",
                        "require": True,
                    },
                    {
                        "description": "sort setting,Array ,format like [{'propertyName':'ASC'}]",
                        "name": "sort",
                        "type": "object"
                    },
                    {
                        "description": "search filter object,Array",
                        "name": "filter",
                        "type": "object"
                    }
                ],
                "path": f"/api/{base_path}",
                "result": "Page<Document>",
                "type": "preset",
                "sort": [],
                "where": [],
            }]
        }

    def publish(self):
        if self.id is None:
            res = req.post("/Modules", json=self.payload).json()  # save
            id = res["data"]["id"]
            payload = copy.deepcopy(self.payload)
            payload.update({
                "id": res["data"]["id"],
                "status": "pending"
            })
            res = req.patch("/Modules", json=payload).json()["data"]
            res = req.patch("/Modules", json={
                "id": res["id"],
                "status": "active",
                "tableName": res["tableName"],
            }).json()  # publish
            if res["code"] == "ok":
                logger.info("publish api {} success, you can test it by: {}", self.base_path,
                            "http://" + server + "#/apiDocAndTest?id=" + self.base_path + "_v1")
                self.id = res["data"]["id"]
            else:
                logger.warn("publish api {} fail, err is: {}", self.base_path, res["message"])
        else:
            payload = {
                "id": self.id,
                "status": "active",
                "tableName": self.tablename,
            }
            res = req.patch("/Modules", json=payload)
            res = res.json()
            if res["code"] == "ok":
                logger.info("publish {} success", self.name)
            else:
                logger.warn("publish {} fail, err is: {}", self.name, res["message"])

    def get(self, name):
        global client_cache
        if len(client_cache.get("apis", {}).get("name_index")) == 0:
            show_apis(quiet=True)
        api = client_cache["apis"]["name_index"].get(name)
        if api is None:
            return False
        api_id = api["id"]
        self.id = api_id
        self.db = api["database"]
        self.tablename = api["tableName"]
        return True

    def status(self, name):
        res = req.get("/Modules")
        data = res.json()["data"]["items"]
        for i in data:
            if i["name"] == name:
                return i["status"]
        return None

    def unpublish(self):
        if self.id is None:
            return
        payload = {
            "id": self.id,
            "status": "pending",
            "tableName": self.tablename,
        }
        res = req.patch("/Modules", json=payload)
        res = res.json()
        if res["code"] == "ok":
            logger.info("unpublish {} success", self.id)
        else:
            logger.warn("unpublish {} fail, err is: {}", self.id, res["message"])

    def delete(self):
        if self.id is None:
            logger.warn("delete api {} fail, err is: {}", self.name, "api not find")
            return
        res = req.delete("/Modules/" + self.id)
        res = res.json()
        if res["code"] == "ok":
            logger.info("delete api {} success", self.name)
        else:
            logger.warn("delete api {} fail, err is: {}", self.name, res["message"])


class ApiServer:
    def __init__(self, id=None, name=None, uri=None):
        logger.warn("This feature is expected to be abandoned in the future")
        self.id = id
        self.name = name
        self.uri = uri
        self.processId = str(uuid.uuid4())

    def save(self):
        url = "/ApiServers"
        # update
        item = False
        if self.id:
            item = self.get()
        if self.id and item:
            if item.get("createTime"):
                del item["createTime"]
            if self.name is not None:
                item.update({"clientName": self.name})
            if self.uri is not None:
                item.update({"clientURI": self.uri})
            res = req.patch(url, json=item)
            if res.json()["code"] == "ok":
                return res.json()["data"]
            else:
                return False
        # create
        else:
            if not self.processId or not self.name or not self.uri:
                logger.error("no attribute: {} not found", 'processId or name or uri')
                return False
            payload = {
                "processId": self.processId,
                "clientName": self.name,
                "clientURI": self.uri,
            }
            res = req.post(url, json=payload)
            if res.json()["code"] == "ok":
                return res.json()["data"]
            else:
                return False

    def delete(self):
        if not isinstance(self.id, str) and not self.id:
            logger.error("id must be set")
            return False
        url = "/ApiServers/" + self.id
        res = req.delete(url)
        if res.json()["code"] == "ok":
            return True
        else:
            return False

    def get(self):
        items = self.list()
        for item in items:
            if item["id"] == self.id:
                return item
        return False

    @classmethod
    def list(cls):
        url = "/ApiServers"
        res = req.get(url, params={"filter": '{"order":"clientName DESC","skip":0,"where":{}}'})
        if res.json()["code"] == "ok":
            return res.json()["data"]["items"]
        else:
            return False


class Job:
    def __init__(self, name=None, id=None, dag=None, pipeline=None):
        self.id = None
        self.setting = {}
        self.job = {}
        self.validateConfig = None
        if id is not None:
            if len(id) == 24:
                self.id = id
                self._get()
                return
            else:
                obj = get_obj("job", id)
                self.id = obj.id
                self._get()
                return

        self.name = name
        self._get()
        if dag is None and pipeline is None:
            return
        if dag is not None:
            self.dag = dag
        else:
            self.dag = pipeline.dag

        self.id = None
        self.cost = 0

    @staticmethod
    def list():
        res = req.get(
            "/Task",
            params={"filter": '{"fields":{"id":true,"name":true,"status":true,"agentId":true,"stats":true}}'}
        )
        if res.status_code != 200:
            return None
        res = res.json()
        jobs = []
        for i in res["data"]['items']:
            jobs.append(Job(id=i["id"]))
        return jobs

    def reset(self):
        res = req.patch("/Task/batchRenew", params={"taskIds": self.id}).json()
        if res["code"] == "ok":
            return True
        return False

    def _get_by_name(self):
        param = '{"where":{"name":{"like":"%s"}}}' % (self.name)
        res = req.get("/Task", params={'filter': param})
        if res.status_code != 200:
            return None
        res = res.json()
        for j in res["data"]["items"]:
            if "name" not in j:
                continue
            if j["name"] == self.name:
                self.id = j["id"]
                self.job = j
                return

    def _get(self):
        if self.id is not None:
            res = req.get("/Task/findTaskDetailById/" + self.id)
            if res.status_code != 200:
                return None
            res = res.json()
            self.name = res["data"]["name"]
            self._get_by_name()
            return

        if self.name is not None:
            self._get_by_name()

    def stop(self, t=30):
        if self.id is None:
            return False
        res = req.put('/Task/batchStop', params={'taskIds': self.id})
        s = time.time()
        while True:
            if time.time() - s > t:
                return False
            time.sleep(1)
            status = self.status()
            print(status)
            if status == JobStatus.stop or status == JobStatus.stopping:
                return True
        return False

    def delete(self):
        if self.id is None:
            return False
        if self.status() in [JobStatus.running, JobStatus.scheduled]:
            logger.warn("job status is {}, please stop it first before delete it", self.status())
            return
        res = req.delete("/Task/batchDelete", params={"taskIds": self.id})
        if res.status_code != 200:
            return False
        res = res.json()
        if res["code"] != "ok":
            return False
        return True

    def save(self):
        if self.id is None:
            self.job = {
                "editVersion": int(time.time() * 1000),
                "syncType": self.dag.jobType,
                "mappingTemplate": self.dag.jobType,
                "name": self.name,
                "status": JobStatus.edit,
                "dag": self.dag.dag,
                "user_id": system_server_conf["user_id"],
                "customId": system_server_conf["user_id"],
                "createUser": system_server_conf["username"]
            }
            if self.validateConfig is not None:
                self.job["validateConfig"] = self.validateConfig
        self.job.update(self.setting)
        print(json.dumps(self.job, indent=4))
        res = req.patch("/Task", json=self.job)
        res = res.json()
        if res["code"] != "ok":
            logger.warn("save failed {}", res)
            return False
        self.id = res["data"]["id"]
        job = res["data"]
        job.update(self.setting)
        res = req.patch("/Task/confirm/" + self.id, json=job)
        res = res.json()
        if res["code"] != "ok":
            logger.warn("start failed {}", res)
            return False
        self.job = res["data"]
        self.setting = res["data"]
        return True

    def start(self):
        try:
            status = self.status()
        except (KeyError, TypeError) as e:
            logger.info("job is not save, it will be save soon")
            resp = self.save()
            if not resp:
                logger.warn("job {} save failed.", self.name)
                return False
            status = self.status()
        if status in [JobStatus.running, JobStatus.scheduled, JobStatus.wait_run]:
            logger.info("job {} status is {} now", self.id, status)
            return True

        if self.id is None:
            logger.warn("save job fail")
            return False
        res = req.put("/Task/batchStart", params={"taskIds": self.id}).json()
        if res["code"] != "ok":
            return False

        return True

    def config(self, config):
        self.setting.update(config)

    def status(self, res=None, quiet=True):
        if res is None:
            res = req.get("/Task/" + self.id).json()
        status = res["data"]["status"]
        if not quiet:
            logger.info("job status is: {}", status)
        return status

    def get_sub_task_ids(self):
        sub_task_ids = []
        res = req.get("/Task/" + self.id).json()
        statuses = res["data"]["statuses"]
        jobStats = JobStats()
        for subTask in statuses:
            sub_task_ids.append(subTask["id"])
        return sub_task_ids

    def stats(self, res=None):
        data = TaskApi().get(self.id)["data"]
        payload = {
            "startAt": int(time.time() * 1000),
            "endAt": int(time.time() * 1000),
            "samples": {
                "totalData": {
                    "endAt": int(time.time() * 1000),
                    "fields": [
                        "inputInsertTotal", "inputUpdateTotal", "inputDeleteTotal",
                        "outputInsertTotal", "outputUpdateTotal", "outputDeleteTotal",
                        "tableTotal", "outputQps"
                    ],
                    "tags": {
                        "taskId": self.id,
                        "taskRecordId": data["taskRecordId"],
                        "type": "task"
                    },
                    "type": "instant",
                }
            }
        }
        res = req.post("/measurement/query/v2", json=payload).json()
        job_stats = JobStats()
        if len(res["data"]["samples"]["totalData"]) > 0:
            stats = res["data"]["samples"]["totalData"][0]
            job_stats.qps = stats["outputQps"]
            job_stats.total = stats["tableTotal"]
            job_stats.input_insert = stats["inputInsertTotal"]
            job_stats.input_update = stats["inputUpdateTotal"]
            job_stats.input_delete = stats["inputDeleteTotal"]
            job_stats.output_insert = stats["outputInsertTotal"]
            job_stats.output_update = stats["outputUpdateTotal"]
            job_stats.output_Delete = stats["outputDeleteTotal"]
        return job_stats

    def logs(self, res=None, limit=100, level="info", t=30, tail=False, quiet=True):
        logs = []
        log_ids = {}
        start_time = time.time()

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                sub_task_ids = self.get_sub_task_ids()
                for sub_task_id in sub_task_ids:
                    payload = {
                        "type": "logs",
                        "filter": {
                            "limit": limit,
                            "order": "id asc",
                            "where": {
                                "contextMap.dataFlowId": {
                                    "eq": sub_task_id,
                                }
                            }
                        }
                    }
                    while True:
                        await websocket.send(json.dumps(payload))
                        while True:
                            recv = await asyncio.wait_for(websocket.recv(), timeout=t)
                            if time.time() - start_time > t:
                                break
                            result = json.loads(recv)
                            if result["type"] != "logs":
                                continue
                            for i in result["data"]:
                                if "id" not in i:
                                    print(i)
                                    continue
                                if i["id"] not in log_ids:
                                    if get_log_level(i["level"]) >= get_log_level(level) and not quiet:
                                        logger.log("[{}] {} {}: {}", i["level"],
                                                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i["date"])),
                                                   i["loggerName"], i["message"],
                                                   i["level"].lower(), "info", "info", "debug")
                                    log_ids[i["id"]] = 1
                            if not tail:
                                break
                            time.sleep(1)
                        await websocket.close()
                        return logs

        try:
            asyncio.get_event_loop().run_until_complete(l())
        except Exception as e:
            pass

    def wait(self, print_log=False, t=600):
        start_time = time.time()
        while True:
            if time.time() - start_time > t:
                break
            time.sleep(1)
            stats = self.stats()
            status = self.status()
            if print_log:
                logger.info(
                    "job {} status: {}, qps: {}, total: {} input_stats: insert: {}, update: {}, delete: {} output_stats: insert: {}, update: {}, delete: {}",
                    self.name, status, stats.qps, stats.total, stats.input_insert, stats.input_update, stats.input_delete,
                    stats.output_insert, stats.output_update, stats.output_Delete,
                    "info", "info", "notice", "info", "info", "info", "info", "info", "info", "info", wrap=False, logger_header=True
                )
            if status in [JobStatus.running, JobStatus.edit, JobStatus.scheduled]:
                continue
            logger.info(
                "job {} status: {}, qps: {}, total: {}\n" + \
                "input_stats: insert: {}, update: {}, delete: {}" + \
                "output_stats: insert: {}, update: {}, delete: {}",
                self.name, status, stats["outputQps"], stats["tableTotal"],
                stats["inputInsertTotal"], stats["inputUpdateTotal"], stats["inputDeleteTotal"],
                stats["outputInsertTotal"], stats["outputUpdateTotal"], stats["outputDeleteTotal"],
                "info", "info", "notice", "info", "info", "info", "info", "info", "info", "info", wrap=False, logger_header=True
            )
            break

    def monitor(self, t=30, quiet=False):
        self.wait(print_log=True, t=t)

    def check(self):
        pass

    def desc(self):
        if self.job["syncType"] not in ["migrate", "sync"]:
            logger.warn("syncType {} not support in this version", self.job["syncType"])
            return

        job_info = {
            # "id": self.job["id"],
            "name": self.job["name"],
            "syncType": self.job["syncType"],
            "createTime": self.job["createTime"],
        }
        logger.info("")
        logger.notice("{}", "-" * 120)
        logger.info("{}", "job info")
        print(json.dumps(job_info, indent=4))

        g = Graph()
        node_map = {}  # {node.id: node config}
        attrs_get = {
            "migrate": ["tableNames", "syncObjects", "writeStrategy"],
            "sync": [
                "processorThreadNum", "script", "updateConditionFields", "expression",
                "joinType", "joinExpressions", "leftNodeId", "rightNodeId", "mergeProperties",
                "scripts", "operations", "operations", "deleteAllFields"
            ]
        }

        for n in self.job["dag"]["nodes"]:
            config = {
                "id": n.get("id"),
                "name": n.get("name"),
                "type": n.get("type"),
                "databaseType": n.get("databaseType"),
            }
            if self.job["syncType"] == "migrate":
                for attr in attrs_get["migrate"]:
                    if n.get(attr):
                        config.update({attr: n.get(attr)})
            elif self.job["syncType"] == "sync":
                for attr in attrs_get["sync"]:
                    if n.get(attr):
                        config.update({attr: n.get(attr)})

            node = Node(n.get("id"), n.get("name"), config=config)
            node_map.update({n.get("id"): config})
            g.addVertex(node)

        for n in self.job["dag"]["edges"]:
            g.addEdgeById(n["source"], n["target"])

        logger.info("")
        logger.notice("{}", "-" * 120)
        logger.info("{}", "node relationship of job")
        for s in g.to_relation():
            logger.info(s)

        for node_id, config in node_map.items():
            logger.info("")
            logger.notice("{}", "-" * 120)
            logger.info("{} {}", "configuration of node id", node_id[-6:])
            print(json.dumps(config, indent=4))


@help_decorate("Data Source, you can see it as database",
               'ds = DataSource("mysql", "mysql-datasource").host("127.0.0.1").port(3306).username().password().db()')
class DataSource:

    def __init__(self, connector="", name=None, type="source_and_target", id=None):
        """
        @param connector: pdkType name
        @param name: datasource name
        @param type: datasource can be used as source and target at the same time
        @param id: datasource id, it will get datasource config from backend by api if id provide
        """
        self.pdk_setting = {}
        self.setting = {}
        # get datasource config
        if id is not None:
            self.id = id
            self.setting = self.get(connector_id=id)
            return
        # name is not provide
        name = connector if connector != "" and name is None else name
        # if name is already exists
        obj = get_obj("datasource", name)
        if obj is not None:
            self.id = obj.id
            self.setting = obj.setting
            return
        self.id = id
        self.connection_type = type
        self.setting = {
            "name": name,
            "database_type": client_cache["connectors"][connector]["name"],
            "connection_type": self.connection_type
        }

    def _set_pdk_setting(self, key):
        """closure function to update value into pdk_setting, only be called by __getattribute__
        """

        def _config_pdk_setting(value):
            if key == "uri" and value:
                self.pdk_setting.update({"isUri": True, key: value})
            else:
                self.pdk_setting.update({key: value})
            return self

        return _config_pdk_setting

    def set(self, config):
        self.setting.update(config)
        return self

    def __getattribute__(self, item):
        """
        1. if item is attribute of self, return
        2. if not, return _config_pdk_setting to set pdk_setting
        """

        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            return self._set_pdk_setting(item)

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
        self.setting.update({"pdkHash": self._get_pdkHash()})
        res = ConfigCheck(self.setting, DATASOURCE_CONFIG, keep_extra=True).checked_config
        self.setting.update(res)
        self.setting.update({"config": self.to_pdk_dict()})
        return self.setting

    def _get_pdkHash(self):
        connector = client_cache["connectors"][self.setting["database_type"].lower()]
        return connector["pdkHash"]

    def to_pdk_dict(self):
        """
        1. get datasource config by connector
        2. check the settings by ConfigCheck
        """
        mode = "uri" if self.pdk_setting.get("isUri") else "form"
        # get database_config
        database_type = self.setting["database_type"].lower()
        if pdk_config.get(database_type):
            param_config = pdk_config.get(database_type)[mode]
            res = ConfigCheck(self.pdk_setting, param_config, keep_extra=True).checked_config
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
        data = DataSourceApi().list(params=params)
        if not data:
            return None
        if len(data["data"]["items"]) == 0:
            return None
        return data["data"]["items"][0]

    def save(self):
        data = self.to_dict()
        data = DataSourceApi().post(data)
        show_connections(quiet=True)
        if data["code"] == "ok":
            self.id = data["data"]["id"]
            self.setting = DataSource.get(self.id)
            self.validate(quiet=False)
            return True
        else:
            logger.warn("save Connection fail, err is: {}", data["message"])
        return False

    def delete(self):
        if self.id is None:
            return
        data = DataSourceApi().delete(self.id, self.setting)
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


class Connection:
    @help_decorate("__init__ method",
                   args="id or connection, connection can be a dict, or Object has a to_dict() method")
    def __init__(self, id=None, connection=None):
        if id is None and connection is None:
            return
        if id is not None:
            self.id = id
        else:
            self.id = connection.id
        self.c = Connection.get(id=id)
        if self.c is None:
            if type(connection) != type({}):
                try:
                    self.c = connection.to_dict()
                    self.id = self.c["id"]
                except Exception as e:
                    return
            else:
                self.c = connection

    @help_decorate("save a connection in idaas system")
    def save(self):
        # self.load_schema(quiet=False)
        res = req.post("/Connections", json=self.c)
        show_connections(quiet=True)
        if res.status_code == 200 and res.json()["code"] == "ok":
            self.id = res.json()["data"]["id"]
            self.c = Connection.get(self.id)
            self.load_schema(quiet=True)
            return True
        else:
            logger.warn("save Connection fail, err is: {}", res.json())
        return False

    def delete(self):
        res = req.delete("/Connections/" + self.id, json=self.c)
        if res.status_code == 200 and res.json()["code"] == "ok":
            logger.info("delete {} Connection success", self.id)
            return True
        else:
            logger.warn("delete Connection fail, err is: {}", res.json())
        return False

    def __getitem__(self, key):
        return self.c[key]

    def __setitem__(self, key, value):
        self.c[key] = value

    @staticmethod
    @help_decorate("static method, used to check whether a connection exists", args="connection name",
                   res="whether exists, bool")
    def exists(name):
        connections = Connection.list()["data"]
        for c in connections:
            if c["name"] == name:
                return True
        return False

    @staticmethod
    @help_decorate("static method, used to list all connections", res="connection list, list")
    def list():
        return req.get("/Connections").json()["data"]

    @staticmethod
    @help_decorate("get a connection, by it's id or name", args="id or name, using kargs",
                   res="a connection/None if not exists, Connection")
    def get(id=None, name=None):
        if id is not None:
            f = {
                "where": {
                    "id": id,
                }
            }
        else:
            f = {
                "where": {
                    "name": name,
                }
            }

        data = req.get("/Connections", params={"filter": json.dumps(f)}).json()["data"]
        if len(data["items"]) == 0:
            return None
        return data["items"][0]

    @help_decorate("test a connection", res="whether connection valid, bool")
    def test(self):
        return self.load_schema()

    def load_schema(self, quiet=False):
        res = True

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                data = self.c
                data["database_password"] = self.c.get("plain_password")
                data["transformed"] = True
                payload = {
                    "type": "testConnection",
                    "data": data,
                    "updateSchema": True
                }
                await websocket.send(json.dumps(payload))

                while True:
                    recv = await websocket.recv()
                    loadResult = json.loads(recv)
                    if loadResult["type"] != "pipe":
                        continue
                    if loadResult["data"]["type"] != "testConnectionResult":
                        continue
                    if loadResult["data"]["result"]["status"] == None:
                        continue

                    if loadResult["data"]["result"]["status"] != "ready":
                        res = False
                    else:
                        res = True

                    if not quiet:
                        if loadResult["data"]["result"] == None:
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
            asyncio.get_event_loop().run_until_complete(l())
        except Exception as e:
            logger.warn("load schema exception, err is: {}", e)

        while True:
            try:
                time.sleep(1)
                res = req.get("/Connections/" + self.id).json()
                if res["data"] == None:
                    break
                if "loadFieldsStatus" not in res["data"]:
                    continue
                if res["data"]["loadFieldsStatus"] == "finished":
                    break
            except Exception as e:
                break
        return res


# used to describe a pipeline job
class Dag:
    def __init__(self, name=""):
        self.name = name
        self.status = JobStatus.edit
        self.dag = {
            "edges": [],
            "nodes": []
        }
        self.jobType = JobType.migrate
        self.setting = {
            "distinctWriteType": "intellect"
        }

    def config(self, config=None):
        if config is None:
            return self.setting
        self.setting.update(config)

    def edge(self, source, sink):
        _source = None
        _sink = None

        for node in self.dag["nodes"]:
            if node["id"] == source.id:
                _source = node
            if node["id"] == sink.id:
                _sink = node

        if source.source is not None:
            sink.source = source.source
        if type(source) == Source:
            sink.source = source

        if _source is None:
            _source = gen_dag_stage(source)
            self.dag["nodes"].append(_source)

        if _sink is None:
            _sink = gen_dag_stage(sink)
            self.dag["nodes"].append(_sink)

        if isinstance(source, Filter) or isinstance(sink, Filter):
            nodes = []
            for node in self.dag["nodes"]:
                if node["type"] == "table":
                    node["isFilter"] = True
                nodes.append(node)

        self.dag["edges"].append({
            "source": source.id,
            "target": sink.id
        })


# object that can be operate by command
op_object_command_class = {
    "job": {
        "obj": Job,
        "cache": "jobs"
    },
    "pipeline": {
        "obj": Job,
        "cache": "jobs"
    },
    "datasource": {
        "obj": DataSource,
        "cache": "connections"
    },
    "connection": {
        "obj": DataSource,
        "cache": "connections"
    },
    "db": {
        "obj": DataSource,
        "cache": "connections"
    },
    "api": {
        "obj": Api,
        "cache": "apis"
    },
    "table": {
        "cache": "tables"
    }
}


def main():
    # ipython settings
    ip = TerminalInteractiveShell.instance()
    ip.register_magics(global_help)
    ip.register_magics(system_command)
    ip.register_magics(show_command)
    ip.register_magics(op_object_command)
    ip.register_magics(ApiCommand)
    login_with_access_code(server, access_code)
    show_connections(quiet=True)
    show_connectors(quiet=True)


def init(custom_server, custom_access_code):
    """
    provide for python sdk to init env
    """
    global server, access_code
    server = custom_server
    access_code = custom_access_code
    login_with_access_code(server, access_code)
    show_connections(quiet=True)
    show_connectors(quiet=True)


if __name__ == "__main__":
    main()
