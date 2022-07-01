import sys
from platform import python_version
if not python_version().startswith("3"):
    print("python version must be 3.x, please install python3 before using tapdata cli")
    sys.exit(-1)
import os

os.environ['PYTHONSTARTUP'] = '>>>'
import argparse, shlex, os
import urllib
import uuid, json
import time
import logging.handlers
from logging import *
import asyncio
import pymongo
import datetime, copy
import functools
from typing import Iterable, Tuple, Sequence
import traceback

import requests
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from IPython.core.magic import Magics, magics_class, line_magic
import websockets
from bson.objectid import ObjectId
from bson.json_util import dumps

from lib.graph import Node, Graph
from lib.rules import job_config
from lib.check import ConfigCheck


# Global Logger Utils
def get_log_level(level):
    levels = {
        "debug": 0,
        "info": 10,
        "notice": 20,
        "warn": 30,
        "error": 40,
    }
    if level.lower() in levels:
        return levels[level.lower()]
    return 100


class Logger:
    def __init__(self, name=""):
        self.name = name
        self.max_len = 0
        self.color_map = {
            "info": "32",
            "notice": "34",
            "debug": "0",
            "warn": "33",
            "error": "31"
        }

    def _header(self):
        if logger_header:
            return "\033[1;34m" + self.name + ", " + "\033[0m" + \
                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + ": "
        else:
            return ""

    def _print(self, msg, wrap=True):
        end = "\r"
        if wrap:
            end = "\n"
        l = len(self._header() + msg)
        tail = ""
        if l > self.max_len:
            self.max_len = l
        if self.max_len > 180:
            self.max_len = 180
        if l < self.max_len:
            tail = " " * (self.max_len - l)
        print(self._header() + msg + tail, end=end)

    def info(self, *args, **kargs):
        msg = args[0].replace("{}", "\033[1;32m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def debug(self, *args, **kargs):
        msg = args[0]
        self._print(msg.format(*args[1:]), **kargs)

    def notice(self, *args, **kargs):
        msg = args[0].replace("{}", "\033[1;34m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def warn(self, *args, **kargs):
        msg = args[0].replace("{}", "\033[1;33m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def error(self, *args, **kargs):
        msg = args[0].replace("{}", "\033[1;31m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def fatal(self, *args, **kargs):
        msg = args[0].replace("{}", "\033[1;31m{}\033[0m")
        self._print(msg.format(*args[1:]), **kargs)

    def log(self, *args, **kargs):
        msg = args[0]
        color_n = msg.count("{}")
        if len(args) != 1 + color_n * 2:
            self._print(
                "log error, \033[1;32m{}\033[0m args expected, \033[1;32m{}\033[0m got, print all args: {}" \
                    .format(1 + color_n * 2, len(args), args), **kargs
            )
            return

        params = list(args[1:1 + color_n])
        for i in args[1 + color_n:]:
            msg = msg.replace(
                "{}", "\033[1;" + self.color_map.get(i) +
                      "m__24FA49F1-7C36-4481-ACF7-BF2146EA4719__\033[0m", 1
            )
        msg = msg.replace("__24FA49F1-7C36-4481-ACF7-BF2146EA4719__", "{}")
        for i in range(len(params)):
            p = str(params[i])
            for j in range(int(p.count("`") / 2)):
                p = p.replace("`", "\033[1;34m", 1)
                p = p.replace("`", "\033[0m", 1)
            params[i] = p
        self._print(msg.format(*params), **kargs)


logger = Logger("tapdata")

# make requests and urllib3 quiet
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


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
                    if is_empty(context):
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


# init global logger header conf, if false, no logger prefix(inlcude time, title ...) will display
logger_header = False

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


## some static utils, simple and no direct relation with this tool
##################################################################
# judge whether a object is empty
def is_empty(arg):
    if arg == "":
        return True
    if arg is None:
        return True
    if type(arg) in [type([]), type({})]:
        if len(arg) == 0:
            return True
    return False


# pad a string to a certain length
def pad(string, length):
    string = str(string)
    import re
    def len_zh(data):
        temp = re.findall('[^_\-a-zA-Z$0-9. #()\',\\\\/]+', data)
        count = 0
        for i in temp:
            count += len(i)
        return (count)

    zh = len_zh(string)
    if len(string) >= length:
        return string
    return string + " " * (length - len(string) - zh)


####################################################################


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
            if is_empty(commands):
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
        if is_empty(t):
            for k, v in help_args.items():
                logger.log("{}: {}", k, _l[v], "info", "debug")
            return
        try:
            eval('show_help("' + t + '")')
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
            if (i >= "0" and i <= "9") or (i >= "a" and i <= "f"):
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
    if client_cache["tables"].get(source) is None:
        show_tables(quiet=True, source=source)
    table = client_cache["tables"][source][index_type][t]

    table_id = table["id"]
    table_name = table["original_name"]
    api = system_server_conf["api"] + "/MetadataInstances/" + table_id + system_server_conf["auth_param"]
    res = requests.get(api)
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
    pdkHash = ""
    if objType == Source or objType == Sink:
        if obj.databaseType.lower() in client_cache["connectors"]:
            pdkHash = client_cache["connectors"][obj.databaseType.lower()]["pdkHash"]

    if objType == Source:
        return {
            "attrs": {
                "accessNodeProcessId": "",
                "connectionType": "source_and_target",
                "position": [0, 0],
                "pdkType": "pdk",
                "pdkHash": pdkHash
            },
            "connectionId": obj.connectionId,
            "databaseType": obj.databaseType,
            "database_type": obj.databaseType,
            "id": str(obj.id),
            "tableName": obj.tableName,
            "name": obj.tableName,
            "type": "table",
            "totalReadMethod": "fullRead",
            "increasePoll": "fullRead",
            "increaseReadSize": 100,

        }
    if objType == Sink:
        return {
            "attrs": {
                "accessNodeProcessId": "",
                "connectionType": "source_and_target",
                "position": [0, 0],
                "pdkHash": pdkHash,
                "pdkType": "pdk"
            },
            "connectionId": obj.connectionId,
            "databaseType": obj.databaseType,
            "id": str(obj.id),
            "tableId": obj.tableId,
            "tableName": obj.tableName,
            "name": obj.tableName,
            "type": "table"
        }

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
    api = system_server_conf["api"] + "/DatabaseTypes"
    res = requests.get(api + system_server_conf["auth_param"])
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
    api = system_server_conf["api"] + "/Task" + system_server_conf["auth_param"] + "&filter=" + urllib.parse.quote_plus(
        json.dumps(f))
    res = requests.get(api)
    data = res.json()["data"]["items"]
    global client_cache
    jobs = {"name_index": {}, "id_index": {}, "number_index": {}}
    logger.info("system has {} jobs", len(data))
    for i in range(len(data)):
        if "name" not in data[i]:
            continue
        if not quiet:
            logger.log(
                "{}: " + pad(data[i]["name"], 42) + " {} {}", data[i]["id"][-6:], pad(data[i].get("status", "unkownn"), 12),
                data[i].get("syncType", "unknown") + "/" + data[i].get("type", "unknown"),
                "debug", "info" if data[i].get("status", "unkownn") != "error" else "error", "notice"
            )
        jobs["name_index"][data[i]["name"]] = data[i]
        jobs["id_index"][data[i]["id"]] = data[i]
        jobs["number_index"][str(i)] = data[i]
    client_cache["jobs"] = jobs


# show all apis
def show_apis(quiet=False):
    global client_cache
    api = system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"]
    res = requests.get(api)
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
        client_cache["apis"]["name_index"][data[i]["basePath"]] = {
            "id": data[i]["id"],
            "table": data[i]["tablename"],
            "name": data[i]["name"]
        }
        if not quiet:
            logger.log(
                "{} {} {} {} {}",
                pad(data[i]["name"], 20),
                pad(data[i]["tablename"], 20),
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
    api = system_server_conf["api"] + "/Connections"
    f = {"limit": 10000}
    res = requests.get(api + system_server_conf["auth_param"] + "&filter=" + urllib.parse.quote_plus(json.dumps(f)))
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
    api = system_server_conf["api"] + "/MetadataInstances" + system_server_conf[
        "auth_param"] + "&filter=" + urllib.parse.quote_plus(json.dumps(f))
    res = requests.get(api)
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
                logger.log("{} "*each_line_table_count, *each_line_tables, *["notice" for i in range(each_line_table_count)])
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
    @help_decorate("[Job] keep monitor a object status", "monitor $job_name t=30")
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
    #del (connection["response_body"])
    #del (connection["transformed"])
    #del (connection["schemaVersion"])
    #del (connection["username"])
    #del (connection["loadCount"])
    #del (connection["loadSchemaDate"])
    #del (connection["tableCount"])
    #del (connection["everLoadSchema"])
    #del (connection["loadFieldsStatus"])
    #del (connection["loadFieldErrMsg"])
    display = {}
    for k, v in connection.items():
        if v is None or v == "":
            continue
        display[k] = v
    print(json.dumps(display, indent=4))


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
        res = requests.patch(system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"], json=payload,
                             cookies=system_server_conf["cookies"])
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
        res = requests.post(system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"],
                            json=payload).json()
        if res["msg"] == "ok":
            logger.info(
                "publish api {} success, you can test it by: {}",
                base_path,
                "http://" + server + "#/apiDocAndTest?id=" + base_path + "_v1"
            )
        else:
            logger.warn("publish api {} fail, err is: {}", base_path, res["msg"])


@magics_class
class show_command(Magics):
    @line_magic
    @help_decorate("[Job,Datasource,Api,Table] show objects", "show objects")
    def show(self, line):
        if is_empty(line):
            pass
        try:
            eval("show_" + line + "()")
        except Exception as e:
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
    print(json.dumps(display_fields, indent=4))


# def login(server, access_code):
#     login_with_access_code(server, access_code)


def login_with_access_code(server, access_code):
    global system_server_conf
    api = "http://" + server + "/api"
    res = requests.post(api + "/users/generatetoken", json={"accesscode": access_code})
    if res.status_code != 200:
        logger.warn("init get token request fail, err is: {}", res.json())
        return False
    data = res.json()["data"]
    token = data["id"]
    user_id = data["userId"]

    res = requests.get(api + "/users?access_token=" + token)
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
        if is_empty(line):
            logger.warn("args can not be empty for login")
            return
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--server", type=str)
        parser.add_argument("-u", "--username", type=str)
        parser.add_argument("-p", "--password", type=str)
        parser.add_argument("-a", "--access_code", type=str)
        args = parser.parse_args(shlex.split(line))
        if is_empty(args.server):
            args.server = "127.0.0.1:3030"
        if not is_empty(args.access_code):
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
        if is_empty(l):
            return
        if i18n.get(l) is None:
            logger.warn("lang {} not support, will use lang {}", l, _lang, "warn", "notice")
            return
        _lang = l
        _l = i18n[_lang]


ip = TerminalInteractiveShell.instance()
ip.register_magics(global_help)
ip.register_magics(system_command)
ip.register_magics(show_command)
ip.register_magics(op_object_command)
ip.register_magics(ApiCommand)


@help_decorate("Enum, used to describe a job status")
class JobStatus():
    edit = "edit"
    running = "running"
    scheduled = "scheduled"
    paused = "paused"
    stop = 'stop'
    stopping = 'stopping'
    complete = "complete"


@help_decorate("Enum, used to describe a connection readable or writeable")
class ConnectionType:
    source = "source"
    target = "target"
    both = "source_and_target"


@help_decorate("Enum, used to describe write mode for a row")
class WriteMode():
    upsert = "updateOrInsert"
    update = "updateWrite"
    upsert_array = "merge_embed"


upsert = "updateOrInsert"
update = "updateWrite"


@help_decorate("Enum, used to config job type")
class SyncType:
    initial = "initial_sync"
    cdc = "cdc"
    both = "initial_sync+cdc"


@help_decorate("Enum, used to config action before sync data")
class DropType:
    no_drop = "no_drop"
    data = "drop_data"
    all = "drop_schema"


no_drop = "no_drop"
drop_data = "drop_data"
drop_schema = "drop_schema"


@help_decorate("Single Table Relation, used to config how a row from source infect it's sink")
class SingleTableRelation:
    def __init__(self, writeMode, association, path="", array_key=""):
        self.writeMode = writeMode
        self.association = association
        self.path = path
        self.array_key = array_key


@help_decorate("Multi Table Relation used to add prefix/suffix when migrate multi tables")
class MultiTableRelation:
    def __init__(self, prefix="", suffix="", drop_type=DropType.no_drop):
        self.prefix = prefix
        self.suffix = suffix
        self.drop_type = drop_type


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
                 mergeType=WriteMode.update,
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
            "children": [ i.to_dict() for i in self.child ],
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
                 mergeType=WriteMode.update,
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
                    "children": [ i.to_dict() for i in self.child ],
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
    input = 0
    output = 0
    insert = 0
    update = 0
    delete = 0
    delay = 0


class LogMinerMode:
    manually = "manually"
    automatically = "automatically"


@help_decorate("use to define a stream pipeline", "p = new Pipeline($name).readFrom($source).writeTo($sink)")
class Pipeline:
    @help_decorate("__init__ method", args="p = Pipeline($name)")
    def __init__(self, name=None):
        if name is None:
            name = str(uuid.uuid4())
        self.dag = Dag(name="name")
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

    @help_decorate("read data from source", args="p.readFrom($source)")
    def readFrom(self, source):
        if type(source) == type(QuickDataSourceMigrateJob()):
            source = source.__db__
        if type(source) == type(""):
            if "." in source:
                db = source.split(".")[0]
                table = source.split(".")[1]
                source = Source(db, table)
            else:
                db = source
                source = Source(db)
        if source.type == "database":
            self.dag.jobType = JobType.migrate
        else:
            self.dag.jobType = JobType.sync
        self.sources.append(source)
        return self._clone(source)

    @help_decorate("write data to sink", args="p.writeTo($sink, $relation)")
    def writeTo(self, sink, relation=MultiTableRelation(), writeMode=WriteMode.upsert, ttl="", prefix="", suffix="",
                path="", array_key="", association=[], drop_type=""):
        if type(sink) == type(QuickDataSourceMigrateJob()):
            sink = sink.__db__
        if type(sink) == type(""):
            if sink in self.cache_sinks:
                sink = self.cache_sinks[sink]
            else:
                if "." in sink:
                    db = sink.split(".")[0]
                    table = sink.split(".")[1]
                    self.cache_sinks[sink] = Sink(db, table)
                else:
                    db = sink.split(".")[0]
                    self.cache_sinks[sink] = Sink(db)
                sink = self.cache_sinks[sink]
        if self.dag.jobType == JobType.sync and type(relation) == type(MultiTableRelation()):
            auto_association = []
            for pk in self.sources[len(self.sources) - 1].primary_key:
                auto_association.append((pk, pk))
            relation = SingleTableRelation(writeMode=writeMode, association=auto_association)
        if self.dag.jobType == JobType.migrate:
            if relation.prefix == "" and prefix != "":
                relation.prefix = prefix
            if relation.suffix == "" and suffix != "":
                relation.suffix = suffix
            if drop_type != "":
                relation.drop_type = drop_type
        if self.dag.jobType == JobType.sync:
            if relation.array_key == "" and array_key != "":
                relation.array_key = array_key
            if relation.path == "" and path != "":
                relation.path = path
            if len(relation.association) == 0 and len(association) != 0:
                relation.association = association
        self.dag.edge(self.stage, sink, relation, ttl)
        self.sinks.append({"sink": sink, "relation": relation})
        return self._clone(sink)

    def _common_stage(self, f):
        self.dag.edge(self.stage, f, None, "")
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
            self.dag.edge(self.stage, f, None, "")
        else:
            self.dag.edge(self.stage, f, None, "")
            self.dag.edge(p.stage, f, None, "")
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
            js_script = translates(inspect.getsource(script))[0]
            f = Js(js_script, False)
        else:
            if script.endswith(".js"):
                js_script = open(script, "r").read()
                script = js_script
            f = Js(script)
        return self._common_stage(f)

    @help_decorate("merge another pipeline", args="p.merge($pipeline)")
    def merge(self, pipeline, association: Iterable[Sequence[Tuple[str, str]]]=None, mergeType="updateWrite", targetPath=""):
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
    def config(self, config: dict = None):

        if not isinstance(config, dict):
            logger.warn("type {} must be {}", config, "dict", "notice", "notice")
            return
        mode = self.dag.jobType
        resp = ConfigCheck(config, job_config[mode], keep_extra=True).checked_config
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
        job.config(self.dag.setting)
        job.config({
            "sync_type": SyncType.both,
            "stopOnError": True,
            "needToCreateIndex": True,
            "readBatchSize": 100,
            "transformModelVersion": "v1",
            "readShareLogMode": "STREAMING",
            "processorConcurrency": 1
        })
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
        logger.info("job {} status is: {}", self.name, self.job.status())
        return self.job.status()

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
        global logger_header
        if self.job is None:
            logger.warn("pipeline not start, no monitor can show")
            return
        logger_header = True
        self.job.monitor(t)
        logger_header = False
        return self

    def check(self):
        global logger_header
        if self.status() not in [JobStatus.running, JobStatus.stop, JobStatus.complete]:
            logger.warn(
                "{}", "The status of this task is not in [running, stop, complete], unable to check data."
            )
            return
        if self.check_job is None:
            self.check_job = DataCheck(self.sources[0], self.sinks[0]["sink"], self.sinks[0]["relation"],
                                       name=self.name)
            self.check_job.start()
        logger_header = True
        while True:
            time.sleep(1)
            if self.check_job.status() == "scheduling":
                logger.info("prepareing for data check, please wait for a while ...", wrap=False)
                continue
            stats = self.check_job.stats()
            if self.check_job.status() == "running":
                logger.info("data check running, progress is: {} %", stats["progress"] * 100, wrap=False)
                continue
            if self.check_job.status() == "done":
                logger.log(
                    "data check finished, check result is: {}, same row is number is: {}, diff row number is: {}",
                    stats["result"],
                    stats["row_passed"],
                    stats["row_failed"],
                    "info" if stats["result"] != "failed" else "error", "info",
                    "warn"
                )
                break
        logger_header = False


class Agg(BaseObj):
    def __init__(self, name, method, key, groupKeys, pk=[], ttl=3600):
        super().__init__()
        self.name = name
        self.method = method
        self.key = key
        self.groupKeys = groupKeys
        self.pk = pk
        self.ttl = ttl


@help_decorate("source is start of a pipeline", "source = Source($Datasource, $table)")
class Source:
    def __getattr__(self, key):
        return None

    @help_decorate("__init__ method", args="connection, table, sql")
    def __init__(self, connection, table=["_"], table_re=None, sql=""):
        global client_cache
        self.ori_connection = connection
        if type(connection) == type(QuickDataSourceMigrateJob()):
            connection = connection.__db__
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        if type(connection) != type(Connection()):
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
        if table == ["_"]:
            if c["id"] not in client_cache["tables"]:
                show_tables(source=connection.id, quiet=True)
            table = list(client_cache["tables"][c["id"]]["name_index"].keys())
        if table_re is not None:
            tables = []
            all_tables = show_tables(source=connection.id, quiet=True)
            import re
            for t in all_tables:
                if "original_name" not in t:
                    continue
                if re.match(table_re, t["original_name"]):
                    tables.append(t["original_name"])
                table = tables

        if type(table) == type([]):
            connection.c["type"] = "database"
        else:
            if c["database_type"] == "mongodb":
                connection.c["type"] = "collection"
            else:
                connection.c["type"] = "table"
        self.connection = connection

        self.id = str(uuid.uuid4())
        self.connectionId = str(connection["id"])
        self.databaseType = connection["database_type"]
        self.type = connection["type"]
        self.sql = sql

        client_cache["connection"] = self.connectionId

        if type(table) == type([]):
            if len(table) > 0:
                self.tableName = table[0]
                self.table = table
            else:
                self.tableName = ""
                self.table = []
        else:
            self.tableName = table
            self.table = [table]

        self.tableId = self._getTableId(self.tableName)
        if self.tableId is None:
            self.tableId = str(uuid.uuid4())
        self.source = None

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
        res = requests.get(system_server_conf["api"] + "/MetadataInstances" + system_server_conf[
            "auth_param"] + "&filter=" + json.dumps(payload)).json()
        table_id = None
        for s in res["data"]["items"]:
            if s["original_name"] == tableName:
                table_id = s["id"]
                break
        if table_id is not None:
            self.primary_key = []
            res = requests.get(
                system_server_conf["api"] + "/MetadataInstances/" + table_id + system_server_conf["auth_param"]).json()
            for field in res["data"]["fields"]:
                if field.get("primaryKey", False):
                    self.primary_key.append(field["field_name"])
        return table_id

    @help_decorate("cache this source to idaas, you can preview it after cache it")
    def cache(self):
        if self.cache_p is not None:
            logger.info("cache already, no need to do it again")
            return
        self.cache_table = self.tableName + "_cache"
        if self.cache_client is None:
            self.cache_conn = Connection.get(name=fdm)
            mongo_uri = "mongodb://"
            if self.cache_conn["database_username"] != "":
                mongo_uri = mongo_uri + self.cache_conn["database_username"]
            if self.cache_conn["database_password"] != "":
                mongo_uri = mongo_uri
            mongo_uri = mongo_uri + "@" + self.cache_conn["database_host"] + "/" + self.cache_conn["database_name"]
            mongo_uri = mongo_uri + "?" + self.cache_conn["additionalString"]
            self.cache_client = pymongo.MongoClient(mongo_uri)
            if len(list(self.cache_client[fdm][self.cache_table].list_indexes())) > 0:
                logger.info("cache table already exists, link done!")
                return
        p = Pipeline(name=str(uuid.uuid4()))
        cache_source = Source(self.ori_connection, table=self.table)
        cache_sink = Sink(fdm)
        p.readFrom(cache_source).writeTo(cache_sink, relation=MultiTableRelation(suffix="_cache"))
        p.start()
        self.cache_p = p

    @help_decorate("get cache job status")
    def cache_status(self):
        self.cache_p.status()

    @help_decorate("preview this source, only AFTER cache() call")
    def preview(self, limit=10, pretty=False):
        if self.cache_p is None and self.cache_client is None:
            logger.warn("please call cache() before using preview")
            return
        example = list(self.cache_client[fdm][self.cache_table].find().limit(limit))
        for i in range(len(example)):
            logger.log("{}: {}", i, example[i] if not pretty else dumps(example[i], indent=4, ensure_ascii=False),
                       "info", "debug")

    @help_decorate("count this source, only AFTER cache() call")
    def count(self):
        if self.cache_p is None and self.cache_client is None:
            logger.warn("please call cache() before using preview")
            return
        c = self.cache_client[fdm][self.cache_table].count()
        logger.info("cache table count is: {}", c)

    @help_decorate("find using a {}, only AFTER cache() call", args="query, {}")
    def find(self, query, limit=10, pretty=False):
        if self.cache_p is None and self.cache_client is None:
            logger.warn("please call cache() before using preview")
            return
        if "_id" in query and type(query["_id"]) == type(""):
            query["_id"] = ObjectId(query["_id"])
        result = self.cache_client[fdm][self.cache_table].find(query).limit(limit)
        c = result.count()
        if c > 10:
            logger.info("query match rows count: {}, will show 10 rows for preview", c)
        example = list(self.cache_client[fdm][self.cache_table].find(query).limit(10))
        for i in range(len(example)):
            logger.log("{}: {}", i, example[i] if not pretty else dumps(example[i], indent=4, ensure_ascii=False),
                       "info", "debug")


@help_decorate("sink is end of a pipeline", "sink = Sink($Datasource, $table)")
class Sink(Source):
    def __init__(self, connection, table=["_"]):
        super().__init__(connection, table)


class Api:
    def __init__(self, id=None, name=None, table=None):
        if id is not None and name is None:
            name = id
        self.id = None
        self.name = name

        if name is None:
            return
        else:
            self.get(name)

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
        db = client_cache["connections"]["name_index"][db]["id"]

        fields = get_table_fields(table2, whole=True, source=db)
        self.base_path = base_path
        self.tablename = table2
        self.payload = {
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
            "tablename": self.tablename,
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

    def publish(self):
        if self.id is None:
            res = requests.post(system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"],
                                json=self.payload).json()
            if res["msg"] == "ok":
                logger.info("publish api {} success, you can test it by: {}", self.base_path,
                            "http://" + server + "#/apiDocAndTest?id=" + self.base_path + "_v1")
                self.id = res["data"]["id"]
            else:
                logger.warn("publish api {} fail, err is: {}", self.base_path, res["msg"])
        else:
            payload = {
                "id": self.id,
                "status": "active"
            }
            res = requests.patch(system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"],
                                 json=payload, cookies=system_server_conf["cookies"])
            res = res.json()
            if res["code"] == "ok":
                logger.info("publish {} success", self.name)
            else:
                logger.warn("publish {} fail, err is: {}", self.name, res["msg"])

    def get(self, name):
        global client_cache
        if len(client_cache.get("apis", {}).get("name_index")) == 0:
            show_apis(quiet=True)
        api = client_cache["apis"]["name_index"].get(name)
        if api is None:
            return None
        api_id = api["id"]
        self.id = api_id

    def unpublish(self):
        if self.id is None:
            return
        payload = {
            "id": self.id,
            "status": "pending"
        }
        res = requests.patch(system_server_conf["api"] + "/Modules" + system_server_conf["auth_param"], json=payload,
                             cookies=system_server_conf["cookies"])
        res = res.json()
        if res["code"] == "ok":
            logger.info("unpublish {} success", self.id)
        else:
            logger.warn("unpublish {} fail, err is: {}", self.id, res["msg"])

    def delete(self):
        if self.id is None:
            logger.warn("delete api {} fail, err is: {}", self.name, "api not find")
            return
        res = requests.delete(system_server_conf["api"] + "/Modules/" + self.id + system_server_conf["auth_param"],
                              cookies=system_server_conf["cookies"])
        res = res.json()
        if res["code"] == "ok":
            logger.info("delete api {} success", self.name)
        else:
            logger.warn("delete api {} fail, err is: {}", self.name, res["msg"])


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
        res = requests.get(system_server_conf["api"] + "/DataFlows" + system_server_conf[
            "auth_param"] + '&filter={"fields":{"id":true,"name":true,"status":true,"agentId":true,"stats":true}}')
        if res.status_code != 200:
            return None
        res = res.json()
        jobs = []
        for i in res["data"]:
            jobs.append(Job(id=i["id"]))
        return jobs

    def reset(self):
        res = requests.post(
            system_server_conf["api"] +
            "/DataFlows/" + self.id + "/reset" +
            system_server_conf['auth_param']
        ).json()
        return True

    def _get_by_name(self):
        param = '{"where":{"name":{"like":"%s"}}}' % (self.name)
        url = system_server_conf["api"] + "/Task" + system_server_conf["auth_param"] + "&filter=" + param
        res = requests.get(url)
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
            url = system_server_conf["api"] + "/Task/findTaskDetailById/" + self.id + system_server_conf["auth_param"]
            res = requests.get(url)
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
        url = system_server_conf['api'] + '/Task/batchStop' + system_server_conf["auth_param"] + "&taskIds=" + self.id
        res = requests.put(url)
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
        res = requests.post(system_server_conf["api"] + "/DataFlows/removeAll" + system_server_conf[
            "auth_param"] + "&where=" + '{"_id":{"inq":["' + self.id + '"]}}')
        if res.status_code != 200:
            return False
        res = res.json()
        if res["code"] != "ok":
            return False
        if len(res["data"]["success"]) != 1:
            return False
        return True

    def save(self):
        job = {}
        if self.id is None:
            self.job = {
                "accessNodeProcessId": "",
                "accessNodeProcessIdList": [],
                "accessNodeType": "AUTOMATIC_PLATFORM_ALLOCATION",
                "deduplicWriteMode": "intelligent",
                "editVersion": int(time.time() * 1000),
                "syncType": self.dag.jobType,
                "type": "initial_sync+cdc",
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
        res = requests.patch(system_server_conf["api"] + "/Task" + system_server_conf["auth_param"], json=self.job,
                             cookies=system_server_conf["cookies"])
        res = res.json()
        if res["code"] != "ok":
            return False
        self.id = res["data"]["id"]

        job = res["data"]

        res = requests.patch(system_server_conf["api"] + "/Task/confirm/" + self.id + system_server_conf["auth_param"],
                             json=job)
        res = res.json()

    def start(self):
        self.save()
        if self.id is None:
            logger.warn("save job fail")
            return False
        res = requests.put(system_server_conf["api"] + "/Task/batchStart" + system_server_conf[
            "auth_param"] + "&taskIds=" + self.id).json()
        if res["code"] != "ok":
            return False

        return True

    def config(self, config):
        self.setting.update(config)

    def status(self, res=None, quiet=True):
        if res is None:
            res = requests.get(system_server_conf["api"] + "/Task/" + self.id + system_server_conf["auth_param"]).json()
        status = res["data"]["status"]
        if not quiet:
            logger.info("job status is: {}", status)
        return status

    def get_sub_task_ids(self):
        sub_task_ids = []
        res = requests.get(system_server_conf["api"] + "/Task/" + self.id + system_server_conf["auth_param"]).json()
        statuses = res["data"]["statuses"]
        jobStats = JobStats()
        for subTask in statuses:
            sub_task_ids.append(subTask["id"])
        return sub_task_ids

    def stats(self, res=None):
        res = requests.get(system_server_conf["api"] + "/Task/" + self.id + system_server_conf["auth_param"]).json()
        statuses = res["data"]["statuses"]
        jobStats = JobStats()
        for subTask in statuses:
            payload = {
              "statistics": [
                {
                  "tags": {
                    "subTaskId": subTask["id"],
                    "type": "subTask"
                  }
                }
              ]
            }
            res = requests.post(
                system_server_conf["api"] + "/measurement/query" + system_server_conf["auth_param"],
                json=payload
            ).json()
            for statistic in res["data"]["statistics"]:
                jobStats.delay = statistic["replicateLag"]
                jobStats.output = statistic["outputTotal"]
                jobStats.input = statistic["inputTotal"]
                jobStats.insert = statistic["insertedTotal"]
                jobStats.update = statistic["updatedTotal"]
                jobStats.delete = statistic["deletedTotal"]
        return jobStats

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
                                        logger.log("[{}] {} {}: {}", i["level"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i["date"])), i["loggerName"], i["message"],
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
                    "job {} status: {}, delay: {}, stats: input {}, output {}, insert {}, update {}, delete {}",
                    self.name, status, stats.delay, stats.input, stats.output, stats.insert, stats.update, stats.delete,
                    "info", "info", "notice", "info", "info", "info", "info", "info", wrap=False
                )
            if status in [JobStatus.running, JobStatus.edit, JobStatus.scheduled]:
                continue
            logger.info(
                "job {} status: {}, delay: {}, stats: input {}, output {}, insert {}, update {}, delete {}",
                self.name, status, stats.delay, stats.input, stats.output, stats.insert, stats.update, stats.delete,
                "info", "info", "notice", "info", "info", "info", "info", "info", wrap=False
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
class DataSource():
    def __init__(self, connector="", name=None, type="source_and_target", id=None):
        if id is not None:
            self.id = id
            self.c = self.get(id=id)
            return
        if connector != "" and name is None:
            name = connector

        if name != "":
            obj = get_obj("datasource", name)
            if obj is not None:
                self.id = obj.id
                self.c = obj.c
                return

        self.custom_options = {}
        self.is_url = False
        self.user_id = system_server_conf["user_id"]
        self._name = name
        self._options = ""
        self._type = "source_and_target"
        self._host = ""
        self._port = ""
        self._username = ""
        self._password = ""
        self._db = ""
        self._manual_options = []
        self._connector = ""
        self._schema = ""
        self.c = None
        if connector != "":
            self._connector = connector
        self._type = type

    def __getattr__(self, key):
        def set_custom_options(*args, **kwargs):
            self.custom_options["_" + key] = args[0]
            return self

        if key in dir(self):
            return getattr(self, key)
        return set_custom_options

    @staticmethod
    @help_decorate("static method, used to list all datasources", res="datasource list, list")
    def list():
        return requests.get(system_server_conf["api"] + "/Connections" + system_server_conf["auth_param"]).json()[
            "data"]

    @help_decorate("desc a datasource, display readable struct", res="datasource struct")
    def desc(self, quiet=True):
        c = self.c
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
            if is_empty(v):
                remove_keys.append(k)

        for k in remove_keys:
            if k in c:
                del (c[k])

        if not quiet:
            print(json.dumps(c, indent=4))

        return c

    @help_decorate("get a datasource status", "")
    def status(self, quiet=True):
        c = self.c
        if c is None:
            logger.warn("the status is None. please check the name or connector")
            return
        status = c.get("status")
        tableCount = c.get("tableCount", "unknown")
        loadCount = c.get("loadCount", 0)
        loadFieldsStatus = c.get("loadFieldsStatus", False)
        loadSchemaDate = c.get("loadSchemaDate", "unknown")
        if not quiet:
            logger.info("datasource {} status is: {}, it has {} tables, loaded {}, last load time is: {}",
                        c.get("name"), status, tableCount, loadCount, loadSchemaDate)
        return status

    def host(self, host):
        self._manual_options.append(sys._getframe().f_code.co_name)
        self._host = host
        if ":" in host:
            self._port = int(host.split(":")[1])
        if self._connector == "mysql" or self._connector == "oracle":
            self._manual_options.append("port")
            if ":" in host:
                self._host = host.split(":")[0]
                self._port = int(host.split(":")[1])
            else:
                self._host = host
                self._port = 3306
        return self

    def schema(self, schema):
        self._schema = schema
        self._manual_options.append(sys._getframe().f_code.co_name)
        return self

    def uri(self, uri):
        self._uri = uri
        self._manual_options.append(sys._getframe().f_code.co_name)
        return self

    def port(self, port):
        self._port = port
        self._manual_options.append(sys._getframe().f_code.co_name)
        return self

    def username(self, username):
        self._username = username
        self._manual_options.append(sys._getframe().f_code.co_name)
        return self

    def password(self, password):
        self._password = password
        self._manual_options.append(sys._getframe().f_code.co_name)
        return self

    def db(self, db):
        self._database = db
        self._manual_options.append("database")
        return self

    def connector(self, connector):
        self._connector = connector
        return self

    def type(self, connection_type):
        self._type = connection_type
        return self

    def props(self, options):
        self._options = options
        return self

    def to_pdk_dict(self):
        d = {}
        for i in self._manual_options:
            d[i] = getattr(self, "_" + i)
        return d

    def to_dict(self):
        if self.c is not None:
            return self.c
        if type(self._uri) == type(""):
            uri = self._uri
        else:
            uri = ""
        d = {
            "additionalString": self._options,
            "connection_type": self._type,
            "database_host": self._host,
            "database_port": self._port,
            "database_name": self._db,
            "database_type": self._connector,
            "database_uri": uri,
            "database_owner": self._schema,
            "database_username": self._username,
            "plain_password": self._password,
            "isUrl": True if self._uri != "" else False,
            "name": self._name,
            "user_id": self.user_id,
            "response_body": {}
        }
        for k, v in self.custom_options.items():
            d[k[1:]] = v

        d["database_password"] = d.get("plain_password")
        database_type = d.get("database_type", "")
        if database_type.lower() not in client_cache["connectors"]:
            logger.warn("connector {} not support, support list is: {}", database_type, client_cache["connectors"])

        connector = client_cache["connectors"][database_type.lower()]
        d["pdkType"] = "pdk"
        d["pdkHash"] = connector["pdkHash"]
        d["database_type"] = connector["name"]
        d["config"] = self.to_pdk_dict()
        return d

    @staticmethod
    @help_decorate("get a datasource by it's id or name", args="id or name, using kargs", res="a DataSource Object")
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

        data = requests.get(system_server_conf["api"] + "/Connections" + system_server_conf[
            "auth_param"] + "&filter=" + urllib.parse.quote_plus(json.dumps(f)),
                            cookies=system_server_conf["cookies"]).json()["data"]
        if len(data["items"]) == 0:
            return None
        return data["items"][0]

    @help_decorate("save a connection in idaas system")
    def save(self):
        api = system_server_conf["api"] + "/Connections" + system_server_conf["auth_param"]
        data = self.to_dict()
        res = requests.post(api, json=data)
        show_connections(quiet=True)
        if res.status_code == 200 and res.json()["code"] == "ok":
            self.id = res.json()["data"]["id"]
            self.c = DataSource.get(self.id)
            self.validate(quiet=False)
            return True
        else:
            logger.warn("save Connection fail, err is: {}", res.json()["msg"])
        return False

    def delete(self):
        if self.id is None:
            return
        api = system_server_conf["api"] + "/Connections/" + self.id + system_server_conf["auth_param"]
        res = requests.delete(api, json=self.c)
        if res.status_code == 200 and res.json()["code"] == "ok":
            logger.info("delete {} Connection success", self.id)
            return True
        else:
            logger.warn("delete Connection fail, err is: {}", res.json())
        return False

    @help_decorate("validate this datasource")
    def validate(self, quiet=False):
        res = True

        async def l():
            async with websockets.connect(system_server_conf["ws_uri"]) as websocket:
                data = self.to_dict()
                data["updateSchema"] = True
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

        logger.info("datasource valid finished, will check table schema now, please wait for a while ...")
        start_time = time.time()
        while True:
            try:
                time.sleep(5)
                api = system_server_conf["api"] + "/Connections/" + self.id + system_server_conf["auth_param"]
                res = requests.get(api).json()
                if res["data"] == None:
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


class MongoDB(DataSource):
    def __init__(self, name):
        self.connector = "mongodb"
        super().__init__(name)

    def uri(self, url):
        self.url = url
        self.is_url = True
        return self


class Mysql(DataSource):
    def __init__(self, name):
        self.connector = "mysql"
        super().__init__(name)

    def to_dict(self):
        base_dict = super().to_dict()
        host = base_dict["database_host"].split(":")[0]
        port = base_dict["database_host"].split(":")[1]
        base_dict["database_host"] = host
        base_dict["database_port"] = port
        return base_dict


class Postgres(DataSource):
    def __init__(self, name):
        self.connector = "postgres"
        super().__init__(name)

    def schema(self, schema):
        self.schema = schema
        return self

    def log_decorder_plugin(self, plugin):
        self.log_decorder_plugin = plugin
        return self

    def to_dict(self):
        base_dict = super().to_dict()
        host = base_dict["database_host"].split(":")[0]
        port = base_dict["database_host"].split(":")[1]
        base_dict["database_host"] = host
        base_dict["database_port"] = port
        base_dict["database_owner"] = self.schema
        base_dict["pgsql_log_decorder_plugin_name"] = "wal2json_streaming"
        if self.log_decorder_plugin is not None:
            base_dict["pgsql_log_decorder_plugin_name"] = self.log_decorder_plugin
        return base_dict


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
        #self.load_schema(quiet=False)
        api = system_server_conf["api"] + "/Connections" + system_server_conf["auth_param"]
        res = requests.post(api, json=self.c)
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
        api = system_server_conf["api"] + "/Connections/" + self.id + system_server_conf["auth_param"]
        res = requests.delete(api, json=self.c)
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
        return requests.get(system_server_conf["api"] + "/Connections" + system_server_conf["auth_param"]).json()[
            "data"]

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

        data = requests.get(system_server_conf["api"] + "/Connections" + system_server_conf[
            "auth_param"] + "&filter=" + urllib.parse.quote_plus(json.dumps(f)),
                            cookies=system_server_conf["cookies"]).json()["data"]
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

                api = system_server_conf["api"] + "/Connections/" + self.id + system_server_conf["auth_param"]
                res = requests.get(api).json()
                if res["data"] == None:
                    break
                if "loadFieldsStatus" not in res["data"]:
                    continue
                if res["data"]["loadFieldsStatus"] == "finished":
                    break
            except Exception as e:
                break
        return res


class DataCheck:
    @help_decorate("__init__ method", args="source, sink, relation, name, check_mode",
                   res="DataCheck Object, DataCheck")
    def __init__(self, source, sink, relation, name=None, check_mode="field"):
        if name is None:
            name = uuid.uuid4()
        self.check_job = DataCheck.get(name)
        association = relation.association
        source_sort_column = ""
        sink_sort_column = ""
        for c in association:
            source_sort_column = source_sort_column + "," + c[0]
        source_sort_column = source_sort_column[0:len(source_sort_column) - 1]
        for c in association:
            sink_sort_column = sink_sort_column + "," + c[1]
        sink_sort_column = sink_sort_column[0:len(sink_sort_column) - 1]
        if self.check_job is not None:
            self.id = self.check_job["id"]
            return
        self.check_job = {
            "mode": "manual",
            "inspectMethod": check_mode,
            "name": name,
            "status": "scheduling",
            "limit": {"keep": 100},
            "tasks": [
                {
                    "fullMatch": True,
                    "source": {
                        "connectionId": source.connectionId,
                        "databaseType": source.databaseType,
                        "table": source.tableName,
                        "sortColumn": source_sort_column,
                    },
                    "target": {
                        "connectionId": sink.connectionId,
                        "databaseType": sink.databaseType,
                        "table": sink.tableName,
                        "sortColumn": sink_sort_column,
                    }
                }
            ]
        }
        self.name = name

    @staticmethod
    @help_decorate("get a data check job by it's name", args="data check name",
                   res="DataCheck or None if not exists, DataCheck")
    def get(name):
        api = system_server_conf["api"] + "/Inspects" + system_server_conf["auth_param"] + "&filter=" + json.dumps(
            {"where": {"name": name}})
        data = requests.get(api).json()["data"]
        if data["total"] == 0:
            return None
        return data[0]

    @help_decorate("save a data check job, and start it")
    def save(self):
        api = system_server_conf["api"] + "/Inspects" + system_server_conf["auth_param"]
        res = requests.post(api, json=self.check_job)
        data = res.json()["data"]
        self.id = data["id"]

    @help_decorate("start data check job")
    def start(self):
        self.save()

    @help_decorate("get data check job status", res="job status")
    def status(self):
        api = system_server_conf["api"] + "/Inspects" + system_server_conf["auth_param"] + "&filter=" + json.dumps(
            {"where": {"id": self.id}})
        res = requests.get(api)
        return res.json()["data"][0]["status"]

    @help_decorate("get data check job stats", res="job stats")
    def stats(self, quiet=False):
        api = system_server_conf["api"] + "/Inspects" + system_server_conf["auth_param"] + "&filter=" + json.dumps(
            {"where": {"id": self.id}})
        res = requests.get(api)
        stats = res.json()["data"][0]["InspectResult"]["stats"][0]
        if not quiet:
            logger.log(
                "data check finished, check result is: {}, same row is number is: {}, diff row number is: {}",
                stats["result"], stats["row_passed"], stats["row_failed"],
                "info" if stats["result"] != "failed" else "error", "info", "warn"
            )
        return stats

    @help_decorate("monitor this job until it finished", args="timeout seconds")
    def monitor(self, t=30, quiet=False):
        global logger_header
        if self.id is None:
            logger.warn("data check job not start, no monitor can show")
            return
        logger_header = True
        start_time = time.time()
        while True:
            if time.time() - start_time > t:
                break
            time.sleep(1)
            status = self.status()
            stats = self.stats()
            if status == "running":
                logger.info("data check running, progress is: {} %", stats["progress"] * 100, wrap=False)
            if status == "done":
                logger.log(
                    "data check finished, check result is: {}, same row is number is: {}, diff row number is: {}",
                    stats["result"], stats["row_passed"], stats["row_failed"],
                    "info" if stats["result"] != "failed" else "error", "info", "warn"
                )
                break
        logger_header = False


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
        for k, v in config.items():
            self.setting[k] = v

    def edge(self, source, sink, relation, ttl):
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

        self.dag["edges"].append({
            "source": source.id,
            "target": sink.id
        })

        if relation is None:
            return

        if type(source) == Source:
            lastSource = source
        else:
            lastSource = sink.source

        if type(relation) == SingleTableRelation:
            _sink["existDataProcessMode"] = "keepData"
            _sink["writeStrategy"] = relation.writeMode
            updateConditionFields = []
            for i in relation.association:
                updateConditionFields.append(i[0])
            _sink["updateConditionFields"] = updateConditionFields
        if type(relation) == MultiTableRelation:
            del (_source["tableName"])
            _source["tableNames"] = lastSource.table
            _source["type"] = "database"
            _sink["syncObjects"] = [{
                "type": "table",
                "objectNames": lastSource.table
            }]
            _sink["type"] = "database"


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

# try auto login with config file
fdm = "fdm"
server = ""
try:
    conf_file = "conf.json"
    if os.path.exists(conf_file):
        f = open(conf_file, "r")
        conf = json.loads(str(f.read()))
        if not is_empty(conf.get("fdm")):
            fdm = conf.get("fdm")
        server = conf.get("server")
        access_code = conf.get("access_code")
        login_with_access_code(server, access_code)
    show_connections(quiet=True)
    show_connectors(quiet=True)
except Exception as e:
    logger.warn("load conf file failed, err is: {}", e)
