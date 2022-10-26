#!/usr/bin/env python
# env file, include common import and global value
import os, sys
import yaml, random, argparse
import pymongo
data_path = os.path.dirname(os.path.abspath(__file__)) + "/data"
sys.path.append(data_path)
from importlib import import_module
from copy import deepcopy

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../init")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../tapshell")
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../utils")
from log import logger as logger2
logger = logger2  # use log from utils, not from tapdata cli

from tapdata_cli.cli import *
from suffix import *
from factory import *

from sources import *
from create_datasource import *
server = os.getenv("secrets.server")
access_token = os.getenv("secrets.access_token")

if server is None:
    server = os.getenv("server")
    access_token = os.getenv("access_token")

if server is None:
    with open(os.path.dirname(os.path.abspath(__file__)) + "/../config.yaml", "r") as fd:
        env = yaml.safe_load(fd).get("env")
        if env is not None:
            server = env.get("server")
            access_token = env.get("access_token")

init(server, access_code)
