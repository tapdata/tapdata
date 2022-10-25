#!/usr/bin/env python
import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../init")
from env import *


# parse datasource from config.yaml
def get_sources():
    datasources = {}
    tables = []
    for f in os.listdir(data_path):
        if not f.endswith(".py"):
            continue
        tables.append(f.split(".")[0])

    with open(os.path.dirname(os.path.abspath(__file__)) + "/../config.yaml", "r") as fd:
        sources = yaml.safe_load(fd)
        for source in sources:
            config = sources[source]
            if "connector" not in config:
                continue
            datasources[source] = {
                "tables": tables,
                "config": config,
            }
            db_client = newDB(source)
            datasources[source]["__has_data"] = False
            if db_client is None:
                continue
            # only datasource with __has_data label can work as job source
            if hasattr(db_client, "load"):
                datasources[source]["__has_data"] = True
    return datasources


if __name__ == "__main__":
    print(json.dumps(get_sources(), indent=4))
