#!/usr/bin/env python
import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../init")
from env import *


def load_data_to_mem():
    data = {}
    for f in os.listdir(data_path):
        if not f.endswith(".py"):
            continue
        p = f.split(".")[0]
        table = p + get_suffix()
        data[table] = import_module(p).data
    return data


# load data to datasource config in config.yaml
def prepare_data():
    data = load_data_to_mem()
    sources = get_sources()
    for source in sources:
        db_client = newDB(source)
        if db_client is None:
            continue
        # using load function from db utils in ../utils path
        if not hasattr(db_client, "load"):
            continue

        for table, table_data in data.items():
            logger.info("loading data to datasource: {}, table: {}, count: {}", source, table, len(table_data))
            db_client.load(data=table_data, drop=True, table=table)


if __name__ == "__main__":
    prepare_data()
