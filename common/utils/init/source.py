#! /usr/bin/env python
import os, sys, yaml, json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../data/init_data'


# parse datasource from config.yaml
def get_sources(*args: str):
    datasources = {}
    tables = []
    for f in os.listdir(data_path):
        if not f.endswith(".py"):
            continue
        tables.append(f.split(".")[0])

    with open(os.path.dirname(os.path.abspath(__file__)) + "/../../../config/config.yaml", "r") as fd:
        sources = yaml.safe_load(fd)
        for source in sources:
            config = sources[source]
            if "connector" not in config:
                continue
            datasources[source] = {
                "tables": tables,
                "config": config,
            }
    datasources = datasources if not args \
        else {"qa_" + source: datasources["qa_" + source] for source in args}
    return datasources


if __name__ == "__main__":
    print(json.dumps(get_sources("dummy", "mysql"), indent=4))

