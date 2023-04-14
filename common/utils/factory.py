import os, sys, yaml
from importlib import import_module

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

from helper.suffix import get_suffix


def newDB(name):
    db_classes = {}
    files = os.listdir(os.path.dirname(os.path.abspath(__file__)))
    for f in files:
        if not f.endswith(".py"):
            continue
        module_name = f.split(".")[0]
        try:
            module = import_module(module_name)
            for m in module.__dict__:
                try:
                    connector = module.__dict__[m].__dict__.get("connector")
                except Exception as e:
                    continue
                if type(connector) != type(""):
                    continue
                if connector is None:
                    continue
                db_classes[connector] = module.__dict__[m]
        except Exception as e:
            print(e)
            pass

    datasource = name.split(".")[0]
    if datasource.endswith(get_suffix()):
        datasource = datasource[0:-len(get_suffix())]
    table = ""
    if len(name.split(".")) == 2:
        table = name.split(".")[1]

    with open(os.path.dirname(os.path.abspath(__file__)) + "/../../config/config.yaml", "r") as fd:
        sources = yaml.safe_load(fd)
        for source in sources:
            if source != datasource:
                continue
            config = sources[source]
            connector = config["connector"]
            if connector not in db_classes:
                continue
            return db_classes[connector](config, table)
