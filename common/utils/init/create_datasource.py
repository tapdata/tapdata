import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + r"\..")

from log import logger

# create datasource in tapdata server
def create_datasource():
    datasources = get_sources()
    for datasource in datasources:
        config = datasources[datasource]["config"]
        connector = config.get("connector")
        if connector is None:
            continue
        logger.info("creating datasource {}, connector is: {}", datasource + get_suffix(), connector)
        tapdata_datasource = DataSource(connector, datasource + get_suffix())
        for c in config:
            if c.startswith("__"):  # ignore __ start config, as it's useless for server config
                continue
            if c == "connector":  # ignore connector config, as it's useless for server config
                continue

            # here we need to run function,such as tapdata_datasource.uri(uri), or tapdata_datasource.host()
            # but dynamic getattr is not possible because function is dynamic, we copy its logic here
            if c == "uri":
                tapdata_datasource.pdk_setting.update({"isUri": True, c: config[c]})
            else:
                tapdata_datasource.pdk_setting.update({c: config[c]})
        tapdata_datasource.save()


if __name__ == "__main__":
    create_datasource()
