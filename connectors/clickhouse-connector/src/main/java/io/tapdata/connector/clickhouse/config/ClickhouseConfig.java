package io.tapdata.connector.clickhouse.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.Map;

public class ClickhouseConfig extends CommonDbConfig implements Serializable {

    public ClickhouseConfig() {
        setDbType("clickhouse");
        setEscapeChar('`');
        setJdbcDriver("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public ClickhouseConfig load(Map<String, Object> map) {
        ClickhouseConfig config = (ClickhouseConfig) super.load(map);
        setSchema(getDatabase());
        return config;
    }

}
