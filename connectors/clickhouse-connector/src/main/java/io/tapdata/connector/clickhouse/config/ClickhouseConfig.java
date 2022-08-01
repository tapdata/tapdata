package io.tapdata.connector.clickhouse.config;

import io.tapdata.common.CommonDbConfig;
import ru.yandex.clickhouse.ClickHouseUtil;

import java.io.Serializable;

public class ClickhouseConfig extends CommonDbConfig implements Serializable {


    public ClickhouseConfig() {
        setDbType("clickhouse");
        setJdbcDriver("ru.yandex.clickhouse.ClickHouseDriver");
    }
}
