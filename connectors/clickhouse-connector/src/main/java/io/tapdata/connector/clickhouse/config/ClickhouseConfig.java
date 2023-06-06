package io.tapdata.connector.clickhouse.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.Map;

public class ClickhouseConfig extends CommonDbConfig implements Serializable {

    private Integer mergeMinutes = 60;

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

    public Integer getMergeMinutes() {
        return mergeMinutes;
    }

    public void setMergeMinutes(Integer mergeMinutes) {
        this.mergeMinutes = mergeMinutes;
    }

}
