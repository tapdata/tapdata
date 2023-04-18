package io.tapdata.connector.hive.config;

import io.tapdata.common.CommonDbConfig;

public class HiveConfig extends CommonDbConfig {

    public HiveConfig() {
        setDbType("hive2");
        setJdbcDriver("org.apache.hive.jdbc.HiveDriver");
        setEscapeChar('`');
    }
}
