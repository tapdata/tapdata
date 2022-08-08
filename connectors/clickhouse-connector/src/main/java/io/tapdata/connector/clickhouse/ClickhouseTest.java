package io.tapdata.connector.clickhouse;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;


public class ClickhouseTest extends CommonDbTest {

    public ClickhouseTest(ClickhouseConfig clickhouseConfig) {
        super(clickhouseConfig);
        jdbcContext = DataSourcePool.getJdbcContext(clickhouseConfig, ClickhouseJdbcContext.class, uuid);

    }
}
