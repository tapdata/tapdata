package io.tapdata.connector.clickhouse;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;


public class ClickhouseTest extends CommonDbTest {

    public ClickhouseTest(ClickhouseConfig clickhouseConfig, Consumer<TestItem> consumer) {
        super(clickhouseConfig, consumer);
        jdbcContext = DataSourcePool.getJdbcContext(clickhouseConfig, ClickhouseJdbcContext.class, uuid);

    }
}
