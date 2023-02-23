package io.tapdata.connector.gauss;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.gauss.config.GaussConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class GaussTest extends CommonDbTest {
    public GaussTest() {
        super();
    }

    public GaussTest(GaussConfig gaussConfig, Consumer<TestItem> consumer) {
        super(gaussConfig, consumer);
    }

    public GaussTest initContext() {
        jdbcContext = DataSourcePool.getJdbcContext(commonDbConfig, GaussJdbcContext.class, uuid);
        return this;
    }
}
