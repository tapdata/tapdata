package io.tapdata.connector.guass;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.guass.config.GuassConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class GuassTest extends CommonDbTest {
    public GuassTest() {
        super();
    }

    public GuassTest(GuassConfig guassConfig, Consumer<TestItem> consumer) {
        super(guassConfig, consumer);
    }

    public GuassTest initContext() {
        jdbcContext = DataSourcePool.getJdbcContext(commonDbConfig, GuassJdbcContext.class, uuid);
        return this;
    }
}
