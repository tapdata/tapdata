package io.tapdata.connector.hive;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class HiveTest extends CommonDbTest {

    public HiveTest(HiveConfig hiveConfig, Consumer<TestItem> consumer) {
        super(hiveConfig, consumer);
        jdbcContext = new HiveJdbcContext(hiveConfig);
    }
}
