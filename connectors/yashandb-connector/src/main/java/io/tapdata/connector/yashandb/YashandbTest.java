package io.tapdata.connector.yashandb;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.yashandb.config.YashandbConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2023/5/22
 **/
public class YashandbTest extends CommonDbTest {
    public YashandbTest(YashandbConfig yashandbConfig, Consumer<TestItem> consumer) {
        super(yashandbConfig, consumer);
    }

    public YashandbTest initContext() {
        jdbcContext = new YashandbJdbcContext((YashandbConfig) commonDbConfig);
        return this;
    }
}
