package io.tapdata.connector.selectdb;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.List;
import java.util.function.Consumer;

/**
 * Author:Skeet
 * Date: 2022/12/9
 **/

public class SelectDbTest extends CommonDbTest {
    protected static final String TAG = SelectDbTest.class.getSimpleName();

    public SelectDbTest(SelectDbConfig selectDbConfig, Consumer<TestItem> consumer) {
        super(selectDbConfig, consumer);

    }

    public SelectDbTest initContext() {
        jdbcContext = DataSourcePool.getJdbcContext(commonDbConfig, SelectDbJdbcContext.class, uuid);
        return this;
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("5.7", "8.0");
    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
