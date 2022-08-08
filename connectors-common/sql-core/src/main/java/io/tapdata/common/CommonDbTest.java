package io.tapdata.common;

import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.util.UUID;

import static io.tapdata.base.ConnectorBase.testItem;

public class CommonDbTest implements AutoCloseable {

    protected final CommonDbConfig commonDbConfig;
    protected JdbcContext jdbcContext;
    protected final String uuid = UUID.randomUUID().toString();

    public CommonDbTest(CommonDbConfig commonDbConfig) {
        this.commonDbConfig = commonDbConfig;
    }

    //Test host and port
    public TestItem testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(commonDbConfig.getHost(), commonDbConfig.getPort());
            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            return testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test connect and log in
    public TestItem testConnect() {
        try (
                Connection connection = jdbcContext.getConnection()
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
