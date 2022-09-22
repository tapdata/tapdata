package io.tapdata.oceanbase;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.constant.DbTestItem;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * @Author dayun
 * @Date 8/23/22
 */
public class OceanbaseTest extends CommonDbTest implements AutoCloseable {

    public OceanbaseTest(OceanbaseConfig oceanbaseConfig) {
        super(oceanbaseConfig);
        jdbcContext = DataSourcePool.getJdbcContext(oceanbaseConfig, OceanbaseJdbcContext.class, uuid);
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


