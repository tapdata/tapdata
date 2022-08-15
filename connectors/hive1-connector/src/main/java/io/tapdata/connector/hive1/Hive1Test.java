package io.tapdata.connector.hive1;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.pdk.apis.entity.TestItem;

import java.sql.Connection;

import static io.tapdata.base.ConnectorBase.testItem;


public class Hive1Test extends CommonDbTest {

    public Hive1Test(Hive1Config hive1Config) {
        super(hive1Config);
        jdbcContext = DataSourcePool.getJdbcContext(hive1Config, Hive1JdbcContext.class, uuid);

    }

    public TestItem testConnect(Hive1Config hive1Config) {
        try (
                Connection connection = ((Hive1JdbcContext)jdbcContext).getConnection(hive1Config)
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }
}
