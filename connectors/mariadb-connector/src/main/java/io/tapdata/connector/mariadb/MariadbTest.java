package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class MariadbTest extends MysqlConnectionTest {

    public MariadbTest(MysqlConfig mysqlConfig, Consumer<TestItem> consumer) {
        super(mysqlConfig, consumer);
        testFunctionMap.remove("testCreateTablePrivilege");
    }
}
