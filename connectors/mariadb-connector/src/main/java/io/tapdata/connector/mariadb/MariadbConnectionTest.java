package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlJdbcContext;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.lang3.StringUtils;

import static io.tapdata.base.ConnectorBase.testItem;


public class MariadbConnectionTest extends MysqlConnectionTest {

    private static final String MARIADB_VERSION_5 = "5";

    private static final String MARIADB_VERSION_10 = "10";


    public MariadbConnectionTest(MysqlJdbcContext mysqlJdbcContext) {
        super(mysqlJdbcContext);
    }

    /**
     * 检查mariadb版本。只支持5.x和10.x版本
     *
     * @return
     */
    public TestItem testDatabaseVersion() {
        try {
            String version = mysqlJdbcContext.getMysqlVersion();
            if (StringUtils.isNotBlank(version)) {
                if (!version.startsWith(MARIADB_VERSION_5) && !version.startsWith(MARIADB_VERSION_10)) {
                    return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_FAILED, "Unsupported this MYSQL database version: " + version);
                }
            }
        } catch (Throwable e) {
            return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_FAILED, "Error checking version, reason: " + e.getMessage());
        }
        return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_SUCCESSFULLY);

    }


}
