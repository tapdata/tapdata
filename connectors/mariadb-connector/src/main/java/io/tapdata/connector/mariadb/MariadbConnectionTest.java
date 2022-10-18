package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlJdbcContext;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;


public class MariadbConnectionTest extends MysqlConnectionTest {

    private static final String MARIADB_VERSION_5 = "5";

    private static final String MARIADB_VERSION_10 = "10";


    public MariadbConnectionTest(MysqlJdbcContext mysqlJdbcContext) {
        super(mysqlJdbcContext);
    }

    public TestItem testConnect() {
        try (
                Connection connection = mysqlJdbcContext.getConnection();
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String errMsg = e.getMessage();
                if (errMsg.contains("using password")) {
                    String password = mysqlJdbcContext.getTapConnectionContext().getConnectionConfig().getString("password");
                    if (StringUtils.isNotEmpty(password)) {
                        errMsg = "password or username is error ,please check";
                    } else {
                        errMsg = "password is empty,please enter password";
                    }
                    return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg);

                }
            }
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

	/**
	 * 检查mariadb版本。只支持5.x和10.x版本
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

    public TestItem testBinlogMode() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            mysqlJdbcContext.query(CHECK_DATABASE_BINLOG_STATUS_SQL, resultSet -> {
                String mode = null;
                String logbin = null;
                while (resultSet.next()) {
                    if ("binlog_format".equals(resultSet.getString(1))) {
                        mode = resultSet.getString(2);
                    } else {
                        logbin = resultSet.getString(2);
                    }
                }

                if (!"ROW".equalsIgnoreCase(mode) || !"ON".equalsIgnoreCase(logbin)) {
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                            "MariadbServer dose not open row level binlog mode, will not be able to use the incremental sync feature"));
                } else {
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY));
                }
            });
        } catch (SQLException e) {
            return testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check binlog mode failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e));

        } catch (Throwable e) {
            return testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check binlog mode failed; " + e.getMessage() + "\n" + getStackString(e));
        }
        return testItem.get();
    }

}
