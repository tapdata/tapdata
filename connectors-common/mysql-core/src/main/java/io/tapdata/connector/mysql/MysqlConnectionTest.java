package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 11:58
 **/
public class MysqlConnectionTest {

	private static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR CURRENT_USER";
	private static final String CHECK_DATABASE_BINLOG_STATUS_SQL = "SHOW GLOBAL VARIABLES where variable_name = 'log_bin' OR variable_name = 'binlog_format'";
	private static final String CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL = "SHOW VARIABLES LIKE '%binlog_row_image%'";
	private static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
			"FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
			"WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
	private MysqlJdbcContext mysqlJdbcContext;

	public MysqlConnectionTest(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
	}

	public TestItem testHostPort(TapConnectionContext tapConnectionContext) {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String host = String.valueOf(connectionConfig.get("host"));
		int port = ((Number) connectionConfig.get("port")).intValue();
		try {
			NetUtil.validateHostPortWithSocket(host, port);
			return testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
		} catch (IOException e) {
			return testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
		}
	}

	public TestItem testConnect() {
		try (
				Connection connection = mysqlJdbcContext.getConnection()
		) {
			return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
		} catch (Exception e) {
			return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
		}
	}

	public TestItem testDatabaseVersion() {
		try {
			String version = mysqlJdbcContext.getMysqlVersion();
			if (StringUtils.isNotBlank(version)) {
				if (version.startsWith("5") || version.startsWith("8")) {
					int diff = version.compareTo("5.1");
					if (diff < 0) {
						return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_FAILED, "Unsupported this MYSQL database version: " + version);
					}
				} else {
					return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_FAILED, "Unsupported this MYSQL database version: " + version);
				}
			}
		} catch (Throwable e) {
			return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_FAILED, "Error checking version, reason: " + e.getMessage());
		}
		return testItem(MysqlTestItem.CHECK_VERSION.getContent(), TestItem.RESULT_SUCCESSFULLY);
	}

	public TestItem testCDCPrivileges() throws Throwable {
		AtomicReference<TestItem> testItem = new AtomicReference<>();
		try {
			StringBuilder missPri = new StringBuilder();
			List<CdcPrivilege> cdcPrivileges = new ArrayList<>(Arrays.asList(CdcPrivilege.values()));
			mysqlJdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
				while (resultSet.next()) {
					String grantSql = resultSet.getString(1);
					Iterator<CdcPrivilege> iterator = cdcPrivileges.iterator();
					while (iterator.hasNext()) {
						boolean match = false;
						CdcPrivilege cdcPrivilege = iterator.next();
						String privileges = cdcPrivilege.getPrivileges();
						String[] split = privileges.split("\\|");
						for (String privilege : split) {
							match = grantSql.contains(privilege);
							if (match) {
								if (cdcPrivilege.onlyNeed) {
									testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
									return;
								}
								break;
							}
						}
						if (match) {
							iterator.remove();
						}
					}
				}
			});
			if (null == testItem.get()) {
				if (CollectionUtils.isNotEmpty(cdcPrivileges) && cdcPrivileges.size() > 1) {
					for (CdcPrivilege cdcPrivilege : cdcPrivileges) {
						String[] split = cdcPrivilege.privileges.split("\\|");
						if (cdcPrivilege.onlyNeed) {
							continue;
						}
						for (String s : split) {
							missPri.append(s).append("|");
						}
						missPri.replace(missPri.lastIndexOf("|"), missPri.length(), "").append(" ,");
					}

					missPri.replace(missPri.length() - 2, missPri.length(), "");
					testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
							"User does not have privileges [" + missPri + "], will not be able to use the incremental sync feature."));
				}
			}
			if (null == testItem.get()) {
				testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
			}
		} catch (SQLException e) {
			int errorCode = e.getErrorCode();
			String sqlState = e.getSQLState();
			String message = e.getMessage();

			// 如果源库是关闭密码认证时，默认权限校验通过
			if (errorCode == 1290 && "HY000".equals(sqlState) && StringUtils.isNotBlank(message) && message.contains("--skip-grant-tables")) {
				return testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
			} else {
				return testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
						"Check cdc privileges failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e));
			}
		} catch (Exception e) {
			return testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
					"Check cdc privileges failed; " + e.getMessage() + "\n" + getStackString(e));
		}
		return testItem.get();
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
							"MySqlServer dose not open row level binlog mode, will not be able to use the incremental sync feature"));
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

	public TestItem testBinlogRowImage() {
		AtomicReference<TestItem> testItem = new AtomicReference<>();
		try {
			mysqlJdbcContext.query(CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL, resultSet -> {
				while (resultSet.next()) {
					String value = resultSet.getString(2);
					if (!StringUtils.equalsAnyIgnoreCase("FULL", value)) {
						testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
								"binlog row image is [" + value + "]"));
					}
				}
			});
			if (null == testItem.get()) {
				testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
			}
		} catch (Throwable e) {
			return testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
					"Check binlog row image failed; " + e.getMessage() + "\n" + getStackString(e));
		}
		return testItem.get();
	}

	public TestItem testCreateTablePrivilege(TapConnectionContext tapConnectionContext) {
		try {
			DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
			String username = String.valueOf(connectionConfig.get("username"));
			boolean missed = checkMySqlCreateTablePrivilege(username);
			if (missed) {
				return testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
						"User does not have privileges [ create ], will not be able to use the create table(s) feature");
			}
			return testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
		} catch (SQLException e) {
			int errorCode = e.getErrorCode();
			String sqlState = e.getSQLState();
			String message = e.getMessage();

			// 如果源库是关闭密码认证时，默认权限校验通过
			if (errorCode == 1290 && "HY000".equals(sqlState) && StringUtils.isNotBlank(message) && message.contains("--skip-grant-tables")) {
				return testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
			} else {
				return testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
						"Check create table privileges failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e));
			}
		} catch (Throwable e) {
			return testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
					"Check create table privileges failed; " + e.getMessage() + "\n" + getStackString(e));
		}
	}

	private boolean checkMySqlCreateTablePrivilege(String username) throws Throwable {
		AtomicBoolean result = new AtomicBoolean(true);
		mysqlJdbcContext.query(String.format(CHECK_CREATE_TABLE_PRIVILEGES_SQL, username), resultSet -> {
			while (resultSet.next()) {
				if (resultSet.getInt(1) > 0) {
					result.set(false);
				}
			}
		});
		return result.get();
	}

	private enum CdcPrivilege {
		ALL_PRIVILEGES("ALL PRIVILEGES ON *.*", true),
		REPLICATION_CLIENT("REPLICATION CLIENT|SUPER", false),
		REPLICATION_SLAVE("REPLICATION SLAVE", false);
		//		LOCK_TABLES("LOCK TABLES|ALL", false),
//    RELOAD("RELOAD", false);


		private String privileges;
		private boolean onlyNeed;

		CdcPrivilege(String privileges, boolean onlyNeed) {
			this.privileges = privileges;
			this.onlyNeed = onlyNeed;
		}

		public String getPrivileges() {
			return privileges;
		}

		public boolean isOnlyNeed() {
			return onlyNeed;
		}
	}
}
