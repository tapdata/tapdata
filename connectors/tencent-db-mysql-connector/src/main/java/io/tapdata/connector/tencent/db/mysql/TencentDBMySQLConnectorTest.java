package io.tapdata.connector.tencent.db.mysql;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;

public class TencentDBMySQLConnectorTest extends MysqlConnectionTest {
    protected static final String TAG = TencentDBMySQLConnectorTest.class.getSimpleName();
    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR %s";
    protected static final String CHECK_DATABASE_BINLOG_STATUS_SQL = "SHOW GLOBAL VARIABLES where variable_name = 'log_bin' OR variable_name = 'binlog_format'";
    protected static final String CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL = "SHOW VARIABLES LIKE '%binlog_row_image%'";
    protected static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
    protected MysqlJdbcContext mysqlJdbcContext;

    protected TapConnectionContext tapConnectionContext;

    protected ConnectionOptions connectionOptions;

    protected boolean cdcCapability = true;


    public TencentDBMySQLConnectorTest(MysqlJdbcContext mysqlJdbcContext, TapConnectionContext tapConnectionContext, Consumer<TestItem> consumer, CommonDbConfig commonDbConfig, ConnectionOptions connectionOptions) {
        super(mysqlJdbcContext, tapConnectionContext, consumer, commonDbConfig, connectionOptions);
        this.tapConnectionContext = tapConnectionContext;
        this.mysqlJdbcContext = mysqlJdbcContext;
    }

    @Override
    public Boolean testWritePrivilege() {
        return WriteOrReadPrivilege("write");
    }

    @Override
    public Boolean testReadPrivilege() {
        return WriteOrReadPrivilege("read");
    }

    private boolean WriteOrReadPrivilege(String mark) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String databaseName = String.valueOf(connectionConfig.get("database"));
        String userName = String.valueOf(connectionConfig.get("username"));
        List<String> tableList = new ArrayList<>();
        AtomicReference<Boolean> globalWrite = new AtomicReference<>();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if ("write".equals(mark)) {
            itemMark = TestItem.ITEM_WRITE;
        }
        try {
            String finalItemMark = itemMark;
            mysqlJdbcContext.query(String.format(CHECK_DATABASE_PRIVILEGES_SQL, userName), resultSet -> {
                while (resultSet.next()) {
                    String grantSql = resultSet.getString(1);
                    if (testWriteOrReadPrivilege(grantSql, tableList, databaseName, mark)) {
                        testItem.set(testItem(finalItemMark, TestItem.RESULT_SUCCESSFULLY));
                        globalWrite.set(true);
                        return;
                    }
                }

            });
        } catch (Throwable e) {
            consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        if (globalWrite.get() != null) {
            consumer.accept(testItem.get());
            return true;
        }
        if (CollectionUtils.isNotEmpty(tableList)) {
            consumer.accept(testItem(itemMark, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, JSONObject.toJSONString(tableList)));
            return true;
        }
        consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, "Without table can " + mark));
        return false;
    }

    @Override
    public boolean testWriteOrReadPrivilege(String grantSql, List<String> tableList, String databaseName, String mark) {
        boolean privilege;
        privilege = grantSql.contains("INSERT") && grantSql.contains("UPDATE") && grantSql.contains("DELETE")
                || grantSql.contains("ALL PRIVILEGES");
        if ("read".equals(mark)) {
            privilege = grantSql.contains("SELECT") || grantSql.contains("ALL PRIVILEGES");
        }
        grantSql = grantSql.replaceAll("\\\\", "");
        if (grantSql.contains("*.* TO")) {
            if (privilege) {
                return true;
            }
        } else if (grantSql.contains("`" + databaseName + "`" + ".* TO")) {
            if (privilege) {
                return true;
            }
        } else if (databaseName.contains("_") && grantSql.contains("`" + databaseName.replace("_", "\\_") + "`" + ".* TO")) {
            if (privilege) {
                return true;
            }
        } else if (grantSql.contains("`" + databaseName + "`" + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        } else if (databaseName.contains("_") && grantSql.contains("`" + databaseName.replace("_", "\\_") + "`" + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName.replace("_", "\\_") + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        }
        return false;
    }

    @Override
    public Boolean testCDCPrivileges() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        //String databaseName = String.valueOf(connectionConfig.get("database"));
        String userName = String.valueOf(connectionConfig.get("username"));
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            StringBuilder missPri = new StringBuilder();
            List<CdcPrivilege> cdcPrivileges = new ArrayList<>(Arrays.asList(CdcPrivilege.values()));
            mysqlJdbcContext.query(String.format(CHECK_DATABASE_PRIVILEGES_SQL, userName), resultSet -> {
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
                    cdcCapability = false;
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
                consumer.accept(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                cdcCapability = false;
                consumer.accept(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Check cdc privileges failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e)));
            }

        } catch (Throwable e) {
            consumer.accept(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check cdc privileges failed; " + e.getMessage() + "\n" + getStackString(e)));
            cdcCapability = false;
            return true;
        }
        consumer.accept(testItem.get());
        return true;
    }

    protected enum CdcPrivilege {
        ALL_PRIVILEGES("ALL PRIVILEGES ON *.*", true),
        REPLICATION_CLIENT("REPLICATION CLIENT|SUPER", false),
        REPLICATION_SLAVE("REPLICATION SLAVE", false);
        //LOCK_TABLES("LOCK TABLES|ALL", false),
        //RELOAD("RELOAD", false);


        private final String privileges;
        private final boolean onlyNeed;

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
