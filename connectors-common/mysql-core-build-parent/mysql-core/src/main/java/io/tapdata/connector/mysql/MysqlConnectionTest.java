package io.tapdata.connector.mysql;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
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
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 11:58
 **/
public class MysqlConnectionTest extends CommonDbTest {
    protected static final String TAG = MysqlConnectionTest.class.getSimpleName();
    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR CURRENT_USER";
    protected static final String CHECK_DATABASE_BINLOG_STATUS_SQL = "SHOW GLOBAL VARIABLES where variable_name = 'log_bin' OR variable_name = 'binlog_format'";
    protected static final String CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL = "SHOW VARIABLES LIKE 'binlog_row_image%'";
    protected static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
    protected MysqlJdbcContext mysqlJdbcContext;

    protected TapConnectionContext tapConnectionContext;

    protected ConnectionOptions connectionOptions;

    protected  boolean cdcCapability = true;

    public MysqlConnectionTest(MysqlJdbcContext mysqlJdbcContext, TapConnectionContext tapConnectionContext,
                               Consumer<TestItem> consumer, CommonDbConfig commonDbConfig, ConnectionOptions connectionOptions) {
        super(commonDbConfig, consumer);
        this.mysqlJdbcContext = mysqlJdbcContext;
        this.tapConnectionContext = tapConnectionContext;
        this.connectionOptions = connectionOptions;
        if (!ConnectionTypeEnum.SOURCE.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testCreateTablePrivilege", this::testCreateTablePrivilege);
        }
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testBinlogMode", this::testBinlogMode);
            testFunctionMap.put("testBinlogRowImage", this::testBinlogRowImage);
            testFunctionMap.put("testCDCPrivileges", this::testCDCPrivileges);
            testFunctionMap.put("setCdcCapabilitie", this::setCdcCapabilitie);
        }
    }

    @Override
    public Boolean testHostPort() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            consumer.accept(testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (IOException e) {
            consumer.accept(testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;

        }
    }

    @Override
    public Boolean testConnect() {
        try (
                Connection connection = mysqlJdbcContext.getConnection();
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
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
                    consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg));
                    return false;

                }
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    public Boolean testWritePrivilege() {
       return  WriteOrReadPrivilege("write");
    }

    private boolean  WriteOrReadPrivilege(String mark){
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String databaseName = String.valueOf(connectionConfig.get("database"));
        List<String> tableList = new ArrayList();
        AtomicReference<Boolean> globalWrite = new AtomicReference();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if("write".equals(mark)){
            itemMark =TestItem.ITEM_WRITE;
        }
        try {
            String finalItemMark = itemMark;
            mysqlJdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
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
        consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, "Without table can "+mark));
        return false;
    }

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
    public Boolean testReadPrivilege(){
        return  WriteOrReadPrivilege("read");
    }

    @Override
    public Boolean testVersion() {
        try {
            String version = mysqlJdbcContext.getMysqlVersion();
            String versionMsg = "version: " + version;
            if (StringUtils.isNotBlank(version)) {
                if (version.startsWith("5") || version.startsWith("8")) {
                    int diff = version.compareTo("5.1");
                    if (diff < 0) {
                        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
                        return true;
                    }
                } else {
                    consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
                    return true;
                }
            }
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testCDCPrivileges(){
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

    public Boolean testBinlogMode() {
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
                    cdcCapability = false;
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                            "MySqlServer dose not open row level binlog mode, will not be able to use the incremental sync feature"));
                } else {
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY));
                }
            });
        } catch (SQLException e) {
            cdcCapability = false;
            consumer.accept(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check binlog mode failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e)));

        } catch (Throwable e) {
            cdcCapability = false;
            consumer.accept(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check binlog mode failed; " + e.getMessage() + "\n" + getStackString(e)));
        }
        consumer.accept(testItem.get());
        return true;
    }

    public Boolean testBinlogRowImage() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            mysqlJdbcContext.query(CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL, resultSet -> {
                while (resultSet.next()) {
                    String value = resultSet.getString(2);
                    if (!StringUtils.equalsAnyIgnoreCase("FULL", value)) {
                        testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                                "binlog row image is [" + value + "]"));
                        cdcCapability = false;
                    }
                }
            });
            if (null == testItem.get()) {
                testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            }
        } catch (Throwable e) {
            consumer.accept(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check binlog row image failed; " + e.getMessage() + "\n" + getStackString(e)));
            cdcCapability = false;
        }
        consumer.accept(testItem.get());
        return true;
    }

    public Boolean testCreateTablePrivilege() {
        try {
            DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
            String username = String.valueOf(connectionConfig.get("username"));
            boolean missed = checkMySqlCreateTablePrivilege(username);
            if (missed) {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "User does not have privileges [ create ], will not be able to use the create table(s) feature"));
                return true;
            }
            consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (SQLException e) {
            int errorCode = e.getErrorCode();
            String sqlState = e.getSQLState();
            String message = e.getMessage();

            // 如果源库是关闭密码认证时，默认权限校验通过
            if (errorCode == 1290 && "HY000".equals(sqlState) && StringUtils.isNotBlank(message) && message.contains("--skip-grant-tables")) {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Check create table privileges failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e)));
            }
        } catch (Throwable e) {
            consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check create table privileges failed; " + e.getMessage() + "\n" + getStackString(e)));
            return true;
        }
        return true;
    }

    protected boolean checkMySqlCreateTablePrivilege(String username) throws Throwable {
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

    public Boolean setCdcCapabilitie(){
        if(cdcCapability){
            List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
            ddlCapabilities.forEach(connectionOptions::capability);
        }
        return true;
    }

    @Override
    public void close() {
        try {
            if (mysqlJdbcContext != null) {
                mysqlJdbcContext.close();
            }
        } catch (Exception ignore) {
            TapLogger.warn(TAG, ignore.getMessage());
        }
    }

    protected enum CdcPrivilege {
        ALL_PRIVILEGES("ALL PRIVILEGES ON *.*", true),
        REPLICATION_CLIENT("REPLICATION CLIENT|SUPER", false),
        REPLICATION_SLAVE("REPLICATION SLAVE", false);
        //		LOCK_TABLES("LOCK TABLES|ALL", false),
//    RELOAD("RELOAD", false);


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
