package io.tapdata.connector.tidb;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;


/**
 * @author lemon
 */
public class TidbConnectionTest extends CommonDbTest {
    public static final String TAG = TidbConnectionTest.class.getSimpleName();
    private final TidbConfig tidbConfig;
    private KafkaConfig kafkaConfig;
    private final static String PB_SERVER_SUCCESS = "Check PDServer host port is valid";
    private final static String IC_CONFIGURATION_ENABLED = " Check  Incremental is enable";
    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR CURRENT_USER";
    protected static final String CHECK_DATABASE_BINLOG_STATUS_SQL = "SHOW GLOBAL VARIABLES where variable_name = 'log_bin' OR variable_name = 'binlog_format'";
    protected static final String CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL = "SHOW VARIABLES LIKE '%binlog_row_image%'";
    protected static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
    protected static final String CHECK_LOW_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'Create'";
    protected static String CHECK_TIDB_VERSION = "SELECT VERSION()";
    private boolean cdcCapability;
    private final ConnectionOptions connectionOptions;
    private String[] array;

    public TidbConnectionTest(TidbConfig tidbConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(tidbConfig, consumer);
        this.tidbConfig = tidbConfig;
        this.connectionOptions = connectionOptions;
        jdbcContext = new TidbJdbcContext(tidbConfig);
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public Boolean testOneByOne() {
        testFunctionMap.put("testPbserver", this::testPbserver);
        testFunctionMap.put("testVersion", this::testVersion);
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            TidbConfig tidbConfig = (TidbConfig) commonDbConfig;
            if (tidbConfig.getEnableIncrement()) {
                testFunctionMap.put("testKafkaHostPort", this::testKafkaHostPort);
            }
        }
        return super.testOneByOne();
    }

    /**
     * check Pbserver
     *
     * @return
     */
    public Boolean testPbserver() {
        URI uri = URI.create("http://" + tidbConfig.getPdServer());
        try {
            NetUtil.validateHostPortWithSocket(uri.getHost(), uri.getPort());
            consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }

    }

    @Override
    public Boolean testConnect() {
        try (
                Connection connection = jdbcContext.getConnection();
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String errMsg = e.getMessage();
                if (errMsg.contains("using password")) {
                    String password = tidbConfig.getPassword();
                    if (StringUtils.isNotEmpty(password)) {
                        errMsg = "password or username is error ,please check";
                    } else {
                        errMsg = "password is empty,please enter password";
                    }
                    consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg));


                }
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    public Boolean testWritePrivilege() {
        return WriteOrReadPrivilege("write");
    }

    private boolean WriteOrReadPrivilege(String mark) {
        String databaseName = tidbConfig.getDatabase();
        List<String> tableList = new ArrayList<>();
        AtomicReference<Boolean> globalWrite = new AtomicReference<>();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if ("write".equals(mark)) {
            itemMark = TestItem.ITEM_WRITE;
        }
        try {
            String finalItemMark = itemMark;
            jdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
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

    public boolean testWriteOrReadPrivilege(String grantSql, List<String> tableList, String databaseName, String mark) {
        boolean privilege;
        privilege = grantSql.contains("INSERT") && grantSql.contains("UPDATE") && grantSql.contains("DELETE")
                || grantSql.contains("ALL PRIVILEGES");
        if ("read".equals(mark)) {
            privilege = grantSql.contains("SELECT") || grantSql.contains("ALL PRIVILEGES");
        }
        if (grantSql.contains("*.* TO")) {
            if (privilege) {
                return true;
            }

        } else if (grantSql.contains(databaseName + ".* TO")) {
            if (privilege) {
                return true;
            }
        } else if (grantSql.contains("`" + databaseName + "`" + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        }
        return false;
    }


    @Override
    public Boolean testReadPrivilege() {
        return WriteOrReadPrivilege("read");
    }

    //    @Override
//    public Boolean testVersion() {
//        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY));
//        return true;
//    }
    @Override
    public Boolean testVersion() {
        AtomicReference<String> version = new AtomicReference<>();
        try {
            jdbcContext.query(CHECK_TIDB_VERSION, resultSet -> {
                while (resultSet.next()) {
                    String versionMsg = resultSet.getString(1);
                    version.set(versionMsg);
                }
            });
            array = String.valueOf(version).split("-");
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, array[1] + "-" + array[2]));
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    public Boolean testCDCPrivileges() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            StringBuilder missPri = new StringBuilder();
            List<TidbConnectionTest.CdcPrivilege> cdcPrivileges = new ArrayList<>(Arrays.asList(TidbConnectionTest.CdcPrivilege.values()));
            jdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
                while (resultSet.next()) {
                    String grantSql = resultSet.getString(1);
                    Iterator<TidbConnectionTest.CdcPrivilege> iterator = cdcPrivileges.iterator();
                    while (iterator.hasNext()) {
                        boolean match = false;
                        TidbConnectionTest.CdcPrivilege cdcPrivilege = iterator.next();
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
                    for (TidbConnectionTest.CdcPrivilege cdcPrivilege : cdcPrivileges) {
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
            jdbcContext.query(CHECK_DATABASE_BINLOG_STATUS_SQL, resultSet -> {
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
                            "TidbServer dose not open row level binlog mode, will not be able to use the incremental sync feature"));
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
            jdbcContext.query(CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL, resultSet -> {
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

    public Boolean setCdcCapabilitie() {
        if (cdcCapability) {
            List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
            ddlCapabilities.forEach(connectionOptions::capability);
        }
        return true;
    }

    protected enum CdcPrivilege {
        ALL_PRIVILEGES("ALL PRIVILEGES ON *.*", true),
        REPLICATION_CLIENT("REPLICATION CLIENT|Super", false),
        REPLICATION_SLAVE("REPLICATION SLAVE", false);

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

    /**
     * check kafka
     */

    public Boolean testKafkaHostPort() {
        try (
                TicdcKafkaService ticdcKafkaService = new TicdcKafkaService(kafkaConfig, tidbConfig);
        ) {
            TestItem testHostAndPort = ticdcKafkaService.testHostAndPort();
            consumer.accept(testHostAndPort);
            return testHostAndPort.getResult() != TestItem.RESULT_FAILED;
        }
    }

}
