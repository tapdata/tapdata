package io.tapdata.common;

import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.constant.DbTestItem;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

public class CommonDbTest implements AutoCloseable {

    protected CommonDbConfig commonDbConfig;
    protected JdbcContext jdbcContext;
    protected Consumer<TestItem> consumer;
    protected Map<String, Supplier<Boolean>> testFunctionMap;
    protected final String uuid = UUID.randomUUID().toString();
    protected static final String TEST_HOST_PORT_MESSAGE = "connected to %s:%s succeed!";
    protected static final String TEST_CONNECTION_LOGIN = "login succeed!";
    protected static final String TEST_WRITE_TABLE = "tapdata___test";

    public CommonDbTest() {

    }

    public CommonDbTest(CommonDbConfig commonDbConfig, Consumer<TestItem> consumer) {
        this.commonDbConfig = commonDbConfig;
        this.consumer = consumer;
        testFunctionMap = new LinkedHashMap<>();
        testFunctionMap.put("testHostPort", this::testHostPort);
        testFunctionMap.put("testConnect", this::testConnect);
        testFunctionMap.put("testVersion", this::testVersion);
        if (!ConnectionTypeEnum.SOURCE.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testWritePrivilege", this::testWritePrivilege);
        }
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testReadPrivilege", this::testReadPrivilege);
            testFunctionMap.put("testStreamRead", this::testStreamRead);
        }
    }

    public Boolean testOneByOne() {
        for (Map.Entry<String, Supplier<Boolean>> entry : testFunctionMap.entrySet()) {
            Boolean res = entry.getValue().get();
            if (EmptyKit.isNotNull(res) && !res) {
                return false;
            }
        }
        return true;
    }

    //Test host and port
    protected Boolean testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(commonDbConfig.getHost(), commonDbConfig.getPort());
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY,
                    String.format(TEST_HOST_PORT_MESSAGE, commonDbConfig.getHost(), commonDbConfig.getPort())));
            return true;
        } catch (IOException e) {
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    //Test connect and log in
    protected Boolean testConnect() {
        try (
                Connection connection = jdbcContext.getConnection()
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    protected Boolean testVersion() {
        try (
                Connection connection = jdbcContext.getConnection()
        ) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            String version = databaseMetaData.getDatabaseMajorVersion() + "." + databaseMetaData.getDatabaseMinorVersion();
            String versionMsg = databaseMetaData.getDatabaseProductName() + " " + version;
            if (supportVersions().stream().noneMatch(v -> {
                String reg = v.replaceAll("\\*", ".*");
                Pattern pattern = Pattern.compile(reg);
                return pattern.matcher(version).matches();
            })) {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " not supported well"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, versionMsg));
            }
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    protected List<String> supportVersions() {
        return Collections.singletonList("*.*");
    }

    protected static final String TEST_CREATE_TABLE = "create table %s(col1 int not null, primary key(col1))";
    protected static final String TEST_WRITE_RECORD = "insert into %s values(0)";
    protected static final String TEST_UPDATE_RECORD = "update %s set col1=1 where 1=1";
    protected static final String TEST_DELETE_RECORD = "delete from %s where 1=1";
    protected static final String TEST_DROP_TABLE = "drop table %s";
    protected static final String TEST_WRITE_SUCCESS = "Create,Insert,Update,Delete,Drop succeed";

    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("\"" + commonDbConfig.getSchema() + "\".") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    protected static String getTestCreateTable() {
        return TEST_CREATE_TABLE;
    }

    protected static String getTestUpdateRecord() {
        return TEST_UPDATE_RECORD;
    }

    public Boolean testReadPrivilege() {
        return true;
    }

    public Boolean testStreamRead() {
        return true;
    }

    //healthCheck-ping
    public ConnectionCheckItem testPing() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_PING);
        try {
            NetUtil.validateHostPortWithSocket(commonDbConfig.getHost(), commonDbConfig.getPort());
            connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    //healthCheck-connection
    public ConnectionCheckItem testConnection() {
        long start = System.currentTimeMillis();
        ConnectionCheckItem connectionCheckItem = ConnectionCheckItem.create();
        connectionCheckItem.item(ConnectionCheckItem.ITEM_CONNECTION);
        try (
                Connection connection = jdbcContext.getConnection()
        ) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            connectionCheckItem.result(ConnectionCheckItem.RESULT_FAILED).information(e.getMessage());
        }
        connectionCheckItem.takes(System.currentTimeMillis() - start);
        return connectionCheckItem;
    }

    @Override
    public void close() {
        try {
            jdbcContext.close();
        } catch (Exception ignored) {
        }
    }

}
