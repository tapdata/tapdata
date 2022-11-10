package io.tapdata.common;

import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.testItem;

public class CommonDbTest implements AutoCloseable {

    protected CommonDbConfig commonDbConfig;
    protected JdbcContext jdbcContext;
    protected Consumer<TestItem> consumer;
    protected Map<String, Supplier<Boolean>> testFunctionMap;
    protected final String uuid = UUID.randomUUID().toString();
    private static final String TEST_HOST_PORT_MESSAGE = "connected to %s:%s succeed!";
    private static final String TEST_CONNECTION_LOGIN = "login succeed!";

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
    }

    public Boolean testOneByOne() {
        for (Map.Entry<String, Supplier<Boolean>> entry : testFunctionMap.entrySet()) {
            if (!entry.getValue().get()) {
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
            String versionStr = databaseMetaData.getDatabaseProductName() + " " +
                    databaseMetaData.getDatabaseMajorVersion() + "." + databaseMetaData.getDatabaseMinorVersion();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, versionStr));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            if (jdbcContext.queryAllTables(Arrays.asList("__tapdata_test", "__TAPDATA_TEST")).size() > 0) {
                sqls.add("drop table __tapdata_test");
            }
            //create
            sqls.add("create table __tapdata_test(col1 int)");
            //insert
            sqls.add("insert into __tapdata_test values(0)");
            //update
            sqls.add("update __tapdata_test set col1=1 where 1=1");
            //delete
            sqls.add("delete from __tapdata_test where 1=1");
            //drop
            sqls.add("drop table __tapdata_test");
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
        }
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
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
